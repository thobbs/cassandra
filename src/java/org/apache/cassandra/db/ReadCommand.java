/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand implements ReadQuery
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    public static final IVersionedSerializer<ReadCommand> legacyRangeSliceCommandSerializer = new LegacyRangeSliceCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyPagedRangeCommandSerializer = new LegacyPagedRangeCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyReadCommandSerializer = new LegacyReadCommandSerializer();

    private final Kind kind;
    private final CFMetaData metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    private boolean isDigestQuery;
    private final boolean isForThrift;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInput in, int version, boolean isDigest, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private SelectionDeserializer selectionDeserializer;

        Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          boolean isDigestQuery,
                          boolean isForThrift,
                          CFMetaData metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits)
    {
        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.isForThrift = isForThrift;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.rowFilter = rowFilter;
        this.limits = limits;
    }

    protected abstract void serializeSelection(DataOutputPlus out, int version) throws IOException;
    protected abstract long selectionSerializedSize(int version);

    /**
     * The metadata for the table queried.
     *
     * @return the metadata for the table queried.
     */
    public CFMetaData metadata()
    {
        return metadata;
    }

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public int nowInSec()
    {
        return nowInSec;
    }

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout();

    /**
     * A filter on which (non-PK) columns must be returned by the query.
     *
     * @return which columns must be fetched by this query.
     */
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    /**
     * Filters/Resrictions on CQL rows.
     * <p>
     * This contains the restrictions that are not directly handled by the
     * {@code ClusteringIndexFilter}. More specifically, this includes any non-PK column
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirely by the clustering index filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the filter holding the expression that rows must satisfy.
     */
    public RowFilter rowFilter()
    {
        return rowFilter;
    }

    /**
     * The limits set on this query.
     *
     * @return the limits set on this query.
     */
    public DataLimits limits()
    {
        return limits;
    }

    /**
     * Whether this query is a digest one or not.
     *
     * @return Whether this query is a digest query.
     */
    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    /**
     * Sets whether this command should be a digest one or not.
     *
     * @param isDigestQuery whether the command should be set as a digest one or not.
     * @return this read command.
     */
    public ReadCommand setIsDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
        return this;
    }

    /**
     * Whether this query is for thrift or not.
     *
     * @return whether this query is for thrift.
     */
    public boolean isForThrift()
    {
        return isForThrift;
    }

    /**
     * The clustering index filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code ClusteringIndexFilter} to use for the partition of key {@code key}.
     */
    public abstract ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key);

    /**
     * Returns a copy of this command.
     *
     * @return a copy of this command.
     */
    public abstract ReadCommand copy();

    /**
     * Whether the provided row, identified by its primary key components, is selected by
     * this read command.
     *
     * @param partitionKey the partition key for the row to test.
     * @param clustering the clustering for the row to test.
     *
     * @return whether the row of partition key {@code partitionKey} and clustering
     * {@code clustering} is selected by this command.
     */
    public abstract boolean selects(DecoratedKey partitionKey, Clustering clustering);

    protected abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadOrderGroup orderGroup);

    public abstract boolean rowsInPartitionAreReversed();

    public ReadResponse createResponse(UnfilteredPartitionIterator iterator)
    {
        return isDigestQuery()
             ? ReadResponse.createDigestResponse(iterator)
             : ReadResponse.createDataResponse(iterator);
    }

    protected SecondaryIndexSearcher getIndexSearcher(ColumnFamilyStore cfs)
    {
        return cfs.indexManager.getBestIndexSearcherFor(this);
    }

    /**
     * Executes this command on the local host.
     *
     * @param cfs the store for the table queried by this command.
     *
     * @return an iterator over the result of executing this command locally.
     */
    @SuppressWarnings("resource") // The result iterator is closed upon exceptions (we know it's fine to potentially not close the intermediary
                                  // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    public UnfilteredPartitionIterator executeLocally(ReadOrderGroup orderGroup)
    {
        long startTimeNanos = System.nanoTime();

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
        SecondaryIndexSearcher searcher = getIndexSearcher(cfs);
        UnfilteredPartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs, orderGroup)
                                         : searcher.search(this, orderGroup);

        try
        {
            resultIterator = UnfilteredPartitionIterators.convertExpiredCellsToTombstones(resultIterator, nowInSec);
            resultIterator = withMetricsRecording(withoutExpiredTombstones(resultIterator, cfs), cfs.metric, startTimeNanos);

            // TODO: we should push the dropping of columns down the layers because
            // 1) it'll be more efficient
            // 2) it could help us solve #6276
            // But there is not reason not to do this as a followup so keeping it here for now (we'll have
            // to be wary of cached row if we move this down the layers)
            if (!metadata().getDroppedColumns().isEmpty())
                resultIterator = UnfilteredPartitionIterators.removeDroppedColumns(resultIterator, metadata().getDroppedColumns());

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            RowFilter updatedFilter = searcher == null
                                       ? rowFilter()
                                       : rowFilter().without(searcher.primaryClause(this));

            // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return limits().filter(rowFilter().filter(resultIterator, nowInSec()), nowInSec());
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    protected abstract void recordLatency(ColumnFamilyMetrics metric, long latencyNanos);

    public PartitionIterator executeInternal(ReadOrderGroup orderGroup)
    {
        return UnfilteredPartitionIterators.filter(executeLocally(orderGroup), nowInSec());
    }

    public ReadOrderGroup startOrderGroup()
    {
        return ReadOrderGroup.forCommand(this);
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final ColumnFamilyMetrics metric, final long startTimeNanos)
    {
        return new WrappingUnfilteredPartitionIterator(iter)
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !ReadCommand.this.metadata().ksName.equals(SystemKeyspace.NAME);

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
            {
                currentKey = iter.partitionKey();

                return new WrappingUnfilteredRowIterator(iter)
                {
                    public Unfiltered next()
                    {
                        Unfiltered unfiltered = super.next();
                        if (unfiltered.kind() == Unfiltered.Kind.ROW)
                        {
                            Row row = (Row) unfiltered;
                            if (row.hasLiveData(ReadCommand.this.nowInSec()))
                                ++liveRows;
                            for (Cell cell : row)
                                if (!cell.isLive(ReadCommand.this.nowInSec()))
                                    countTombstone(row.clustering());
                        }
                        else
                        {
                            countTombstone(unfiltered.clustering());
                        }

                        return unfiltered;
                    }

                    private void countTombstone(ClusteringPrefix clustering)
                    {
                        ++tombstones;
                        if (tombstones > failureThreshold && respectTombstoneThresholds)
                        {
                            String query = ReadCommand.this.toCQLString();
                            Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                            throw new TombstoneOverwhelmingException(tombstones, query, ReadCommand.this.metadata(), currentKey, clustering);
                        }
                    }
                };
            }

            @Override
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    recordLatency(metric, System.nanoTime() - startTimeNanos);

                    metric.tombstoneScannedHistogram.update(tombstones);
                    metric.liveScannedHistogram.update(liveRows);

                    boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                    if (warnTombstones)
                    {
                        String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toCQLString());
                        ClientWarn.warn(msg);
                        logger.warn(msg);
                    }

                    Tracing.trace("Read {} live and {} tombstone cells{}", new Object[]{ liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : "") });
                }
            }
        };
    }

    /**
     * Creates a message for this command.
     */
    public MessageOut<ReadCommand> createMessage(int version)
    {
        if (version >= MessagingService.VERSION_30)
            return new MessageOut<>(MessagingService.Verb.READ, this, serializer);

        return createLegacyMessage();
    }

    protected abstract MessageOut<ReadCommand> createLegacyMessage();

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip expired tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for expired tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutExpiredTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        return new TombstonePurgingPartitionIterator(iterator, cfs.gcBefore(nowInSec()))
        {
            protected long getMaxPurgeableTimestamp()
            {
                return Long.MAX_VALUE;
            }
        };
    }

    /**
     * Recreate the CQL string corresponding to this query.
     * <p>
     * Note that in general the returned string will not be exactly the original user string, first
     * because there isn't always a single syntax for a given query,  but also because we don't have
     * all the information needed (we know the non-PK columns queried but not the PK ones as internally
     * we query them all). So this shouldn't be relied too strongly, but this should be good enough for
     * debugging purpose which is what this is for.
     */
    public String toCQLString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(columnFilter());
        sb.append(" FROM ").append(metadata().ksName).append(".").append(metadata.cfName);
        appendCQLWhereClause(sb);

        if (limits() != DataLimits.NONE)
            sb.append(" ").append(limits());
        return sb.toString();
    }

    private static class Serializer implements IVersionedSerializer<ReadCommand>
    {
        private static int digestFlag(boolean isDigest)
        {
            return isDigest ? 0x01 : 0;
        }

        private static boolean isDigest(int flags)
        {
            return (flags & 0x01) != 0;
        }

        private static int thriftFlag(boolean isForThrift)
        {
            return isForThrift ? 0x02 : 0;
        }

        private static boolean isForThrift(int flags)
        {
            return (flags & 0x02) != 0;
        }

        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO special behavior for paged?
                if (command.kind == Kind.SINGLE_PARTITION)
                    legacyReadCommandSerializer.serialize(command, out, version);
                else
                    legacyRangeSliceCommandSerializer.serialize(command, out, version);

                return;
            }

            out.writeByte(command.kind.ordinal());
            out.writeByte(digestFlag(command.isDigestQuery()) | thriftFlag(command.isForThrift()));
            CFMetaData.serializer.serialize(command.metadata(), out, version);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(), out, version);
            RowFilter.serializer.serialize(command.rowFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                return legacyReadCommandSerializer.deserialize(in, version);

            Kind kind = Kind.values()[in.readByte()];
            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            boolean isForThrift = isForThrift(flags);
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);
            RowFilter rowFilter = RowFilter.serializer.deserialize(in, version, metadata);
            DataLimits limits = DataLimits.serializer.deserialize(in, version);

            return kind.selectionDeserializer.deserialize(in, version, isDigest, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                if (command.kind == Kind.SINGLE_PARTITION)
                    return legacyReadCommandSerializer.serializedSize(command, version);
                else
                    return legacyRangeSliceCommandSerializer.serializedSize(command, version);
            }

            return 2 // kind + flags
                 + CFMetaData.serializer.serializedSize(command.metadata(), version)
                 + TypeSizes.sizeof(command.nowInSec())
                 + ColumnFilter.serializer.serializedSize(command.columnFilter(), version)
                 + RowFilter.serializer.serializedSize(command.rowFilter(), version)
                 + DataLimits.serializer.serializedSize(command.limits(), version)
                 + command.selectionSerializedSize(version);
        }
    }

    private enum LegacyType
    {
        GET_BY_NAMES((byte)1),
        GET_SLICES((byte)2);

        public final byte serializedValue;

        private LegacyType(byte b)
        {
            this.serializedValue = b;
        }
        public static LegacyType fromPartitionFilterKind(ClusteringIndexFilter.Kind kind)
        {
            return kind == ClusteringIndexFilter.Kind.SLICE
                   ? GET_SLICES
                   : GET_BY_NAMES;
        }

        public static LegacyType fromSerializedValue(byte b)
        {
            return b == 1 ? GET_BY_NAMES : GET_SLICES;
        }
    }

    /*
     * Deserialize pre-3.0 RangeSliceCommand for backward compatibility sake
     */
    private static class LegacyRangeSliceCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            rangeCommand = LegacyReadCommandSerializer.maybeConvertNamesToSlice(rangeCommand);

            CFMetaData metadata = rangeCommand.metadata();

            out.writeUTF(metadata.ksName);
            out.writeUTF(metadata.cfName);
            out.writeLong(rangeCommand.nowInSec() * 1000L);  // convert from seconds to millis

            // begin DiskAtomFilterSerializer.serialize()
            if (rangeCommand.isNamesQuery())
            {
                out.writeByte(1);  // 0 for slices, 1 for names
                ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter;
                LegacyReadCommandSerializer.serializeNamesFilter(rangeCommand, filter, out);
            }
            else
            {
                out.writeByte(0);  // 0 for slices, 1 for names

                // slice filter serialization
                ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

                boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                LegacyReadCommandSerializer.serializeSlices(out, filter.requestedSlices(), filter.isReversed(), makeStaticSlice, metadata);

                out.writeBoolean(filter.isReversed());

                // limit
                DataLimits.Kind kind = rangeCommand.limits().kind();
                boolean isDistinct = (kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && rangeCommand.limits().perPartitionCount() == 1;
                if (isDistinct)
                    out.writeInt(1);
                else
                    out.writeInt(LegacyReadCommandSerializer.updateLimitForQuery(rangeCommand.limits().count(), filter.requestedSlices()));

                int compositesToGroup;
                boolean selectsStatics = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() || filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                if (kind == DataLimits.Kind.THRIFT_LIMIT)
                    compositesToGroup = -1;
                else if (isDistinct && !selectsStatics)
                    compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
                else
                    compositesToGroup = metadata.isDense() ? -1 : metadata.clusteringColumns().size();

                out.writeInt(compositesToGroup);
            }

            serializeRowFilter(out, rangeCommand.rowFilter());

            // key range serialization
            AbstractBounds.rowPositionSerializer.serialize(rangeCommand.dataRange().keyRange(), out, version);
            out.writeInt(rangeCommand.limits().count());  // maxResults
            boolean countCQL3Rows = true;
            if (rangeCommand.isForThrift())
                countCQL3Rows = false;
            else if (rangeCommand.limits().perPartitionCount() == 1)
                countCQL3Rows = false;  // for DISTINCT queries
            out.writeBoolean(countCQL3Rows);

            assert !rangeCommand.dataRange().isPaging();
            out.writeBoolean(false);  // isPaging
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            if (metadata == null)
            {
                String message = String.format("Got legacy range command for nonexistent table %s.%s.", keyspace, columnFamily);
                throw new UnknownColumnFamilyException(message, null);
            }

            int nowInSec = (int) (in.readLong() / 1000);  // convert from millis to seconds

            try
            {
                ClusteringIndexFilter filter;
                ColumnFilter selection;
                int compositesToGroup = 0;
                int perPartitionLimit = -1;
                byte readType = in.readByte();  // 0 for slices, 1 for names
                if (readType == 1)
                {
                    // TODO unify with single-partition names query deser
                    int numCellNames = in.readInt();
                    NavigableSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
                    for (int i = 0; i < numCellNames; i++)
                    {
                        ByteBuffer buffer = ByteBufferUtil.readWithShortLength(in);
                        LegacyLayout.LegacyCellName cellName = LegacyLayout.decodeCellName(metadata, buffer);
                        if (!cellName.clustering.equals(Clustering.STATIC_CLUSTERING))
                            clusterings.add(cellName.clustering);
                    }

                    in.readBoolean();  // countCql3Rows

                    selection = ColumnFilter.all(metadata);
                    filter = new ClusteringIndexNamesFilter(clusterings, false);
                }
                else
                {
                    filter = LegacyReadCommandSerializer.deserializeSlicePartitionFilter(in, metadata);
                    perPartitionLimit = in.readInt();
                    compositesToGroup = in.readInt();

                    if (compositesToGroup == -2)
                    {
                        selection = ColumnFilter.selection(PartitionColumns.NONE);
                        filter = new ClusteringIndexSliceFilter(((ClusteringIndexSliceFilter) filter).requestedSlices(), filter.isReversed());
                    }
                    else
                    {
                        // if a slice query from a pre-3.0 node doesn't cover statics, we shouldn't select them at all
                        PartitionColumns columns = filter.selects(Clustering.STATIC_CLUSTERING)
                                                 ? metadata.partitionColumns()
                                                 : metadata.partitionColumns().withoutStatics();
                        selection = ColumnFilter.selection(columns);
                    }
                }

                RowFilter rowFilter = deserializeRowFilter(in, metadata);

                AbstractBounds<PartitionPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, StorageService.getPartitioner(), version);
                int maxResults = in.readInt();

                // TODO what needs to be done for these?
                in.readBoolean();  // countCQL3Rows
                in.readBoolean();  // isPaging

                boolean selectsStatics = (!selection.fetchedColumns().statics.isEmpty() || filter.selects(Clustering.STATIC_CLUSTERING));
                boolean isDistinct = compositesToGroup == -2 || (perPartitionLimit == 1 && selectsStatics);
                DataLimits limits;
                if (isDistinct)
                    limits = DataLimits.distinctLimits(maxResults);
                else if (compositesToGroup == -1)
                    limits = DataLimits.thriftLimits(maxResults, perPartitionLimit);
                else
                    limits = DataLimits.cqlLimits(maxResults);

                return new PartitionRangeReadCommand(false, true, metadata, nowInSec, selection, rowFilter, limits, new DataRange(keyRange, filter));
            }
            catch (UnknownColumnException exc)
            {
                // TODO what to do?
                throw new RuntimeException(exc);
            }
        }

        static void serializeRowFilter(DataOutputPlus out, RowFilter rowFilter) throws IOException
        {
            ArrayList<RowFilter.Expression> indexExpressions = Lists.newArrayList(rowFilter.iterator());
            out.writeInt(indexExpressions.size());
            for (RowFilter.Expression expression : indexExpressions)
            {
                ByteBufferUtil.writeWithShortLength(expression.column().name.bytes, out);
                expression.operator().writeTo(out);
                ByteBufferUtil.writeWithShortLength(expression.getIndexValue(), out);
            }
        }

        static long serializedRowFilterSize(RowFilter rowFilter)
        {
            long size = TypeSizes.sizeof(0);  // rowFilterCount
            for (RowFilter.Expression expression : rowFilter)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes);
                size += TypeSizes.sizeof(0);  // operator int value
                size += ByteBufferUtil.serializedSizeWithShortLength(expression.getIndexValue());
            }
            return size;
        }

        static RowFilter deserializeRowFilter(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int numRowFilters = in.readInt();
            if (numRowFilters == 0)
                return RowFilter.NONE;

            RowFilter rowFilter = RowFilter.create(numRowFilters);
            for (int i = 0; i < numRowFilters; i++)
            {
                ByteBuffer columnName = ByteBufferUtil.readWithShortLength(in);
                ColumnDefinition column = metadata.getColumnDefinition(columnName);
                Operator op = Operator.readFrom(in);
                ByteBuffer indexValue = ByteBufferUtil.readWithShortLength(in);
                rowFilter.add(column, op, indexValue);
            }
            return rowFilter;
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.PARTITION_RANGE;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            rangeCommand = LegacyReadCommandSerializer.maybeConvertNamesToSlice(rangeCommand);
            CFMetaData metadata = rangeCommand.metadata();

            long size = TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof(metadata.cfName);
            size += TypeSizes.sizeof((long) rangeCommand.nowInSec());

            size += 1;  // single byte flag: 0 for slices, 1 for names
            if (rangeCommand.isNamesQuery())
            {
                PartitionColumns columns = rangeCommand.columnFilter().fetchedColumns();
                ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter;
                size += LegacyReadCommandSerializer.serializedNamesFilterSize(filter, metadata, columns);
            }
            else
            {
                ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;
                boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                size += LegacyReadCommandSerializer.serializedSlicesSize(filter.requestedSlices(), makeStaticSlice, metadata);
                size += TypeSizes.sizeof(filter.isReversed());
                size += TypeSizes.sizeof(rangeCommand.limits().perPartitionCount());
                size += TypeSizes.sizeof(0); // compositesToGroup
            }

            if (rangeCommand.rowFilter().equals(RowFilter.NONE))
            {
                size += TypeSizes.sizeof(0);
            }
            else
            {
                ArrayList<RowFilter.Expression> indexExpressions = Lists.newArrayList(rangeCommand.rowFilter().iterator());
                size += TypeSizes.sizeof(indexExpressions.size());
                for (RowFilter.Expression expression : indexExpressions)
                {
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes);
                    size += TypeSizes.sizeof(expression.operator().ordinal());
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.getIndexValue());
                }
            }

            size += AbstractBounds.rowPositionSerializer.serializedSize(rangeCommand.dataRange().keyRange(), version);
            size += TypeSizes.sizeof(rangeCommand.limits().count());
            size += TypeSizes.sizeof(!rangeCommand.isForThrift());
            return size + TypeSizes.sizeof(rangeCommand.dataRange().isPaging());
        }
    }

    /*
     * Deserialize pre-3.0 PagedRangeCommand for backward compatibility sake
     */
    private static class LegacyPagedRangeCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            assert rangeCommand.dataRange().isPaging();
            CFMetaData metadata = rangeCommand.metadata();

            out.writeUTF(metadata.ksName);
            out.writeUTF(metadata.cfName);
            out.writeLong(rangeCommand.nowInSec() * 1000L);  // convert from seconds to millis

            AbstractBounds.rowPositionSerializer.serialize(rangeCommand.dataRange().keyRange(), out, version);

            // pre-3.0 nodes only accept slice filters for paged range commands
            ClusteringIndexSliceFilter filter;
            if (rangeCommand.dataRange().clusteringIndexFilter.kind() == ClusteringIndexFilter.Kind.NAMES)
                filter = LegacyReadCommandSerializer.convertNamesFilterToSliceFilter((ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter, metadata);
            else
                filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

            // slice filter
            boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            LegacyReadCommandSerializer.serializeSlices(out, filter.requestedSlices(), filter.isReversed(), makeStaticSlice, metadata);
            out.writeBoolean(filter.isReversed());

            // slice filter count
            DataLimits.Kind kind = rangeCommand.limits().kind();
            boolean isDistinct = (kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && rangeCommand.limits().perPartitionCount() == 1;
            if (isDistinct)
                out.writeInt(1);
            else
                out.writeInt(LegacyReadCommandSerializer.updateLimitForQuery(rangeCommand.limits().perPartitionCount(), filter.requestedSlices()));

            // compositesToGroup
            boolean selectsStatics = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() || filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            int compositesToGroup;
            if (kind == DataLimits.Kind.THRIFT_LIMIT)
                compositesToGroup = -1;
            else if (isDistinct && !selectsStatics)
                compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
            else
                compositesToGroup = metadata.isDense() ? -1 : metadata.clusteringColumns().size();

            out.writeInt(compositesToGroup);

            // command-level "start" and "stop" composites.  The start is the last-returned cell name if there is one,
            // otherwise it's the same as the slice filter's start.  The stop appears to always be the same as the
            // slice filter's stop.
            DataRange.Paging pagingRange = (DataRange.Paging) rangeCommand.dataRange();
            Clustering lastReturned = pagingRange.getLastReturned();
            Slice.Bound newStart = Slice.Bound.exclusiveStartOf(lastReturned);
            Slice lastSlice = filter.requestedSlices().get(filter.requestedSlices().size() - 1);
            ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeBound(metadata, newStart, true), out);
            ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeClustering(metadata, lastSlice.end().clustering()), out);

            LegacyRangeSliceCommandSerializer.serializeRowFilter(out, rangeCommand.rowFilter());

            // command-level limit
            // TODO: pre-3.0 we would always request one more row than we actually needed, but the command-level "start" would
            // be the last-returned cell name, so the response would always include it.  Instead, here we are using the
            // exact count we need along with an exclusive "start" to avoid re-fetching the last-returned cell.  I'm
            // not confident that this will not cause problems.
            out.writeInt(rangeCommand.limits().count());

            // countCQL3Rows
            boolean countCQL3Rows = true;
            if (rangeCommand.isForThrift())
                countCQL3Rows = false;
            else if (rangeCommand.limits().perPartitionCount() == 1)
                countCQL3Rows = false;  // for DISTINCT queries
            out.writeBoolean(countCQL3Rows);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            if (metadata == null)
            {
                String message = String.format("Got legacy paged range command for nonexistent table %s.%s.", keyspace, columnFamily);
                throw new UnknownColumnFamilyException(message, null);
            }

            int nowInSec = (int) (in.readLong() / 1000);  // convert from millis to seconds
            AbstractBounds<PartitionPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, StorageService.getPartitioner(), version);

            ClusteringIndexSliceFilter filter = LegacyReadCommandSerializer.deserializeSlicePartitionFilter(in, metadata);
            int perPartitionLimit = in.readInt();
            int compositesToGroup = in.readInt();

            // TODO what to do with the stop?
            LegacyLayout.LegacyBound startBound = LegacyLayout.decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), true);
            LegacyLayout.LegacyBound stopBound = LegacyLayout.decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), false);
            Clustering startClustering = startBound == LegacyLayout.LegacyBound.BOTTOM
                                       ? Clustering.EMPTY
                                       : startBound.getAsClustering(metadata);
            Clustering stopClustering = stopBound == LegacyLayout.LegacyBound.TOP
                                      ? Clustering.EMPTY
                                      : stopBound.getAsClustering(metadata);

            ColumnFilter selection;
            if (compositesToGroup == -2)
            {
                selection = ColumnFilter.selection(PartitionColumns.NONE);
            }
            else
            {
                // if a slice query from a pre-3.0 node doesn't cover statics, we shouldn't select them at all
                PartitionColumns columns = filter.selects(Clustering.STATIC_CLUSTERING)
                                         ? metadata.partitionColumns()
                                         : metadata.partitionColumns().withoutStatics();
                selection = ColumnFilter.selection(columns);
            }

            RowFilter rowFilter = LegacyRangeSliceCommandSerializer.deserializeRowFilter(in, metadata);
            int maxResults = in.readInt();
            boolean countCQL3Rows = in.readBoolean(); // countCQL3Rows


            boolean selectsStatics = (!selection.fetchedColumns().statics.isEmpty() || filter.selects(Clustering.STATIC_CLUSTERING));
            boolean isDistinct = compositesToGroup == -2 || (perPartitionLimit == 1 && selectsStatics);
            DataLimits limits;
            if (isDistinct)
                limits = DataLimits.distinctLimits(maxResults);
            else if (compositesToGroup == -1)
                limits = DataLimits.thriftLimits(1, perPartitionLimit); // we only use paging w/ thrift for get_count(), so partition limit must be 1
            else
                limits = DataLimits.cqlLimits(maxResults);

            limits = limits.forPaging(maxResults);

            // TODO should "inclusive" always be false?
            DataRange dataRange = new DataRange(keyRange, filter).forPaging(keyRange, metadata.comparator, startClustering, false);
            ReadCommand command = new PartitionRangeReadCommand(false, true, metadata, nowInSec, selection, rowFilter, limits, dataRange);
            logger.warn("#### deserializing legacy partition range read command: {}", command);
            return command;
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.PARTITION_RANGE;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            CFMetaData metadata = rangeCommand.metadata();
            assert rangeCommand.dataRange().isPaging();

            long size = TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof(metadata.cfName);
            size += TypeSizes.sizeof((long) rangeCommand.nowInSec());

            size += AbstractBounds.rowPositionSerializer.serializedSize(rangeCommand.dataRange().keyRange(), version);

            // pre-3.0 nodes only accept slice filters for paged range commands
            ClusteringIndexSliceFilter filter;
            if (rangeCommand.dataRange().clusteringIndexFilter.kind() == ClusteringIndexFilter.Kind.NAMES)
                filter = LegacyReadCommandSerializer.convertNamesFilterToSliceFilter((ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter, metadata);
            else
                filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

            // slice filter
            boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            size += LegacyReadCommandSerializer.serializedSlicesSize(filter.requestedSlices(), makeStaticSlice, metadata);
            size += TypeSizes.sizeof(filter.isReversed());

            // slice filter's count
            size += TypeSizes.sizeof(rangeCommand.limits().perPartitionCount());

            // compositesToGroup
            size += TypeSizes.sizeof(0);

            DataRange.Paging pagingRange = (DataRange.Paging) rangeCommand.dataRange();
            Clustering lastReturned = pagingRange.getLastReturned();
            Slice lastSlice = filter.requestedSlices().get(filter.requestedSlices().size() - 1);
            ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeClustering(metadata, lastReturned));
            ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeClustering(metadata, lastSlice.end().clustering()));

            size += LegacyRangeSliceCommandSerializer.serializedRowFilterSize(rangeCommand.rowFilter());

            // command-level limit
            size += TypeSizes.sizeof(rangeCommand.limits().count());

            // countCQL3Rows
            return size + TypeSizes.sizeof(true);
        }
    }

    // From old ReadCommand
    static class LegacyReadCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert command.kind == Kind.SINGLE_PARTITION;

            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;
            singleReadCommand = maybeConvertNamesToSlice(singleReadCommand);

            CFMetaData metadata = singleReadCommand.metadata();

            out.writeByte(LegacyType.fromPartitionFilterKind(singleReadCommand.clusteringIndexFilter().kind()).serializedValue);

            out.writeBoolean(singleReadCommand.isDigestQuery());
            out.writeUTF(metadata.ksName);
            ByteBufferUtil.writeWithShortLength(singleReadCommand.partitionKey().getKey(), out);
            out.writeUTF(metadata.cfName);
            out.writeLong(singleReadCommand.nowInSec() * 1000L);  // convert from seconds to millis

            if (singleReadCommand.clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.SLICE)
                serializeSliceCommand((SinglePartitionSliceCommand) singleReadCommand, out);
            else
                serializeNamesCommand((SinglePartitionNamesCommand) singleReadCommand, out);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            LegacyType msgType = LegacyType.fromSerializedValue(in.readByte());

            boolean isDigest = in.readBoolean();
            String keyspaceName = in.readUTF();
            DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
            String cfName = in.readUTF();
            long nowInMillis = in.readLong();
            int nowInSeconds = (int) (nowInMillis / 1000);
            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

            try
            {
                switch (msgType)
                {
                    case GET_BY_NAMES:
                        return deserializeNamesCommand(in, isDigest, metadata, key, nowInSeconds);
                    case GET_SLICES:
                        return deserializeSliceCommand(in, isDigest, metadata, key, nowInSeconds);
                    default:
                        throw new AssertionError();
                }
            }
            catch (UnknownColumnException exc)
            {
                // TODO what to do with an unknown column?
                throw new RuntimeException(exc);
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert command.kind == Kind.SINGLE_PARTITION;
            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;
            singleReadCommand = maybeConvertNamesToSlice(singleReadCommand);

            int keySize = singleReadCommand.partitionKey().getKey().remaining();

            CFMetaData metadata = singleReadCommand.metadata();

            long size = 1;  // message type (single byte)
            size += TypeSizes.sizeof(command.isDigestQuery());
            size += TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof((short) keySize) + keySize;
            size += TypeSizes.sizeof((long) command.nowInSec());

            if (singleReadCommand.clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.SLICE)
                return size + serializedSliceCommandSize((SinglePartitionSliceCommand) singleReadCommand);
            else
                return size + serializedNamesCommandSize((SinglePartitionNamesCommand) singleReadCommand);
        }

        private void serializeNamesCommand(SinglePartitionNamesCommand command, DataOutputPlus out) throws IOException
        {
            serializeNamesFilter(command, command.clusteringIndexFilter(), out);
        }

        private SinglePartitionReadCommand maybeConvertNamesToSlice(SinglePartitionReadCommand command)
        {
            if (command.clusteringIndexFilter().kind() != ClusteringIndexFilter.Kind.NAMES)
                return command;

            CFMetaData metadata = command.metadata();
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            ClusteringIndexNamesFilter filter = ((SinglePartitionNamesCommand) command).clusteringIndexFilter();


            // On pre-3.0 nodes, due to CASSANDRA-5762, we always do a slice for CQL3 tables (not dense, composite).
            boolean shouldConvert = (!metadata.isDense() && metadata.isCompound());
            if (!shouldConvert)
            {
                // pre-3.0 nodes don't support names filters for reading collections, so if we're requesting any of those,
                // we need to convert this to a slice filter
                for (ColumnDefinition column : columns)
                {
                    if (column.type.isMultiCell())
                    {
                        shouldConvert = true;
                        break;
                    }
                }
            }

            if (!shouldConvert)
                return command;

            ClusteringIndexSliceFilter sliceFilter = convertNamesFilterToSliceFilter(filter, metadata);
            return new SinglePartitionSliceCommand(
                    command.isDigestQuery(), command.isForThrift(), metadata, command.nowInSec(),
                    ColumnFilter.selection(columns), command.rowFilter(), command.limits(), command.partitionKey(), sliceFilter);
        }

        static PartitionRangeReadCommand maybeConvertNamesToSlice(PartitionRangeReadCommand command)
        {
            if (!command.dataRange().isNamesQuery())
                return command;

            CFMetaData metadata = command.metadata();
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) command.dataRange().clusteringIndexFilter;

            // On pre-3.0 nodes, due to CASSANDRA-5762, we always do a slice for CQL3 tables (not dense, composite).
            boolean shouldConvert = (!metadata.isDense() && metadata.isCompound());
            if (!shouldConvert)
            {
                // pre-3.0 nodes don't support names filters for reading collections, so if we're requesting any of those,
                // we need to convert this to a slice filter
                for (ColumnDefinition column : columns)
                {
                    if (column.type.isMultiCell())
                    {
                        shouldConvert = true;
                        break;
                    }
                }
            }

            if (!shouldConvert)
                return command;

            ClusteringIndexSliceFilter sliceFilter = convertNamesFilterToSliceFilter(filter, metadata);
            DataRange newRange = new DataRange(command.dataRange().keyRange(), sliceFilter);
            return new PartitionRangeReadCommand(
                    command.isDigestQuery(), command.isForThrift(), metadata, command.nowInSec(),
                    ColumnFilter.selection(columns), command.rowFilter(), command.limits(), newRange);
        }

        static ClusteringIndexSliceFilter convertNamesFilterToSliceFilter(ClusteringIndexNamesFilter filter, CFMetaData metadata)
        {
            SortedSet<Clustering> requestedRows = filter.requestedRows();
            Slices slices;
            if (requestedRows.isEmpty() || requestedRows.size() == 1 && requestedRows.first().size() == 0)
            {
                slices = Slices.ALL;
            }
            else
            {
                Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
                for (Clustering clustering : requestedRows)
                    slicesBuilder.add(Slice.Bound.inclusiveStartOf(clustering), Slice.Bound.inclusiveEndOf(clustering));
                slices = slicesBuilder.build();
            }

            return new ClusteringIndexSliceFilter(slices, filter.isReversed());
        }

        public static void serializeNamesFilter(ReadCommand command, ClusteringIndexNamesFilter filter, DataOutputPlus out) throws IOException
        {
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            CFMetaData metadata = command.metadata();
            SortedSet<Clustering> requestedRows = filter.requestedRows();

            if (requestedRows.isEmpty())
            {
                // only static columns are requested
                out.writeInt(columns.size());
                for (ColumnDefinition column : columns)
                    ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
            }
            else
            {
                out.writeInt(requestedRows.size() * columns.size());
                for (Clustering clustering : requestedRows)
                {
                    for (ColumnDefinition column : columns)
                        ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null), out);
                }
            }

            // countCql3Rows should be true if it's not a DISTINCT query and it's fetching a range of cells, meaning
            // one of the following is true:
            //  * it's a sparse, simple table
            //  * it's a dense table, but the clustering columns have been fully specified
            // We only use ClusteringIndexNamesFilter when the clustering columns have been fully specified, so we can
            // combine the last two cases into a check for compact storage.
            if (metadata.isCompactTable() && !(command.limits().kind() == DataLimits.Kind.CQL_LIMIT && command.limits().perPartitionCount() == 1))
                out.writeBoolean(true);  // it's compact and not a DISTINCT query
            else
                out.writeBoolean(false);
        }

        public static long serializedNamesFilterSize(ClusteringIndexNamesFilter filter, CFMetaData metadata, PartitionColumns fetchedColumns)
        {
            SortedSet<Clustering> requestedRows = filter.requestedRows();

            long size = 0;
            if (requestedRows.isEmpty())
            {
                // only static columns are requested
                size += TypeSizes.sizeof(fetchedColumns.size());
                for (ColumnDefinition column : fetchedColumns)
                    size += ByteBufferUtil.serializedSizeWithShortLength(column.name.bytes);
            }
            else
            {
                size += TypeSizes.sizeof(requestedRows.size() * fetchedColumns.size());
                for (Clustering clustering : requestedRows)
                {
                    for (ColumnDefinition column : fetchedColumns)
                        size += ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null));
                }
            }

            return size + TypeSizes.sizeof(true);  // countCql3Rows
        }

        public long serializedNamesCommandSize(SinglePartitionNamesCommand command)
        {
            ClusteringIndexNamesFilter filter = command.clusteringIndexFilter();
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            return serializedNamesFilterSize(filter, command.metadata(), columns);
        }

        private SinglePartitionNamesCommand deserializeNamesCommand(DataInputPlus in, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds) throws IOException, UnknownColumnException
        {
            int numCellNames = in.readInt();
            NavigableSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
            Set<ColumnDefinition> staticColumns = new HashSet<>();
            Set<ColumnDefinition> columns = new HashSet<>();
            for (int i = 0; i < numCellNames; i++)
            {
                ByteBuffer buffer = ByteBufferUtil.readWithShortLength(in);
                LegacyLayout.LegacyCellName cellName = LegacyLayout.decodeCellName(metadata, buffer);
                if (!cellName.clustering.equals(Clustering.STATIC_CLUSTERING))
                {
                    clusterings.add(cellName.clustering);
                    columns.add(cellName.column);
                }
                else
                {
                    staticColumns.add(cellName.column);
                }
            }

            in.readBoolean();  // countCql3Rows

            ColumnFilter selection = ColumnFilter.selection(new PartitionColumns(Columns.from(staticColumns), Columns.from(columns)));
            ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionNamesCommand(isDigest, true, metadata, nowInSeconds, selection, RowFilter.NONE, DataLimits.NONE, key, filter);
        }

        static int updateLimitForQuery(int limit, Slices slices)
        {
            // Pre-3.0 nodes don't support exclusive bounds for slices. Instead, we query one more element if necessary
            // and filter it later (in LegacyRemoteDataResponse)
            if (!slices.hasLowerBound() && ! slices.hasUpperBound())
                return limit;

            for (Slice slice : slices)
            {
                if (limit == Integer.MAX_VALUE)
                    return limit;

                if (!slice.start().isInclusive())
                    limit++;
                if (!slice.end().isInclusive())
                    limit++;
            }
            return limit;
        }

        private void serializeSliceCommand(SinglePartitionSliceCommand command, DataOutputPlus out) throws IOException
        {
            CFMetaData metadata = command.metadata();
            ClusteringIndexSliceFilter filter = command.clusteringIndexFilter();

            Slices slices = filter.requestedSlices();
            boolean makeStaticSlice = !command.columnFilter().fetchedColumns().statics.isEmpty() && !slices.selects(Clustering.STATIC_CLUSTERING);
            serializeSlices(out, slices, filter.isReversed(), makeStaticSlice, metadata);

            out.writeBoolean(filter.isReversed());

            boolean selectsStatics = !command.columnFilter().fetchedColumns().statics.isEmpty() || slices.selects(Clustering.STATIC_CLUSTERING);
            DataLimits.Kind kind = command.limits().kind();
            boolean isDistinct = (kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && command.limits().perPartitionCount() == 1;
            if (isDistinct)
                out.writeInt(1);
            else
                out.writeInt(updateLimitForQuery(command.limits().count(), filter.requestedSlices()));

            int compositesToGroup;
            if (kind == DataLimits.Kind.THRIFT_LIMIT)
                compositesToGroup = -1;
            else if (isDistinct && !selectsStatics)
                compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
            else
                compositesToGroup = metadata.isDense() ? -1 : metadata.clusteringColumns().size();

            out.writeInt(compositesToGroup);
        }

        private static void serializeSlice(DataOutputPlus out, Slice slice, boolean isReversed, CFMetaData metadata) throws IOException
        {
            ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, isReversed ? slice.end() : slice.start(), !isReversed);
            ByteBufferUtil.writeWithShortLength(sliceStart, out);

            ByteBuffer sliceEnd = LegacyLayout.encodeBound(metadata, isReversed ? slice.start() : slice.end(), isReversed);
            ByteBufferUtil.writeWithShortLength(sliceEnd, out);
        }

        private static void serializeStaticSlice(DataOutputPlus out, boolean isReversed, CFMetaData metadata) throws IOException
        {
            if (!isReversed)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
                ByteBufferUtil.writeWithShortLength(sliceStart, out);
            }

            out.writeShort(2 + metadata.comparator.size() * 3);  // two bytes + EOC for each component, plus static prefix
            out.writeShort(LegacyLayout.STATIC_PREFIX);
            for (int i = 0; i < metadata.comparator.size(); i++)
            {
                ByteBufferUtil.writeWithShortLength(ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
                out.writeByte(i == metadata.comparator.size() - 1 ? 1 : 0);
            }

            if (isReversed)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
                ByteBufferUtil.writeWithShortLength(sliceStart, out);
            }
        }

        static void serializeSlices(DataOutputPlus out, Slices slices, boolean isReversed, boolean makeStaticSlice, CFMetaData metadata) throws IOException
        {
            out.writeInt(slices.size() + (makeStaticSlice ? 1 : 0));

            // In 3.0 we always store the slices in normal comparator order.  Pre-3.0 nodes expect the slices to
            // be in reversed order if the query is reversed, so we handle that here.
            if (isReversed)
            {
                for (int i = slices.size() - 1; i >= 0; i--)
                    serializeSlice(out, slices.get(i), true, metadata);
                if (makeStaticSlice)
                    serializeStaticSlice(out, true, metadata);
            }
            else
            {
                if (makeStaticSlice)
                    serializeStaticSlice(out, false, metadata);
                for (Slice slice : slices)
                    serializeSlice(out, slice, false, metadata);
            }
        }

        public long serializedSliceCommandSize(SinglePartitionSliceCommand command)
        {
            CFMetaData metadata = command.metadata();
            ClusteringIndexSliceFilter filter = command.clusteringIndexFilter();

            Slices slices = filter.requestedSlices();
            boolean makeStaticSlice = !command.columnFilter().fetchedColumns().statics.isEmpty() && !slices.selects(Clustering.STATIC_CLUSTERING);

            long size = serializedSlicesSize(slices, makeStaticSlice, metadata);
            size += TypeSizes.sizeof(command.clusteringIndexFilter().isReversed());
            size += TypeSizes.sizeof(command.limits().count());
            return size + TypeSizes.sizeof(0);  // compositesToGroup
        }

        static long serializedSlicesSize(Slices slices, boolean makeStaticSlice, CFMetaData metadata)
        {
            long size = TypeSizes.sizeof(slices.size());

            for (Slice slice : slices)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, slice.start(), true);
                size += ByteBufferUtil.serializedSizeWithShortLength(sliceStart);
                ByteBuffer sliceEnd = LegacyLayout.encodeBound(metadata, slice.end(), false);
                size += ByteBufferUtil.serializedSizeWithShortLength(sliceEnd);
            }

            if (makeStaticSlice)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
                size += ByteBufferUtil.serializedSizeWithShortLength(sliceStart);

                size += TypeSizes.sizeof((short) (metadata.comparator.size() * 3 + 2));
                size += TypeSizes.sizeof((short) LegacyLayout.STATIC_PREFIX);
                for (int i = 0; i < metadata.comparator.size(); i++)
                {
                    size += ByteBufferUtil.serializedSizeWithShortLength(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                    size += 1;  // EOC
                }
            }

            return size;
        }

        private SinglePartitionSliceCommand deserializeSliceCommand(DataInputPlus in, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds) throws IOException, UnknownColumnException
        {
            ClusteringIndexSliceFilter filter = deserializeSlicePartitionFilter(in, metadata);
            int count = in.readInt();
            int compositesToGroup = in.readInt();

            // if a slice query from a pre-3.0 node doesn't cover statics, we shouldn't select them at all
            boolean selectsStatics = filter.selects(Clustering.STATIC_CLUSTERING);
            PartitionColumns columns = selectsStatics
                                     ? metadata.partitionColumns()
                                     : metadata.partitionColumns().withoutStatics();

            boolean isDistinct = compositesToGroup == -2 || (count == 1 && selectsStatics);
            DataLimits limits;
            if (compositesToGroup == -2 || isDistinct)
                limits = DataLimits.distinctLimits(count);  // See CASSANDRA-8490 for the explanation of this value
            else if (compositesToGroup == -1)
                limits = DataLimits.thriftLimits(1, count);
            else
                limits = DataLimits.cqlLimits(count);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionSliceCommand(isDigest, true, metadata, nowInSeconds, ColumnFilter.selection(columns), RowFilter.NONE, limits, key, filter);
        }

        static ClusteringIndexSliceFilter deserializeSlicePartitionFilter(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int numSlices = in.readInt();
            ByteBuffer[] startBuffers = new ByteBuffer[numSlices];
            ByteBuffer[] finishBuffers = new ByteBuffer[numSlices];
            for (int i = 0; i < numSlices; i++)
            {
                startBuffers[i] = ByteBufferUtil.readWithShortLength(in);
                finishBuffers[i] = ByteBufferUtil.readWithShortLength(in);
            }

            boolean reversed = in.readBoolean();

            Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
            for (int i = 0; i < numSlices; i++)
            {
                Slice.Bound start, finish;
                if (!reversed)
                {
                    start = LegacyLayout.decodeBound(metadata, startBuffers[i], true).bound;
                    finish = LegacyLayout.decodeBound(metadata, finishBuffers[i], false).bound;
                }
                else
                {
                    finish = LegacyLayout.decodeBound(metadata, startBuffers[i], false).bound;
                    start = LegacyLayout.decodeBound(metadata, finishBuffers[i], true).bound;
                }
                slicesBuilder.add(Slice.make(start, finish));
            }

            ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slicesBuilder.build(), reversed);
            return filter;
        }
    }
}
