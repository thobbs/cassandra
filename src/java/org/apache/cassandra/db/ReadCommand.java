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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
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
    private final DataLimits limits;

    private boolean isDigestQuery;
    private final boolean isForThrift;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInput in, int version, boolean isDigest, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, DataLimits limits) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private SelectionDeserializer selectionDeserializer;

        private Kind(SelectionDeserializer selectionDeserializer)
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
                          DataLimits limits)
    {
        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.isForThrift = isForThrift;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
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

    // Filters on CQL columns (will be handled either by a 2ndary index if
    // there is one, or by on-replica filtering otherwise)
    /**
     * Filters/Resrictions on CQL columns.
     * <p>
     * This contains those restrictions that are not directly handled by the
     * {@code PartitionFilter}. More specifically, this includes any non-PK columns
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirely by the partition filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the restrictions on CQL columns that aren't directly satisfied by the
     * underlying {@code PartitionFilter} of this command.
     */
    public ColumnFilter columnFilter()
    {
        return columnFilter;
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
     * The columns queried by this command.
     *
     * @return the columns queried by this command.
     */
    public abstract ColumnsSelection queriedColumns();

    /**
     * The partition filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code PartitionFilter} to use for the partition of key {@code key}.
     */
    public abstract PartitionFilter partitionFilter(DecoratedKey key);

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

    protected abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs);

    public ReadResponse makeResponse(UnfilteredPartitionIterator iter, boolean isLocalDataQuery)
    {
        if (isDigestQuery())
            return ReadResponse.createDigestResponse(iter);
        else if (isLocalDataQuery)
            return ReadResponse.createLocalDataResponse(iter);
        else
            return ReadResponse.createDataResponse(iter);
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
    public UnfilteredPartitionIterator executeLocally(ColumnFamilyStore cfs)
    {
        SecondaryIndexSearcher searcher = getIndexSearcher(cfs);
        UnfilteredPartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs)
                                         : searcher.search(this);

        try
        {
            resultIterator = withMetricsRecording(withoutExpiredTombstones(resultIterator, cfs), cfs.metric);

            // TODO: we should push the dropping of columns down the layers because
            // 1) it'll be more efficient
            // 2) it could help us solve #6276
            // But there is not reason not to do this as a followup so keeping it here for now (we'll have
            // to be wary of cached row if we move this down the layers)
            if (!metadata().getDroppedColumns().isEmpty())
                resultIterator = UnfilteredPartitionIterators.removeDroppedColumns(resultIterator, metadata().getDroppedColumns());

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            ColumnFilter updatedFilter = searcher == null
                                       ? columnFilter()
                                       : columnFilter().without(searcher.primaryClause(this));

            // TODO: We'll currently do filtering by the columnFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return limits().filter(columnFilter().filter(resultIterator));
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    public PartitionIterator executeLocally()
    {
        return UnfilteredPartitionIterators.filter(executeLocally(Keyspace.openAndGetStore(metadata())));
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final ColumnFamilyMetrics metric)
    {
        return new WrappingUnfilteredPartitionIterator(iter)
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

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
                            if (row.hasLiveData())
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
                        if (tombstones > failureThreshold)
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
                    metric.tombstoneScannedHistogram.update(tombstones);
                    metric.liveScannedHistogram.update(liveRows);

                    boolean warnTombstones = tombstones > warningThreshold;
                    if (warnTombstones)
                    {
                        String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toString());
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

        if (this.kind == Kind.SINGLE_PARTITION)
            return new MessageOut<>(MessagingService.Verb.READ, this, legacyReadCommandSerializer);
        else
            // TODO separate serializer needed for paged commands
            return new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, legacyRangeSliceCommandSerializer);
    }

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip expired tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for expired tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutExpiredTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        final int gcBefore = cfs.gcBefore(nowInSec());
        return new FilteringPartitionIterator(iterator)
        {
            protected FilteringRow makeRowFilter()
            {
                return new FilteringRow()
                {
                    @Override
                    protected boolean include(LivenessInfo info)
                    {
                        return !info.hasLocalDeletionTime() || !info.isPurgeable(Long.MAX_VALUE, gcBefore);
                    }

                    @Override
                    protected boolean include(DeletionTime dt)
                    {
                        return includeDelTime(dt);
                    }

                    @Override
                    protected boolean include(ColumnDefinition c, DeletionTime dt)
                    {
                        return includeDelTime(dt);
                    }
                };
            }

            private boolean includeDelTime(DeletionTime dt)
            {
                return dt.isLive() || !dt.isPurgeable(Long.MAX_VALUE, gcBefore);
            }

            @Override
            protected boolean includePartitionDeletion(DeletionTime dt)
            {
                return includeDelTime(dt);
            }

            @Override
            protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
            {
                return includeDelTime(marker.deletionTime());
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
        sb.append("SELECT ");
        if (queriedColumns().equals(metadata().partitionColumns()))
        {
            sb.append("*");
        }
        else
        {
            sb.append(ColumnDefinition.toCQLString(Iterables.concat(metadata().partitionKeyColumns(), metadata().clusteringColumns())));
            if (!queriedColumns().isEmpty())
                sb.append(", ").append(queriedColumns());
        }

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
            DataLimits.serializer.serialize(command.limits(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
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
            DataLimits limits = DataLimits.serializer.deserialize(in, version);

            return kind.selectionDeserializer.deserialize(in, version, isDigest, isForThrift, metadata, nowInSec, columnFilter, limits);
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

            TypeSizes sizes = TypeSizes.NATIVE;

            return 2 // kind + flags
                 + CFMetaData.serializer.serializedSize(command.metadata(), version, sizes)
                 + sizes.sizeof(command.nowInSec())
                 + ColumnFilter.serializer.serializedSize(command.columnFilter(), version)
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
        public static LegacyType fromPartitionFilterKind(PartitionFilter.Kind kind)
        {
            return kind == PartitionFilter.Kind.SLICE
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
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;

            CFMetaData metadata = rangeCommand.metadata();

            out.writeUTF(metadata.ksName);
            out.writeUTF(metadata.cfName);
            out.writeLong(rangeCommand.nowInSec() * 1000L);  // convert from seconds to millis

            // begin DiskAtomFilterSerializer.serialize()
            if (rangeCommand.isNamesQuery())
            {
                // TODO unify with single-partition names query serialization
                out.writeByte(1);  // 0 for slices, 1 for names
                NamesPartitionFilter filter = (NamesPartitionFilter) rangeCommand.dataRange().partitionFilter;
                PartitionColumns columns = filter.queriedColumns().columns();
                out.writeInt(columns.size());

                for (ColumnDefinition column : columns)
                {
                    Clustering clustering = LegacyLayout.decodeClustering(metadata, column.name.bytes);
                    ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null), out);
                }

                // see serializeNamesCommand() for an explanation of the countCql3Rows  ield
                if (metadata.isCompactTable() && !(command.limits().kind() == DataLimits.Kind.CQL_LIMIT && command.limits().perPartitionCount() == 1))
                    out.writeBoolean(true);  // it's compact and not a DISTINCT query
                else
                    out.writeBoolean(false);
            }
            else
            {
                out.writeByte(0);  // 0 for slices, 1 for names

                // slice filter serialization
                SlicePartitionFilter filter = (SlicePartitionFilter) rangeCommand.dataRange().partitionFilter;
                LegacyReadCommandSerializer.serializeSlices(out, filter.requestedSlices(), metadata);

                out.writeBoolean(filter.isReversed());
                out.writeInt(command.limits().perPartitionCount());  // TODO check that this is the right count for the slice limit
                int compositesToGroup;
                DataLimits.Kind kind = command.limits().kind();
                if (kind == DataLimits.Kind.THRIFT_LIMIT)
                    compositesToGroup = -1;
                else if ((kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && command.limits().perPartitionCount() == 1)
                    compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
                else
                    compositesToGroup = metadata.clusteringColumns().size();
                out.writeInt(compositesToGroup);
            }

            // rowFilter serialization
            if (rangeCommand.columnFilter().equals(ColumnFilter.NONE))
            {
                out.writeInt(0);
            }
            else
            {
                ArrayList<ColumnFilter.Expression> indexExpressions = Lists.newArrayList(rangeCommand.columnFilter().iterator());
                out.writeInt(indexExpressions.size());
                for (ColumnFilter.Expression expression : indexExpressions)
                {
                    ByteBufferUtil.writeWithShortLength(expression.column().name.bytes, out);
                    out.writeInt(expression.operator().ordinal());
                    ByteBufferUtil.writeWithShortLength(expression.getIndexValue(), out);
                }
            }

            // key range serialization
            AbstractBounds.rowPositionSerializer.serialize(rangeCommand.dataRange().keyRange(), out, version);
            out.writeInt(rangeCommand.limits().count());  // maxResults
            out.writeBoolean(!rangeCommand.isForThrift());  // countCQL3Rows TODO probably not correct, need to handle DISTINCT
            out.writeBoolean(rangeCommand.dataRange().isPaging());  // isPaging
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            int nowInSec = (int) (in.readLong() / 1000);  // convert from millis to seconds

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);

            try
            {
                PartitionFilter filter;
                int compositesToGroup = 0;
                int perPartitionLimit = -1;
                boolean isNamesQuery = in.readBoolean();  // 0 for slices, 1 for names
                if (isNamesQuery)
                {
                    // TODO unify with single-partition names query deser
                    int numCellNames = in.readInt();
                    SortedSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
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

                    ColumnsSelection selection = ColumnsSelection.withoutSubselection(new PartitionColumns(Columns.from(staticColumns), Columns.from(columns)));
                    filter = new NamesPartitionFilter(selection, clusterings, false);
                }
                else
                {
                    filter = LegacyReadCommandSerializer.deserializeSlicePartitionFilter(in, metadata);
                    perPartitionLimit = in.readInt();
                    compositesToGroup = in.readInt();
                }

                int numColumnFilters = in.readInt();
                ColumnFilter columnFilter;
                if (numColumnFilters == 0)
                {
                    columnFilter = ColumnFilter.NONE;
                }
                else
                {
                    int numFilters = in.readInt();
                    columnFilter = ColumnFilter.create(numFilters);
                    for (int i = 0; i < numFilters; i++)
                    {
                        ByteBuffer columnName = ByteBufferUtil.readWithShortLength(in);
                        ColumnDefinition column = metadata.getColumnDefinition(columnName);
                        Operator op = Operator.readFrom(in);
                        ByteBuffer indexValue = ByteBufferUtil.readWithShortLength(in);
                        columnFilter.add(column, op, indexValue);
                    }
                }

                AbstractBounds<PartitionPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, StorageService.getPartitioner(), version);
                int maxResults = in.readInt();

                // TODO what needs to be done for these?
                boolean countCQL3Rows = in.readBoolean();
                boolean isPaging = in.readBoolean();

                boolean isDistinct = compositesToGroup == -2;
                DataLimits limits;
                if (isDistinct)
                    limits = DataLimits.distinctLimits(maxResults);
                else if (compositesToGroup == -1)
                    limits = DataLimits.thriftLimits(maxResults, perPartitionLimit);
                else
                    limits = DataLimits.cqlLimits(maxResults);

                PartitionRangeReadCommand command = new PartitionRangeReadCommand(false, true, metadata, nowInSec, columnFilter, limits, new DataRange(keyRange, filter));
                return command;
            }
            catch (UnknownColumnException exc)
            {
                // TODO what to do?
                throw new RuntimeException(exc);
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;

            assert command.kind == Kind.PARTITION_RANGE;
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            CFMetaData metadata = rangeCommand.metadata();

            long size = sizes.sizeof(metadata.ksName);
            size += sizes.sizeof(metadata.cfName);
            size += sizes.sizeof((long) rangeCommand.nowInSec());

            size += 1;  // 0 for slices, 1 for names
            if (rangeCommand.isNamesQuery())
            {
                PartitionColumns columns = command.queriedColumns().columns();
                size = sizes.sizeof(columns.size());
                for (ColumnDefinition column : columns)
                {
                    Clustering clustering = LegacyLayout.decodeClustering(metadata, column.name.bytes);
                    ByteBuffer columnName = LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null);
                    size += ByteBufferUtil.serializedSizeWithShortLength(columnName, sizes);
                }

                size += sizes.sizeof(true);  // countCql3Rows
            }
            else
            {
                SlicePartitionFilter filter = (SlicePartitionFilter) rangeCommand.dataRange().partitionFilter;
                size += LegacyReadCommandSerializer.serializedSlicesSize(filter.requestedSlices(), metadata);
                size += sizes.sizeof(filter.isReversed());
                size += sizes.sizeof(command.limits().perPartitionCount());
                size += sizes.sizeof(0); // compositesToGroup
            }

            if (rangeCommand.columnFilter().equals(ColumnFilter.NONE))
            {
                size += sizes.sizeof(0);
            }
            else
            {
                ArrayList<ColumnFilter.Expression> indexExpressions = Lists.newArrayList(rangeCommand.columnFilter().iterator());
                size += sizes.sizeof(indexExpressions.size());
                for (ColumnFilter.Expression expression : indexExpressions)
                {
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes, sizes);
                    size += sizes.sizeof(expression.operator().ordinal());
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.getIndexValue(), sizes);
                }
            }

            size += AbstractBounds.rowPositionSerializer.serializedSize(rangeCommand.dataRange().keyRange(), version);
            size += sizes.sizeof(rangeCommand.limits().count());
            size += sizes.sizeof(!rangeCommand.isForThrift());
            return size + sizes.sizeof(rangeCommand.dataRange().isPaging());
        }
    }

    /*
     * Deserialize pre-3.0 PagedRangeCommand for backward compatibility sake
     */
    private static class LegacyPagedRangeCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {

            // TODO
            throw new UnsupportedOperationException();
            //        out.writeUTF(cmd.keyspace);
            //        out.writeUTF(cmd.columnFamily);
            //        out.writeLong(cmd.timestamp);

            //        AbstractBounds.serializer.serialize(cmd.keyRange, out, version);

            //        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            //        // SliceQueryFilter (the count is not used)
            //        SliceQueryFilter filter = (SliceQueryFilter)cmd.predicate;
            //        metadata.comparator.sliceQueryFilterSerializer().serialize(filter, out, version);

            //        // The start and stop of the page
            //        metadata.comparator.serializer().serialize(cmd.start, out);
            //        metadata.comparator.serializer().serialize(cmd.stop, out);

            //        out.writeInt(cmd.rowFilter.size());
            //        for (IndexExpression expr : cmd.rowFilter)
            //        {
            //            ByteBufferUtil.writeWithShortLength(expr.column, out);
            //            out.writeInt(expr.operator.ordinal());
            //            ByteBufferUtil.writeWithShortLength(expr.value, out);
            //        }

            //        out.writeInt(cmd.limit);
            //        if (version >= MessagingService.VERSION_21)
            //            out.writeBoolean(cmd.countCQL3Rows);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        String keyspace = in.readUTF();
            //        String columnFamily = in.readUTF();
            //        long timestamp = in.readLong();

            //        AbstractBounds<PartitionPosition> keyRange = AbstractBounds.serializer.deserialize(in, version).toRowBounds();

            //        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);

            //        SliceQueryFilter predicate = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);

            //        Composite start = metadata.comparator.serializer().deserialize(in);
            //        Composite stop =  metadata.comparator.serializer().deserialize(in);

            //        int filterCount = in.readInt();
            //        List<IndexExpression> rowFilter = new ArrayList<IndexExpression>(filterCount);
            //        for (int i = 0; i < filterCount; i++)
            //        {
            //            IndexExpression expr = new IndexExpression(ByteBufferUtil.readWithShortLength(in),
            //                                                       IndexExpression.Operator.findByOrdinal(in.readInt()),
            //                                                       ByteBufferUtil.readWithShortLength(in));
            //            rowFilter.add(expr);
            //        }

            //        int limit = in.readInt();
            //        boolean countCQL3Rows = version >= MessagingService.VERSION_21
            //                              ? in.readBoolean()
            //                              : predicate.compositesToGroup >= 0 || predicate.count != 1; // See #6857
            //        return new PagedRangeCommand(keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit, countCQL3Rows);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            throw new UnsupportedOperationException();
            //        long size = 0;

            //        size += TypeSizes.NATIVE.sizeof(cmd.keyspace);
            //        size += TypeSizes.NATIVE.sizeof(cmd.columnFamily);
            //        size += TypeSizes.NATIVE.sizeof(cmd.timestamp);

            //        size += AbstractBounds.serializer.serializedSize(cmd.keyRange, version);

            //        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            //        size += metadata.comparator.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)cmd.predicate, version);

            //        size += metadata.comparator.serializer().serializedSize(cmd.start, TypeSizes.NATIVE);
            //        size += metadata.comparator.serializer().serializedSize(cmd.stop, TypeSizes.NATIVE);

            //        size += TypeSizes.NATIVE.sizeof(cmd.rowFilter.size());
            //        for (IndexExpression expr : cmd.rowFilter)
            //        {
            //            size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
            //            size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
            //            size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            //        }

            //        size += TypeSizes.NATIVE.sizeof(cmd.limit);
            //        if (version >= MessagingService.VERSION_21)
            //            size += TypeSizes.NATIVE.sizeof(cmd.countCQL3Rows);
            //        return size;
        }
    }

    // From old ReadCommand
    static class LegacyReadCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert command.kind == Kind.SINGLE_PARTITION;

            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;

            CFMetaData metadata = singleReadCommand.metadata();

            out.writeByte(LegacyType.fromPartitionFilterKind(singleReadCommand.partitionFilter().getKind()).ordinal());

            out.writeBoolean(singleReadCommand.isDigestQuery());
            out.writeUTF(metadata.ksName);
            ByteBufferUtil.writeWithShortLength(singleReadCommand.partitionKey().getKey(), out);
            out.writeUTF(metadata.cfName);
            out.writeLong(singleReadCommand.nowInSec() * 1000L);  // convert from seconds to millis

            if (singleReadCommand.partitionFilter().getKind() == PartitionFilter.Kind.SLICE)
                serializeSliceCommand((SinglePartitionSliceCommand) singleReadCommand, out, version);
            else
                serializeNamesCommand((SinglePartitionNamesCommand) singleReadCommand, out, version);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
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
                        return deserializeNamesCommand(in, version, isDigest, metadata, key, nowInSeconds);
                    case GET_SLICES:
                        return deserializeSliceCommand(in, version, isDigest, metadata, key, nowInSeconds);
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
            TypeSizes sizes = TypeSizes.NATIVE;

            assert command.kind == Kind.SINGLE_PARTITION;
            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;

            int keySize = singleReadCommand.partitionKey().getKey().remaining();

            CFMetaData metadata = singleReadCommand.metadata();

            long size = 1;  // message type (single byte)
            size += sizes.sizeof(command.isDigestQuery());
            size += sizes.sizeof(metadata.ksName);
            size += sizes.sizeof((short) keySize) + keySize;
            size += sizes.sizeof((long) command.nowInSec());

            if (singleReadCommand.partitionFilter().getKind() == PartitionFilter.Kind.SLICE)
                return size + serializedSliceCommandSize((SinglePartitionSliceCommand) singleReadCommand, version);
            else
                return size + serializedNamesCommandSize((SinglePartitionNamesCommand) singleReadCommand, version);
        }

        private void serializeNamesCommand(SinglePartitionNamesCommand command, DataOutputPlus out, int version) throws IOException
        {
            CFMetaData metadata = command.metadata();

            PartitionColumns columns = command.queriedColumns().columns();
            out.writeInt(columns.size());
            for (ColumnDefinition column : columns)
            {
                // TODO this is not correct for the clustering value when the clustering columns exist and are fully specified
                Clustering clustering = LegacyLayout.decodeClustering(metadata, column.name.bytes);
                ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null), out);
            }

            // countCql3Rows should be true if it's not a DISTINCT query and it's fetching a range of cells, meaning
            // one of the following is true:
            //  * it's a sparse, simple table
            //  * it's a dense table, but the clustering columns have been fully specified
            // We only use NamesPartitionFilters when the clustering columns have been fully specified, so we can
            // combine the last two cases into a check for compact storage.
            if (metadata.isCompactTable() && !(command.limits().kind() == DataLimits.Kind.CQL_LIMIT && command.limits().perPartitionCount() == 1))
                out.writeBoolean(true);  // it's compact and not a DISTINCT query
            else
                out.writeBoolean(false);
        }

        public long serializedNamesCommandSize(SinglePartitionNamesCommand command, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            CFMetaData metadata = command.metadata();

            PartitionColumns columns = command.queriedColumns().columns();
            long size = sizes.sizeof(columns.size());
            for (ColumnDefinition column : columns)
            {
                Clustering clustering = LegacyLayout.decodeClustering(metadata, column.name.bytes);
                ByteBuffer columnName = LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null);
                size += sizes.sizeof((short) columnName.remaining()) + columnName.remaining();
            }

            return size + sizes.sizeof(true);  // countCql3Rows
        }

        private SinglePartitionNamesCommand deserializeNamesCommand(DataInput in, int version, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds) throws IOException, UnknownColumnException
        {
            int numCellNames = in.readInt();
            SortedSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
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

            ColumnsSelection selection = ColumnsSelection.withoutSubselection(new PartitionColumns(Columns.from(staticColumns), Columns.from(columns)));
            NamesPartitionFilter filter = new NamesPartitionFilter(selection, clusterings, false);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionNamesCommand(isDigest, true, metadata, nowInSeconds, ColumnFilter.NONE, DataLimits.NONE, key, filter);
        }

        private void serializeSliceCommand(SinglePartitionSliceCommand command, DataOutputPlus out, int version) throws IOException
        {
            CFMetaData metadata = command.metadata();

            // slice filter serialization
            serializeSlices(out, command.partitionFilter().requestedSlices(), metadata);

            out.writeBoolean(command.partitionFilter().isReversed());
            out.writeInt(command.limits().count());
            int compositesToGroup;
            DataLimits.Kind kind = command.limits().kind();
            if (kind == DataLimits.Kind.THRIFT_LIMIT)
                compositesToGroup = -1;
            else if ((kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && command.limits().perPartitionCount() == 1)
                compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
            else
                compositesToGroup = metadata.clusteringColumns().size();
            out.writeInt(compositesToGroup);
        }

        static void serializeSlices(DataOutputPlus out, Slices slices, CFMetaData metadata) throws IOException
        {
            out.writeInt(slices.size());
            for (Slice slice : slices)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeCellName(metadata, slice.start().clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                ByteBufferUtil.writeWithShortLength(sliceStart, out);

                ByteBuffer sliceEnd = LegacyLayout.encodeCellName(metadata, slice.end().clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                ByteBufferUtil.writeWithShortLength(sliceEnd, out);
            }
        }

        public long serializedSliceCommandSize(SinglePartitionSliceCommand command, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            long size = serializedSlicesSize(command.partitionFilter().requestedSlices(), command.metadata());
            size += sizes.sizeof(command.partitionFilter().isReversed());
            size += sizes.sizeof(command.limits().count());
            return size + sizes.sizeof(0);  // compositesToGroup
        }

        static long serializedSlicesSize(Slices slices, CFMetaData metadata)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            long size = sizes.sizeof(slices.size());

            for (Slice slice : slices)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeCellName(metadata, slice.start().clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                ByteBuffer sliceEnd = LegacyLayout.encodeCellName(metadata, slice.end().clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                size += sizes.sizeof((short) sliceStart.remaining()) + sliceStart.remaining();
                size += sizes.sizeof((short) sliceEnd.remaining()) + sliceEnd.remaining();
            }
            return size;
        }

        private SinglePartitionSliceCommand deserializeSliceCommand(DataInput in, int version, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds) throws IOException, UnknownColumnException
        {
            SlicePartitionFilter filter = deserializeSlicePartitionFilter(in, metadata);
            int count = in.readInt();
            int compositesToGroup = in.readInt();

            DataLimits limits;
            if (compositesToGroup == -2)
                limits = DataLimits.distinctLimits(count);  // See CASSANDRA-8490 for the explanation of this value
            else if (compositesToGroup == -1)
                limits = DataLimits.thriftLimits(1, count);
            else
                limits = DataLimits.cqlLimits(count);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionSliceCommand(isDigest, true, metadata, nowInSeconds, ColumnFilter.NONE, limits, key, filter);
        }

        static SlicePartitionFilter deserializeSlicePartitionFilter(DataInput in, CFMetaData metadata) throws IOException
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
                    finish = LegacyLayout.decodeBound(metadata, finishBuffers[i], false).bound;
                    start = LegacyLayout.decodeBound(metadata, startBuffers[i], true).bound;
                }
                slicesBuilder.add(Slice.make(start, finish));
            }

            return new SlicePartitionFilter(metadata.partitionColumns(), slicesBuilder.build(), reversed);
        }
    }
}
