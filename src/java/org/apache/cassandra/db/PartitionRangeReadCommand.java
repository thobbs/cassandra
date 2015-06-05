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
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * A read command that selects a (part of a) range of partitions.
 */
public class PartitionRangeReadCommand extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DataRange dataRange;

    public PartitionRangeReadCommand(boolean isDigest,
                                     boolean isForThrift,
                                     CFMetaData metadata,
                                     int nowInSec,
                                     ColumnFilter columnFilter,
                                     DataLimits limits,
                                     DataRange dataRange)
    {
        super(Kind.PARTITION_RANGE, isDigest, isForThrift, metadata, nowInSec, columnFilter, limits);
        this.dataRange = dataRange;
    }

    public PartitionRangeReadCommand(CFMetaData metadata,
                                     int nowInSec,
                                     ColumnFilter columnFilter,
                                     DataLimits limits,
                                     DataRange dataRange)
    {
        this(false, false, metadata, nowInSec, columnFilter, limits, dataRange);
    }

    /**
     * Creates a new read command that query all the data in the table.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     *
     * @return a newly created read command that queries everything in the table.
     */
    public static PartitionRangeReadCommand allDataRead(CFMetaData metadata, int nowInSec)
    {
        return new PartitionRangeReadCommand(metadata,
                                             nowInSec,
                                             ColumnFilter.NONE,
                                             DataLimits.NONE,
                                             DataRange.allData(metadata, StorageService.getPartitioner()));
    }

    public DataRange dataRange()
    {
        return dataRange;
    }

    public PartitionFilter partitionFilter(DecoratedKey key)
    {
        return dataRange.partitionFilter(key);
    }

    public boolean isNamesQuery()
    {
        return dataRange.isNamesQuery();
    }

    public PartitionRangeReadCommand forSubRange(AbstractBounds<PartitionPosition> range)
    {
        return new PartitionRangeReadCommand(isDigestQuery(), isForThrift(), metadata(), nowInSec(), columnFilter(), limits(), dataRange().forSubRange(range));
    }

    public PartitionRangeReadCommand copy()
    {
        return new PartitionRangeReadCommand(isDigestQuery(), isForThrift(), metadata(), nowInSec(), columnFilter(), limits(), dataRange());
    }

    public PartitionRangeReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeReadCommand(metadata(), nowInSec(), columnFilter(), newLimits, dataRange());
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public ColumnsSelection queriedColumns()
    {
        return dataRange().queriedColumns();
    }

    public boolean selects(DecoratedKey partitionKey, Clustering clustering)
    {
        return dataRange().contains(partitionKey) && dataRange().partitionFilter(partitionKey).selects(clustering);
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(this, consistency);
    }

    public QueryPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState pagingState)
    {
        return getPager(consistency, pagingState, false);
    }

    public QueryPager getLocalPager()
    {
        return getPager(null, null, true);
    }

    private QueryPager getPager(ConsistencyLevel consistency, PagingState pagingState, boolean local)
    {
        if (isNamesQuery())
            return new RangeNamesQueryPager(this, consistency, local, pagingState);
        else
            return new RangeSliceQueryPager(this, consistency, local, pagingState);
    }

    protected UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs)
    {
        final long start = System.nanoTime();
        final OpOrder.Group op = cfs.readOrdering.start();
        try
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(dataRange().keyRange()));
            Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), dataRange().keyRange().getString(metadata().getKeyValidator()));
            return new WrappingUnfilteredPartitionIterator(getSequentialIterator(view, cfs))
            {
                private boolean closed;

                @Override
                public void close()
                {
                    if (closed)
                        return;

                    try
                    {
                        super.close();
                    }
                    finally
                    {
                        op.close();
                        closed = true;
                        cfs.metric.rangeLatency.addNano(System.nanoTime() - start);
                    }
                }
            };
        }
        catch (RuntimeException e)
        {
            op.close();
            cfs.metric.rangeLatency.addNano(System.nanoTime() - start);
            throw e;
        }
    }

    private UnfilteredPartitionIterator getSequentialIterator(ColumnFamilyStore.ViewFragment view, ColumnFamilyStore cfs)
    {
        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<UnfilteredPartitionIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());

        for (Memtable memtable : view.memtables)
        {
            UnfilteredPartitionIterator iter = memtable.makePartitionIterator(dataRange(), nowInSec(), isForThrift());
            iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter, metadata()) : iter);
        }

        for (SSTableReader sstable : view.sstables)
        {
            UnfilteredPartitionIterator iter = sstable.getScanner(dataRange(), nowInSec(), isForThrift());
            iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter, metadata()) : iter);
        }

        return checkCacheFilter(UnfilteredPartitionIterators.mergeLazily(iterators), cfs);
    }

    private UnfilteredPartitionIterator checkCacheFilter(UnfilteredPartitionIterator iter, final ColumnFamilyStore cfs)
    {
        return new WrappingUnfilteredPartitionIterator(iter)
        {
            @Override
            public UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
            {
                // Note that we rely on the fact that until we actually advance 'iter', no really costly operation is actually done
                // (except for reading the partition key from the index file) due to the call to mergeLazily in getSequentialIterator.
                DecoratedKey dk = iter.partitionKey();

                // Check if this partition is in the rowCache and if it is, if  it covers our filter
                CachedPartition cached = cfs.getRawCachedPartition(dk);
                PartitionFilter filter = dataRange().partitionFilter(dk);

                if (cached != null && cfs.isFilterFullyCoveredBy(filter, limits(), cached, nowInSec()))
                {
                    // We won't use 'iter' so close it now.
                    iter.close();

                    return filter.getUnfilteredRowIterator(cached, nowInSec());
                }

                return iter;
            }
        };
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (dataRange.isUnrestricted() && columnFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the column filter first because the data range can end by "ORDER BY"
        if (!columnFilter().isEmpty())
        {
            sb.append(columnFilter());
            if (!dataRange.isUnrestricted())
                sb.append(" AND ");
        }
        if (!dataRange.isUnrestricted())
            sb.append(dataRange.toCQLString(metadata()));
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public PartitionIterator postReconciliationProcessing(PartitionIterator result)
    {
        ColumnFamilyStore cfs = Keyspace.open(metadata().ksName).getColumnFamilyStore(metadata().cfName);
        SecondaryIndexSearcher searcher = getIndexSearcher(cfs);
        return searcher == null ? result : searcher.postReconciliationProcessing(columnFilter(), result);
    }

    public boolean rowsInPartitionAreReversed()
    {
        return dataRange.partitionFilter.isReversed();
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s.%s cfilter=%s limits=%s %s)",
                             metadata().ksName,
                             metadata().cfName,
                             columnFilter(),
                             limits(),
                             dataRange().toString(metadata()));
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        DataRange.serializer.serialize(dataRange(), out, version, metadata());
    }

    protected long selectionSerializedSize(int version)
    {
        return DataRange.serializer.serializedSize(dataRange(), version, metadata());
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInput in, int version, boolean isDigest, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, DataLimits limits)
        throws IOException
        {
            DataRange range = DataRange.serializer.deserialize(in, version, metadata);
            return new PartitionRangeReadCommand(isDigest, isForThrift, metadata, nowInSec, columnFilter, limits, range);
        }
    };
}
