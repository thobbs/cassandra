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
package org.apache.cassandra.db.filter;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Object in charge of tracking if we have fetch enough data for a given query.
 *
 * The reason this is not just a simple integer is that Thrift and CQL3 count
 * stuffs in different ways. This is what abstract those differences.
 */
public abstract class DataLimits
{
    public static final Serializer serializer = new Serializer();

    // Please note that this shouldn't be use for thrift
    public static final DataLimits NONE = new CQLLimits(Integer.MAX_VALUE)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            return false;
        }

        @Override
        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter)
        {
            return iter;
        }

        @Override
        public UnfilteredRowIterator filter(UnfilteredRowIterator iter)
        {
            return iter;
        }
    };
    public static final DataLimits DISTINCT_NONE = new CQLLimits(Integer.MAX_VALUE, 1);

    public enum Kind { CQL_LIMIT, CQL_PAGING_LIMIT, THRIFT_LIMIT, SUPER_COLUMN_COUNTING_LIMIT }

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return new CQLLimits(cqlRowLimit);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return new CQLLimits(cqlRowLimit, perPartitionLimit);
    }

    public static DataLimits distinctLimits(int cqlRowLimit)
    {
        return CQLLimits.distinct(cqlRowLimit);
    }

    public static DataLimits thriftLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        return new ThriftLimits(partitionLimit, cellPerPartitionLimit);
    }

    public static DataLimits superColumnCountingLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        return new SuperColumnCountingLimits(partitionLimit, cellPerPartitionLimit);
    }

    public abstract Kind kind();

    public abstract boolean isUnlimited();

    public abstract DataLimits forPaging(int pageSize);
    public abstract DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

    public abstract DataLimits forShortReadRetry(int toFetch);

    public abstract boolean hasEnoughLiveData(CachedPartition cached, int nowInSec);

    /**
     * Returns a new {@code Counter} for this limits.
     *
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @return a new {@code Counter} for this limits.
     */
    public abstract Counter newCounter(boolean assumeLiveData);

    /**
     * The max number of results this limits enforces.
     * <p>
     * Note that the actual definition of "results" depends a bit: for CQL, it's always rows, but for
     * thrift, it means cells. The {@link #countCells} allows to distinguish between the two cases if
     * needed.
     *
     * @return the maximum number of results this limits enforces.
     */
    public abstract int count();

    public abstract int perPartitionCount();

    public abstract boolean countCells();

    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter)
    {
        return new CountingUnfilteredPartitionIterator(iter, newCounter(false));
    }

    public UnfilteredRowIterator filter(UnfilteredRowIterator iter)
    {
        return new CountingUnfilteredRowIterator(iter, newCounter(false));
    }

    public PartitionIterator filter(PartitionIterator iter)
    {
        return new CountingPartitionIterator(iter, this);
    }

    /**
     * Estimate the number of results (the definition of "results" will be rows for CQL queries
     * and partitions for thrift ones) that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public interface Counter
    {
        public void newPartition(DecoratedKey partitionKey);
        public void newRow(Row row);

        /**
         * The number of results counted.
         * <p>
         * Note that the definition of "results" should be the same that for {@link #count}.
         *
         * @return the number of results counted.
         */
        public int counted();

        public int countedInCurrentPartition();

        public boolean isDone();
        public boolean isDoneForPartition();
    }

    /**
     * Limits used by CQL; this count rows.
     */
    private static class CQLLimits extends DataLimits
    {
        protected final int rowLimit;
        protected final int perPartitionLimit;
        protected final boolean isDistinct;

        private CQLLimits(int rowLimit)
        {
            this(rowLimit, Integer.MAX_VALUE);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit)
        {
            this(rowLimit, perPartitionLimit, false);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit, boolean isDistinct)
        {
            this.rowLimit = rowLimit;
            this.perPartitionLimit = perPartitionLimit;
            this.isDistinct = isDistinct;
        }

        private static CQLLimits distinct(int rowLimit)
        {
            return new CQLLimits(rowLimit, 1, true);
        }

        public Kind kind()
        {
            return Kind.CQL_LIMIT;
        }

        public boolean isUnlimited()
        {
            return rowLimit == Integer.MAX_VALUE && perPartitionLimit == Integer.MAX_VALUE;
        }

        public DataLimits forPaging(int pageSize)
        {
            return new CQLLimits(pageSize, perPartitionLimit);
        }

        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLPagingLimits(pageSize, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // When we do a short read retry, we're only ever querying the single partition on which we have a short read. So
            // we use toFetch as the row limit and use no perPartitionLimit (it would be equivalent in practice to use toFetch
            // for both argument or just for perPartitionLimit with no limit on rowLimit).
            return new CQLLimits(toFetch, Integer.MAX_VALUE, isDistinct);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            // We want the number of row that are currently live. Getting that precise number forces
            // us to iterate the cached partition in general, but we can avoid that if:
            //   - The number of rows with at least one non-expiring cell is greater than what we ask,
            //     in which case we know we have enough live.
            //   - The number of rows is less than requested, in which case we  know we won't have enough.
            if (cached.rowsWithNonExpiringCells() >= rowLimit)
                return true;

            if (cached.rowCount() < rowLimit)
                return false;

            // Otherwise, we need to re-count
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnsSelection.withoutSubselection(cached.columns()), Slices.ALL, false, nowInSec);
                 CountingUnfilteredRowIterator iter = new CountingUnfilteredRowIterator(cacheIter, newCounter(false)))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext() && !iter.counter().isDone())
                    iter.next();
                return iter.counter().isDone();
            }
        }

        public Counter newCounter(boolean assumeLiveData)
        {
            return new CQLCounter(assumeLiveData);
        }

        public int count()
        {
            return rowLimit;
        }

        public int perPartitionCount()
        {
            return perPartitionLimit;
        }

        public boolean countCells()
        {
            return false;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells, which
            // is what getMeanColumns returns)
            float rowsPerPartition = ((float) cfs.getMeanColumns()) / cfs.metadata.partitionColumns().regulars.columnCount();
            return rowsPerPartition * (cfs.estimateKeys());
        }

        protected class CQLCounter implements Counter
        {
            protected final boolean assumeLiveData;

            protected int rowCounted;
            protected int rowInCurrentPartition;

            public CQLCounter(boolean assumeLiveData)
            {
                this.assumeLiveData = assumeLiveData;
            }

            public void newPartition(DecoratedKey partitionKey)
            {
                rowInCurrentPartition = 0;
            }

            public int counted()
            {
                return rowCounted;
            }

            public int countedInCurrentPartition()
            {
                return rowInCurrentPartition;
            }

            public boolean isDone()
            {
                return rowCounted >= rowLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || rowInCurrentPartition >= perPartitionLimit;
            }

            public void newRow(Row row)
            {
                if (assumeLiveData || row.hasLiveData())
                {
                    ++rowCounted;
                    ++rowInCurrentPartition;
                }
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (rowLimit != Integer.MAX_VALUE)
            {
                sb.append("LIMIT ").append(rowLimit);
                if (perPartitionLimit != Integer.MAX_VALUE)
                    sb.append(" ");
            }

            if (perPartitionLimit != Integer.MAX_VALUE)
                sb.append("PER PARTITION LIMIT ").append(perPartitionLimit);

            return sb.toString();
        }
    }

    private static class CQLPagingLimits extends CQLLimits
    {
        private final ByteBuffer lastReturnedKey;
        private final int lastReturnedKeyRemaining;

        public CQLPagingLimits(int rowLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            super(rowLimit, perPartitionLimit, isDistinct);
            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter newCounter(boolean assumeLiveData)
        {
            return new PagingAwareCounter(assumeLiveData);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(boolean assumeLiveData)
            {
                super(assumeLiveData);
            }

            @Override
            public void newPartition(DecoratedKey partitionKey)
            {
                rowInCurrentPartition = partitionKey.getKey().equals(lastReturnedKey)
                                      ? perPartitionLimit - lastReturnedKeyRemaining
                                      : 0;
            }
        }
    }

    /**
     * Limits used by thrift; this count partition and cells.
     */
    private static class ThriftLimits extends DataLimits
    {
        protected final int partitionLimit;
        protected final int cellPerPartitionLimit;

        private ThriftLimits(int partitionLimit, int cellPerPartitionLimit)
        {
            this.partitionLimit = partitionLimit;
            this.cellPerPartitionLimit = cellPerPartitionLimit;
        }

        public Kind kind()
        {
            return Kind.THRIFT_LIMIT;
        }

        public boolean isUnlimited()
        {
            return partitionLimit == Integer.MAX_VALUE && cellPerPartitionLimit == Integer.MAX_VALUE;
        }

        public DataLimits forPaging(int pageSize)
        {
            // We don't support paging on thrift in general but do use paging under the hood for get_count. For
            // that case, we only care about limiting cellPerPartitionLimit (since it's paging over a single
            // partition). We do check that the partition limit is 1 however to make sure this is not misused
            // (as this wouldn't work properly for range queries).
            assert partitionLimit == 1;
            return new ThriftLimits(partitionLimit, pageSize);
        }

        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // Short read retries are always done for a single partition at a time, so it's ok to ignore the
            // partition limit for those
            return new ThriftLimits(1, toFetch);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            // We want the number of cells that are currently live. Getting that precise number forces
            // us to iterate the cached partition in general, but we can avoid that if:
            //   - The number of non-expiring live cells is greater than the number of cells asked (we then
            //     know we have enough live cells).
            //   - The number of cells cached is less than requested, in which case we know we won't have enough.
            if (cached.nonExpiringLiveCells() >= cellPerPartitionLimit)
                return true;

            if (cached.nonTombstoneCellCount() < cellPerPartitionLimit)
                return false;

            // Otherwise, we need to re-count
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnsSelection.withoutSubselection(cached.columns()), Slices.ALL, false, nowInSec);
                 CountingUnfilteredRowIterator iter = new CountingUnfilteredRowIterator(cacheIter, newCounter(false)))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext() && !iter.counter().isDone())
                    iter.next();
                return iter.counter().isDone();
            }
        }

        public Counter newCounter(boolean assumeLiveData)
        {
            return new ThriftCounter(assumeLiveData);
        }

        public int count()
        {
            return partitionLimit * cellPerPartitionLimit;
        }

        public int perPartitionCount()
        {
            return cellPerPartitionLimit;
        }

        public boolean countCells()
        {
            return true;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // remember that getMeansColumns returns a number of cells: we should clean nomenclature
            float cellsPerPartition = ((float) cfs.getMeanColumns()) / cfs.metadata.partitionColumns().regulars.columnCount();
            return cellsPerPartition * cfs.estimateKeys();
        }

        protected class ThriftCounter implements Counter
        {
            protected final boolean assumeLiveData;

            protected int partitionsCounted;
            protected int cellsCounted;
            protected int cellsInCurrentPartition;

            public ThriftCounter(boolean assumeLiveData)
            {
                this.assumeLiveData = assumeLiveData;
            }

            public void newPartition(DecoratedKey partitionKey)
            {
                cellsInCurrentPartition = 0;
                ++partitionsCounted;
            }

            public int counted()
            {
                return cellsCounted;
            }

            public int countedInCurrentPartition()
            {
                return cellsInCurrentPartition;
            }

            public boolean isDone()
            {
                return partitionsCounted >= partitionLimit;
            }

            public boolean isDoneForPartition()
            {
                // Note that as we count partition at the beginning while this is called withing a partition, we don't want to
                // check isDone() as this would make use stop too early.
                return cellsInCurrentPartition >= cellPerPartitionLimit;
            }

            public void newRow(Row row)
            {
                for (Cell cell : row)
                {
                    if (assumeLiveData || cell.isLive(row.nowInSec()))
                    {
                        ++cellsCounted;
                        ++cellsInCurrentPartition;
                    }
                }
            }
        }

        @Override
        public String toString()
        {
            // This is not valid CQL, but that's ok since it's not used for CQL queries.
            return String.format("THRIFT LIMIT (partitions=%d, cells_per_partition=%d)", partitionLimit, cellPerPartitionLimit);
        }
    }

    /**
     * Limits used for thrift get_count when we only want to count super columns.
     */
    private static class SuperColumnCountingLimits extends ThriftLimits
    {
        private SuperColumnCountingLimits(int partitionLimit, int cellPerPartitionLimit)
        {
            super(partitionLimit, cellPerPartitionLimit);
        }

        public Kind kind()
        {
            return Kind.SUPER_COLUMN_COUNTING_LIMIT;
        }

        public DataLimits forPaging(int pageSize)
        {
            // We don't support paging on thrift in general but do use paging under the hood for get_count. For
            // that case, we only care about limiting cellPerPartitionLimit (since it's paging over a single
            // partition). We do check that the partition limit is 1 however to make sure this is not misused
            // (as this wouldn't work properly for range queries).
            assert partitionLimit == 1;
            return new SuperColumnCountingLimits(partitionLimit, pageSize);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // Short read retries are always done for a single partition at a time, so it's ok to ignore the
            // partition limit for those
            return new SuperColumnCountingLimits(1, toFetch);
        }

        public Counter newCounter(boolean assumeLiveData)
        {
            return new SuperColumnCountingCounter(assumeLiveData);
        }

        protected class SuperColumnCountingCounter extends ThriftCounter
        {
            public SuperColumnCountingCounter(boolean assumeLiveData)
            {
                super(assumeLiveData);
            }

            public void newRow(Row row)
            {
                // In the internal format, a row == a super column, so that's what we want to count.
                if (assumeLiveData || row.hasLiveData())
                {
                    ++cellsCounted;
                    ++cellsInCurrentPartition;
                }
            }
        }
    }

    public static class Serializer
    {
        public void serialize(DataLimits limits, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits)limits;
                    out.writeInt(cqlLimits.rowLimit);
                    out.writeInt(cqlLimits.perPartitionLimit);
                    out.writeBoolean(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        ByteBufferUtil.writeWithShortLength(pagingLimits.lastReturnedKey, out);
                        out.writeInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits)limits;
                    out.writeInt(thriftLimits.partitionLimit);
                    out.writeInt(thriftLimits.cellPerPartitionLimit);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        public DataLimits deserialize(DataInput in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    int rowLimit = in.readInt();
                    int perPartitionLimit = in.readInt();
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return new CQLLimits(rowLimit, perPartitionLimit, isDistinct);

                    ByteBuffer lastKey = ByteBufferUtil.readWithShortLength(in);
                    int lastRemaining = in.readInt();
                    return new CQLPagingLimits(rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    int partitionLimit = in.readInt();
                    int cellPerPartitionLimit = in.readInt();
                    return kind == Kind.THRIFT_LIMIT
                         ? new ThriftLimits(partitionLimit, cellPerPartitionLimit)
                         : new SuperColumnCountingLimits(partitionLimit, cellPerPartitionLimit);
            }
            throw new AssertionError();
        }

        public long serializedSize(DataLimits limits, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            long size = sizes.sizeof((byte)limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits)limits;
                    size += sizes.sizeof(cqlLimits.rowLimit);
                    size += sizes.sizeof(cqlLimits.perPartitionLimit);
                    size += sizes.sizeof(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        size += ByteBufferUtil.serializedSizeWithShortLength(pagingLimits.lastReturnedKey, sizes);
                        size += sizes.sizeof(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits)limits;
                    size += sizes.sizeof(thriftLimits.partitionLimit);
                    size += sizes.sizeof(thriftLimits.cellPerPartitionLimit);
                    break;
                default:
                    throw new AssertionError();
            }
            return size;
        }
    }
}
