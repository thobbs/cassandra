/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

/**
 * Groups both the range of partitions to query, and the partition filter to
 * apply for each partition (for a (partition) range query).
 * <p>
 * The main "trick" is that the partition filter can only be obtained by
 * providing the partition key on which the filter will be applied. This is
 * necessary when paging range queries, as we might need a different filter
 * for the starting key than for other keys (because the previous page we had
 * queried may have ended in the middle of a partition).
 */
public class DataRange
{
    public static final Serializer serializer = new Serializer();

    private final AbstractBounds<PartitionPosition> keyRange;
    protected final PartitionFilter partitionFilter;

    /**
     * Creates a {@code DataRange} given a range of partition keys and a partition filter. The
     * return {@code DataRange} will return the same filter for all keys.
     *
     * @param range the range over partition keys to use.
     * @param partitionFilter the partition filter to use.
     */
    public DataRange(AbstractBounds<PartitionPosition> range, PartitionFilter partitionFilter)
    {
        this.keyRange = range;
        this.partitionFilter = partitionFilter;
    }

    /**
     * Creates a {@code DataRange} to query all data (over the whole ring).
     *
     * @param metadata the table for which to create the {@code DataRange}.
     * @param partitioner the partitioner in use for the table.
     *
     * @return the newly create {@code DataRange}.
     */
    public static DataRange allData(CFMetaData metadata, IPartitioner partitioner)
    {
        return forTokenRange(metadata, new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
    }

    /**
     * Creates a {@code DataRange} to query all rows over the provided token range.
     *
     * @param metadata the table for which to create the {@code DataRange}.
     * @param tokenRange the (partition key) token range to query.
     *
     * @return the newly create {@code DataRange}.
     */
    public static DataRange forTokenRange(CFMetaData metadata, Range<Token> tokenRange)
    {
        return forKeyRange(metadata, Range.makeRowRange(tokenRange));
    }

    /**
     * Creates a {@code DataRange} to query all rows over the provided key range.
     *
     * @param metadata the table for which to create the {@code DataRange}.
     * @param keyRange the (partition key) range to query.
     *
     * @return the newly create {@code DataRange}.
     */
    public static DataRange forKeyRange(CFMetaData metadata, Range<PartitionPosition> keyRange)
    {
        return new DataRange(keyRange, new SlicePartitionFilter(metadata.partitionColumns(), Slices.ALL, false));
    }

    /**
     * Creates a {@code DataRange} to query all partitions of the ring using the provided
     * partition filter.
     *
     * @param partitioner the partitioner in use for the table queried.
     * @param filter the partition filter to use.
     *
     * @return the newly create {@code DataRange}.
     */
    public static DataRange allData(IPartitioner partitioner, PartitionFilter filter)
    {
        return new DataRange(Range.makeRowRange(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken())), filter);
    }

    /**
     * The range of partition key queried by this {@code DataRange}.
     *
     * @return the range of partition key queried by this {@code DataRange}.
     */
    public AbstractBounds<PartitionPosition> keyRange()
    {
        return keyRange;
    }

    /**
     * The start of the partition key range queried by this {@code DataRange}.
     *
     * @return the start of the partition key range queried by this {@code DataRange}.
     */
    public PartitionPosition startKey()
    {
        return keyRange.left;
    }

    /**
     * The end of the partition key range queried by this {@code DataRange}.
     *
     * @return the end of the partition key range queried by this {@code DataRange}.
     */
    public PartitionPosition stopKey()
    {
        return keyRange.right;
    }

    /**
     * Whether the underlying partition filter is a names filter or not.
     *
     * @return Whether the underlying partition filter is a names filter or not.
     */
    public boolean isNamesQuery()
    {
        return partitionFilter instanceof NamesPartitionFilter;
    }

    /**
     * Whether the data range is for a paged request or not.
     *
     * @return true if for paging, false otherwise
     */
    public boolean isPaging()
    {
        return false;
    }

    /**
     * The columns queried by the underlying partition filter.
     *
     * @return the columns queried by the underlying partition filter.
     */
    public ColumnsSelection queriedColumns()
    {
        return partitionFilter.queriedColumns();
    }

    /**
     * Whether the range queried by this {@code DataRange} actually wraps around.
     *
     * @return whether the range queried by this {@code DataRange} actually wraps around.
     */
    public boolean isWrapAround()
    {
        // Only range can ever wrap
        return keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround();
    }

    /**
     * Whether the provided ring position is covered by this {@code DataRange}.
     *
     * @return whether the provided ring position is covered by this {@code DataRange}.
     */
    public boolean contains(PartitionPosition pos)
    {
        return keyRange.contains(pos);
    }

    /**
     * Whether this {@code DataRange} queries everything (has no restriction neither on the
     * partition queried, nor within the queried partition).
     *
     * @return Whether this {@code DataRange} queries everything.
     */
    public boolean isUnrestricted()
    {
        return startKey().isMinimum() && stopKey().isMinimum() && partitionFilter.selectsAllPartition();
    }

    /**
     * The partition filter to use for the provided key.
     * <p>
     * This may or may not be the same filter for all keys (that is, paging range
     * use a different filter for their start key).
     *
     * @param key the partition key for which we want the partition filter.
     *
     * @return the partition filter to use for {@code key}.
     */
    public PartitionFilter partitionFilter(DecoratedKey key)
    {
        return partitionFilter;
    }

    /**
     * Returns a new {@code DataRange} for use when paging {@code this} range.
     *
     * @param range the range of partition keys to query.
     * @param comparator the comparator for the table queried.
     * @param lastReturned the clustering for the last result returned by the previous page, i.e. the result we want to start our new page
     * from. This last returned must <b>must</b> correspond to left bound of {@code range} (in other words, {@code range.left} must be the
     * partition key for that {@code lastReturned} result).
     * @param inclusive whether or not we want to include the {@code lastReturned} in the newly returned page of results.
     *
     * @return a new {@code DataRange} suitable for paging {@code this} range given the {@code lastRetuned} result of the previous page.
     */
    public DataRange forPaging(AbstractBounds<PartitionPosition> range, ClusteringComparator comparator, Clustering lastReturned, boolean inclusive)
    {
        return new Paging(range, partitionFilter, comparator, lastReturned, inclusive);
    }

    /**
     * Returns a new {@code DataRange} equivalent to {@code this} one but restricted to the provided sub-range.
     *
     * @param range the sub-range to use for the newly returned data range. Note that assumes that {@code range} is a proper
     * sub-range of the initial range but doesn't validate it. You should make sure to only provided sub-ranges however or this
     * might throw off the paging case (see Paging.forSubRange()).
     *
     * @return a new {@code DataRange} using {@code range} as partition key range and the partition filter filter from {@code this}.
     */
    public DataRange forSubRange(AbstractBounds<PartitionPosition> range)
    {
        return new DataRange(range, partitionFilter);
    }

    public String toString(CFMetaData metadata)
    {
        return String.format("range=%s pfilter=%s", keyRange.getString(metadata.getKeyValidator()), partitionFilter.toString(metadata));
    }

    public String toCQLString(CFMetaData metadata)
    {
        if (isUnrestricted())
            return "UNRESTRICTED";

        StringBuilder sb = new StringBuilder();

        boolean needAnd = false;
        if (!startKey().isMinimum())
        {
            appendClause(startKey(), sb, metadata, true, keyRange.isStartInclusive());
            needAnd = true;
        }
        if (!stopKey().isMinimum())
        {
            if (needAnd)
                sb.append(" AND ");
            appendClause(stopKey(), sb, metadata, false, keyRange.isEndInclusive());
        }

        String filterString = partitionFilter.toCQLString(metadata);
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);

        return sb.toString();
    }

    private void appendClause(PartitionPosition pos, StringBuilder sb, CFMetaData metadata, boolean isStart, boolean isInclusive)
    {
        sb.append("token(");
        sb.append(ColumnDefinition.toCQLString(metadata.partitionKeyColumns()));
        sb.append(") ").append(getOperator(isStart, isInclusive)).append(" ");
        if (pos instanceof DecoratedKey)
        {
            sb.append("token(");
            appendKeyString(sb, metadata.getKeyValidator(), ((DecoratedKey)pos).getKey());
            sb.append(")");
        }
        else
        {
            sb.append(((Token.KeyBound)pos).getToken());
        }
    }

    private static String getOperator(boolean isStart, boolean isInclusive)
    {
        return isStart
             ? (isInclusive ? ">=" : ">")
             : (isInclusive ? "<=" : "<");
    }

    // TODO: this is reused in SinglePartitionReadCommand but this should not really be here. Ideally
    // we need a more "native" handling of composite partition keys.
    public static void appendKeyString(StringBuilder sb, AbstractType<?> type, ByteBuffer key)
    {
        if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)type;
            ByteBuffer[] values = ct.split(key);
            for (int i = 0; i < ct.types.size(); i++)
                sb.append(i == 0 ? "" : ", ").append(ct.types.get(i).getString(values[i]));
        }
        else
        {
            sb.append(type.getString(key));
        }
    }

    /**
     * Specialized {@code DataRange} used for the paging case.
     * <p>
     * It uses the clustering of the last result of the previous page to restrict the filter on the
     * first queried partition (the one for that last result) so it only fetch results that follow that
     * last result. In other words, this makes sure this resume paging where we left off.
     */
    private static class Paging extends DataRange
    {
        private final ClusteringComparator comparator;
        private final Clustering lastReturned;
        private final boolean inclusive;

        private Paging(AbstractBounds<PartitionPosition> range,
                       PartitionFilter filter,
                       ClusteringComparator comparator,
                       Clustering lastReturned,
                       boolean inclusive)
        {
            super(range, filter);

            // When using a paging range, we don't allow wrapped ranges, as it's unclear how to handle them properly.
            // This is ok for now since we only need this in range queries, and the range are "unwrapped" in that case.
            assert !(range instanceof Range) || !((Range<?>)range).isWrapAround() || range.right.isMinimum() : range;
            assert lastReturned != null;

            this.comparator = comparator;
            this.lastReturned = lastReturned;
            this.inclusive = inclusive;
        }

        @Override
        public PartitionFilter partitionFilter(DecoratedKey key)
        {
            return key.equals(startKey())
                 ? partitionFilter.forPaging(comparator, lastReturned, inclusive)
                 : partitionFilter;
        }

        @Override
        public DataRange forSubRange(AbstractBounds<PartitionPosition> range)
        {
            // This is called for subrange of the initial range. So either it's the beginning of the initial range,
            // and we need to preserver lastReturned, or it's not, and we don't care about it anymore.
            return range.left.equals(keyRange().left)
                 ? new Paging(range, partitionFilter, comparator, lastReturned, inclusive)
                 : new DataRange(range, partitionFilter);
        }

        @Override
        public boolean isPaging()
        {
            return true;
        }

        @Override
        public boolean isUnrestricted()
        {
            return false;
        }
    }

    public static class Serializer
    {
        public void serialize(DataRange range, DataOutputPlus out, int version, CFMetaData metadata) throws IOException
        {
            AbstractBounds.rowPositionSerializer.serialize(range.keyRange, out, version);
            PartitionFilter.serializer.serialize(range.partitionFilter, out, version);
            boolean isPaging = range instanceof Paging;
            out.writeBoolean(isPaging);
            if (isPaging)
            {
                Clustering.serializer.serialize(((Paging)range).lastReturned, out, version, metadata.comparator.subtypes());
                out.writeBoolean(((Paging)range).inclusive);
            }
        }

        public DataRange deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
        {
            AbstractBounds<PartitionPosition> range = AbstractBounds.rowPositionSerializer.deserialize(in, MessagingService.globalPartitioner(), version);
            PartitionFilter filter = PartitionFilter.serializer.deserialize(in, version, metadata);
            if (in.readBoolean())
            {
                ClusteringComparator comparator = metadata.comparator;
                Clustering lastReturned = Clustering.serializer.deserialize(in, version, comparator.subtypes());
                boolean inclusive = in.readBoolean();
                return new Paging(range, filter, comparator, lastReturned, inclusive);
            }
            else
            {
                return new DataRange(range, filter);
            }
        }

        public long serializedSize(DataRange range, int version, CFMetaData metadata)
        {
            long size = AbstractBounds.rowPositionSerializer.serializedSize(range.keyRange, version)
                      + PartitionFilter.serializer.serializedSize(range.partitionFilter, version)
                      + 1; // isPaging boolean

            if (range instanceof Paging)
            {
                size += Clustering.serializer.serializedSize(((Paging)range).lastReturned, version, metadata.comparator.subtypes(), TypeSizes.NATIVE);
                size += 1; // inclusive boolean
            }
            return size;
        }
    }
}
