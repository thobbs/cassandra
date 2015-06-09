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
package org.apache.cassandra.db.partitions;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredPartitionIterators.class);

    private static final Serializer serializer = new Serializer();

    private static final Comparator<UnfilteredRowIterator> partitionComparator = new Comparator<UnfilteredRowIterator>()
    {
        public int compare(UnfilteredRowIterator p1, UnfilteredRowIterator p2)
        {
            return p1.partitionKey().compareTo(p2.partitionKey());
        }
    };

    public static final UnfilteredPartitionIterator EMPTY = new AbstractUnfilteredPartitionIterator()
    {
        public boolean isForThrift()
        {
            return false;
        }

        public boolean hasNext()
        {
            return false;
        }

        public UnfilteredRowIterator next()
        {
            throw new NoSuchElementException();
        }
    };

    private UnfilteredPartitionIterators() {}

    public interface MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions);
        public void close();
    }

    public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        UnfilteredRowIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : UnfilteredRowIterators.emptyIterator(command.metadata(),
                                                                     command.partitionKey(),
                                                                     command.partitionFilter().isReversed(),
                                                                     command.nowInSec());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole UnfilteredPartitionIterator.
        return new WrappingUnfilteredRowIterator(toReturn)
        {
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    // asserting this only now because it bothers Serializer if hasNext() is called before
                    // the previously returned iterator hasn't been fully consumed.
                    assert !iter.hasNext();

                    iter.close();
                }
            }
        };
    }

    public static PartitionIterator mergeAndFilter(List<UnfilteredPartitionIterator> iterators, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the UnfilteredRowIterators directly as RowIterators
        return filter(merge(iterators, listener));
    }

    public static PartitionIterator filter(final UnfilteredPartitionIterator iterator)
    {
        return new PartitionIterator()
        {
            private RowIterator next;

            public boolean hasNext()
            {
                while (next == null && iterator.hasNext())
                {
                    UnfilteredRowIterator rowIterator = iterator.next();
                    next = UnfilteredRowIterators.filter(rowIterator);
                    if (!iterator.isForThrift() && RowIterators.isEmpty(next))
                    {
                        rowIterator.close();
                        next = null;
                    }
                }
                return next != null;
            }

            public RowIterator next()
            {
                if (next == null && !hasNext())
                    throw new NoSuchElementException();

                RowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                try
                {
                    iterator.close();
                }
                finally
                {
                    if (next != null)
                        next.close();
                }
            }
        };
    }

    public static UnfilteredPartitionIterator merge(final List<? extends UnfilteredPartitionIterator> iterators, final MergeListener listener)
    {
        assert listener != null;
        assert !iterators.isEmpty();

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            private CFMetaData metadata;
            private DecoratedKey partitionKey;
            private boolean isReverseOrder;
            private int nowInSec;

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                metadata = current.metadata();
                partitionKey = current.partitionKey();
                isReverseOrder = current.isReverseOrder();
                nowInSec = current.nowInSec();

                // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                // Non-present iterator will thus be set to empty in getReduced.
                toMerge.set(idx, current);
            }

            protected UnfilteredRowIterator getReduced()
            {
                UnfilteredRowIterators.MergeListener rowListener = listener.getRowMergeListener(partitionKey, toMerge);

                // Replace nulls by empty iterators
                for (int i = 0; i < toMerge.size(); i++)
                    if (toMerge.get(i) == null)
                        toMerge.set(i, UnfilteredRowIterators.emptyIterator(metadata, partitionKey, isReverseOrder, nowInSec));

                return UnfilteredRowIterators.merge(toMerge, rowListener);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                for (int i = 0; i < iterators.size(); i++)
                    toMerge.add(null);
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static UnfilteredPartitionIterator mergeLazily(final List<? extends UnfilteredPartitionIterator> iterators)
    {
        assert !iterators.isEmpty();

        if (iterators.size() == 1)
            return iterators.get(0);

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return false;
            }

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                toMerge.add(current);
            }

            protected UnfilteredRowIterator getReduced()
            {
                return new LazilyInitializedUnfilteredRowIterator(toMerge.get(0).partitionKey())
                {
                    protected UnfilteredRowIterator initializeIterator()
                    {
                        return UnfilteredRowIterators.merge(toMerge);
                    }
                };
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static UnfilteredPartitionIterator removeDroppedColumns(UnfilteredPartitionIterator iterator, final Map<ColumnIdentifier, CFMetaData.DroppedColumn> droppedColumns)
    {
        return new FilteringPartitionIterator(iterator)
        {
            @Override
            protected FilteringRow makeRowFilter()
            {
                return new FilteringRow()
                {
                    @Override
                    protected boolean include(Cell cell)
                    {
                        return include(cell.column(), cell.livenessInfo().timestamp());
                    }

                    @Override
                    protected boolean include(ColumnDefinition c, DeletionTime dt)
                    {
                        return include(c, dt.markedForDeleteAt());
                    }

                    private boolean include(ColumnDefinition column, long timestamp)
                    {
                        CFMetaData.DroppedColumn dropped = droppedColumns.get(column.name);
                        return dropped == null || timestamp > dropped.droppedTime;
                    }
                };
            }

            @Override
            protected boolean shouldFilter(UnfilteredRowIterator iterator)
            {
                // TODO: We could have row iterators return the smallest timestamp they might return
                // (which we can get from sstable stats), and ignore any dropping if that smallest
                // timestamp is bigger that the biggest droppedColumns timestamp.

                // If none of the dropped columns is part of the columns that the iterator actually returns, there is nothing to do;
                for (ColumnDefinition c : iterator.columns())
                    if (droppedColumns.containsKey(c.name))
                        return true;

                return false;
            }
        };
    }

    public static void digest(UnfilteredPartitionIterator iterator, MessageDigest digest)
    {
        try (UnfilteredPartitionIterator iter = iterator)
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIterators.digest(partition, digest);
                }
            }
        }
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    /**
     * Wraps the provided iterator so it logs the returned rows/RT for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails)
    {
        return new WrappingUnfilteredPartitionIterator(iterator)
        {
            public UnfilteredRowIterator next()
            {
                return UnfilteredRowIterators.loggingIterator(super.next(), id, fullDetails);
            }
        };
    }

    public static class SingletonPartitionIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final UnfilteredRowIterator iterator;
        private boolean returned;
        private boolean isForThrift;

        public SingletonPartitionIterator(UnfilteredRowIterator iterator, boolean isForThrift)
        {
            this.iterator = iterator;
            this.isForThrift = isForThrift;
        }

        protected UnfilteredRowIterator computeNext()
        {
            if (returned)
                return endOfData();

            returned = true;
            return iterator;
        }

        public void close()
        {
            iterator.close();
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }
    }

    /**
     * Serialize each UnfilteredSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public void serialize(UnfilteredPartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                {
                    while (iter.hasNext())
                    {
                        try (UnfilteredRowIterator partition = iter.next())
                        {
                            serializePartition(partition, out, version);
                        }
                    }
                    return;
                }

            }

            out.writeBoolean(iter.isForThrift());
            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(partition, out, version);
                }
            }
            out.writeBoolean(false);
        }

        public UnfilteredPartitionIterator deserialize(final DataInput in, final int version, final SerializationHelper.Flag flag) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                int partitionCount = in.readInt();
                ArrayList<UnfilteredRowIterator> partitions = new ArrayList<>(partitionCount);
                for (int i = 0; i < partitionCount; i++)
                {
                    DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
                    boolean present = in.readBoolean();
                    assert present;
                    partitions.add(deserializePartition(in, key, version));
                }
                final Iterator<UnfilteredRowIterator> iterator = partitions.iterator();

                return new AbstractUnfilteredPartitionIterator()
                {
                    UnfilteredRowIterator next = null;

                    public boolean isForThrift()
                    {
                        return true;
                    }

                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    public UnfilteredRowIterator next()
                    {
                        next = iterator.next();
                        return next;
                    }

                    @Override
                    public void close()
                    {
                        if (next != null)
                            next.close();
                    }
                };
            }

            final boolean isForThrift = in.readBoolean();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public boolean isForThrift()
                {
                    return isForThrift;
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public UnfilteredRowIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, flag);
                        return next;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }

        void serializePartition(UnfilteredRowIterator partition, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
            Pair<DeletionInfo, Iterator<LegacyLayout.LegacyCell>> pair = LegacyLayout.fromUnfilteredRowIterator(partition);

            if (!pair.right.hasNext())
            {
                out.writeBoolean(false);
                return;
            }

            out.writeBoolean(true);
            UUIDSerializer.serializer.serialize(partition.metadata().cfId, out, version);

            DeletionTime.serializer.serialize(pair.left.getPartitionDeletion(), out);

            // begin serialization of the range tombstone list
            out.writeInt(pair.left.rangeCount());
            if (pair.left.hasRanges())
            {
                Iterator<RangeTombstone> rangeTombstoneIterator = pair.left.rangeIterator(false);  // TODO reversed?
                CompositeType type = CompositeType.getInstance(pair.left.rangeComparator().subtypes());
                while (rangeTombstoneIterator.hasNext())
                {
                    RangeTombstone rt = rangeTombstoneIterator.next();
                    Slice slice = rt.deletedSlice();
                    CompositeType.Builder startBuilder = type.builder();
                    CompositeType.Builder finishBuilder = type.builder();
                    for (int i = 0; i < slice.start().clustering().size(); i++)
                    {
                        startBuilder.add(slice.start().get(i));
                        finishBuilder.add(slice.end().get(i));
                    }

                    // TODO double check inclusive ends
                    ByteBufferUtil.writeWithShortLength(startBuilder.build(), out);
                    if (slice.end().isInclusive())
                        ByteBufferUtil.writeWithShortLength(startBuilder.build(), out);
                    else
                        ByteBufferUtil.writeWithShortLength(startBuilder.buildAsEndOfRange(), out);

                    out.writeInt(rt.deletionTime().localDeletionTime());
                    out.writeLong(rt.deletionTime().markedForDeleteAt());
                }
            }

            // begin cell serialization
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right);
            out.writeInt(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                ByteBufferUtil.writeWithShortLength(cell.name.encode(partition.metadata()), out);
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    out.writeByte(LegacyLayout.EXPIRATION_MASK);  // serialization flags
                    out.writeInt(cell.ttl);
                    out.writeInt(cell.localDeletionTime);
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
                {
                    out.writeByte(LegacyLayout.DELETION_MASK);  // serialization flags
                    out.writeLong(cell.timestamp);
                    out.writeInt(TypeSizes.NATIVE.sizeof(cell.localDeletionTime));
                    out.writeInt(cell.localDeletionTime);
                    continue;
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    out.writeByte(LegacyLayout.COUNTER_MASK);  // serialization flags
                    out.writeLong(Long.MIN_VALUE);  // timestampOfLastDelete
                }
                else
                {
                    out.writeByte(0);  // serialization flags
                }

                out.writeLong(cell.timestamp);
                ByteBufferUtil.writeWithLength(cell.value, out);
            }
        }

        public UnfilteredRowIterator deserializePartition(DataInput in, DecoratedKey key, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            LegacyLayout.LegacyDeletionInfo info = LegacyLayout.LegacyDeletionInfo.serializer.deserialize(metadata, in, version);
            int size = in.readInt();
            // TODO double-check that this is the correct flag to use
            SerializationHelper helper = new SerializationHelper(version, SerializationHelper.Flag.FROM_REMOTE, FBUtilities.nowInSeconds());
            Iterator<LegacyLayout.LegacyCell> cells = LegacyLayout.deserializeCells(metadata, in, SerializationHelper.Flag.FROM_REMOTE, size);
            return LegacyLayout.onWireCellstoUnfilteredRowIterator(metadata, key, info, cells, false, helper);
        }

        public long serializedSize(UnfilteredPartitionIterator iter, int version)
        {
            assert version < MessagingService.VERSION_30;
            TypeSizes sizes = TypeSizes.NATIVE;

            if (!iter.hasNext())
                return sizes.sizeof(false);

            long size = sizes.sizeof(true);
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    size += serializedPartitionSize(partition, version);
                }
            }
            return size;
        }

        long serializedPartitionSize(UnfilteredRowIterator partition, int version)
        {
            assert version < MessagingService.VERSION_30;
            TypeSizes sizes = TypeSizes.NATIVE;

            long size = ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey(), sizes);
            Pair<DeletionInfo, Iterator<LegacyLayout.LegacyCell>> pair = LegacyLayout.fromUnfilteredRowIterator(partition);

            if (!pair.right.hasNext())
                return size + sizes.sizeof(false);

            size += sizes.sizeof(true);
            size += UUIDSerializer.serializer.serializedSize(partition.metadata().cfId, version);
            size += DeletionTime.serializer.serializedSize(pair.left.getPartitionDeletion(), sizes);

            // begin range tombstone list
            size += sizes.sizeof(pair.left.rangeCount());
            if (pair.left.hasRanges())
            {
                Iterator<RangeTombstone> rangeTombstoneIterator = pair.left.rangeIterator(false);  // TODO reversed?
                CompositeType type = CompositeType.getInstance(pair.left.rangeComparator().subtypes());
                while (rangeTombstoneIterator.hasNext())
                {
                    RangeTombstone rt = rangeTombstoneIterator.next();
                    Slice slice = rt.deletedSlice();
                    CompositeType.Builder startBuilder = type.builder();
                    CompositeType.Builder finishBuilder = type.builder();
                    for (int i = 0; i < slice.start().clustering().size(); i++)
                    {
                        startBuilder.add(slice.start().get(i));
                        finishBuilder.add(slice.end().get(i));
                    }

                    size += ByteBufferUtil.serializedSizeWithShortLength(startBuilder.build(), sizes);
                    size += ByteBufferUtil.serializedSizeWithShortLength(finishBuilder.build(), sizes);
                    size += sizes.sizeof(rt.deletionTime().localDeletionTime());
                    size += sizes.sizeof(rt.deletionTime().markedForDeleteAt());
                }
            }

            // begin cell serialization
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right);
            size += sizes.sizeof(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(cell.name.encode(partition.metadata()), sizes);
                size += 1;  // serialization flags
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    size += sizes.sizeof(cell.ttl);
                    size += sizes.sizeof(cell.localDeletionTime);
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    size += sizes.sizeof(Long.MAX_VALUE);
                }
                size += sizes.sizeof(cell.timestamp);
                size += ByteBufferUtil.serializedSizeWithLength(cell.value, sizes);
            }

            return size;
        }
    }
}
