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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MergeIterator;

import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to work with partition iterators.
 */
public abstract class PartitionIterators
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionIterators.class);

    private static final Serializer serializer = new Serializer();

    private static final Comparator<AtomIterator> partitionComparator = new Comparator<AtomIterator>()
    {
        public int compare(AtomIterator p1, AtomIterator p2)
        {
            return p1.partitionKey().compareTo(p2.partitionKey());
        }
    };

    public static final PartitionIterator EMPTY = new AbstractPartitionIterator()
    {
        public boolean isForThrift()
        {
            return false;
        }

        public boolean hasNext()
        {
            return false;
        }

        public AtomIterator next()
        {
            throw new NoSuchElementException();
        }
    };

    private PartitionIterators() {}

    public interface MergeListener
    {
        public AtomIterators.MergeListener getAtomMergeListener(DecoratedKey partitionKey, List<AtomIterator> versions);
        public void close();
    }

    public static AtomIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        AtomIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : AtomIterators.emptyIterator(command.metadata(),
                                                            command.partitionKey(),
                                                            command.partitionFilter().isReversed(),
                                                            command.nowInSec());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole PartitionIterator.
        return new WrappingAtomIterator(toReturn)
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

    public static DataIterator mergeAsDataIterator(List<PartitionIterator> iterators, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the AtomIterators directly as RowIterators
        return asDataIterator(merge(iterators, listener));
    }

    public static DataIterator asDataIterator(final PartitionIterator iterator)
    {
        return new DataIterator()
        {
            private RowIterator next;

            public boolean hasNext()
            {
                while (next == null && iterator.hasNext())
                {
                    AtomIterator atomIter = iterator.next();
                    next = new RowIteratorFromAtomIterator(atomIter);
                    if (!iterator.isForThrift() && RowIterators.isEmpty(next))
                    {
                        atomIter.close();
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

    public static PartitionIterator merge(final List<? extends PartitionIterator> iterators, final MergeListener listener)
    {
        assert listener != null;
        assert !iterators.isEmpty();

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<AtomIterator, AtomIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<AtomIterator, AtomIterator>()
        {
            private final List<AtomIterator> toMerge = new ArrayList<>(iterators.size());

            private CFMetaData metadata;
            private DecoratedKey partitionKey;
            private boolean isReverseOrder;
            private int nowInSec;

            public void reduce(int idx, AtomIterator current)
            {
                metadata = current.metadata();
                partitionKey = current.partitionKey();
                isReverseOrder = current.isReverseOrder();
                nowInSec = current.nowInSec();

                // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                // Non-present iterator will thus be set to empty in getReduced.
                toMerge.set(idx, current);
            }

            protected AtomIterator getReduced()
            {
                AtomIterators.MergeListener atomListener = listener.getAtomMergeListener(partitionKey, toMerge);

                // Replace nulls by empty iterators
                for (int i = 0; i < toMerge.size(); i++)
                    if (toMerge.get(i) == null)
                        toMerge.set(i, AtomIterators.emptyIterator(metadata, partitionKey, isReverseOrder, nowInSec));

                return AtomIterators.merge(toMerge, atomListener);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                for (int i = 0; i < iterators.size(); i++)
                    toMerge.add(null);
            }
        });

        return new AbstractPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public AtomIterator next()
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

    public static PartitionIterator mergeLazily(final List<? extends PartitionIterator> iterators)
    {
        assert !iterators.isEmpty();

        if (iterators.size() == 1)
            return iterators.get(0);

        final boolean isForThrift = iterators.get(0).isForThrift();

        final MergeIterator<AtomIterator, AtomIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<AtomIterator, AtomIterator>()
        {
            private final List<AtomIterator> toMerge = new ArrayList<>(iterators.size());

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return false;
            }

            public void reduce(int idx, AtomIterator current)
            {
                toMerge.add(current);
            }

            protected AtomIterator getReduced()
            {
                return new LazilyInitializedAtomIterator(toMerge.get(0).partitionKey())
                {
                    protected AtomIterator initializeIterator()
                    {
                        return AtomIterators.merge(toMerge);
                    }
                };
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });

        return new AbstractPartitionIterator()
        {
            public boolean isForThrift()
            {
                return isForThrift;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public AtomIterator next()
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

    public static PartitionIterator removeDroppedColumns(PartitionIterator iterator, final Map<ColumnIdentifier, CFMetaData.DroppedColumn> droppedColumns)
    {
        return new AbstractFilteringIterator(iterator)
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
            protected boolean shouldFilter(AtomIterator atoms)
            {
                // TODO: We could have atom iterators return the smallest timestamp they might return
                // (which we can get from sstable stats), and ignore any dropping if that smallest
                // timestamp is bigger that the biggest droppedColumns timestamp.

                // If none of the dropped columns is part of the columns that the iterator actually returns, there is nothing to do;
                for (ColumnDefinition c : atoms.columns())
                    if (droppedColumns.containsKey(c.name))
                        return true;

                return false;
            }
        };
    }

    public static void digest(PartitionIterator iterator, MessageDigest digest)
    {
        try (PartitionIterator iter = iterator)
        {
            while (iter.hasNext())
            {
                try (AtomIterator partition = iter.next())
                {
                    AtomIterators.digest(partition, digest);
                }
            }
        }
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    /**
     * Wraps the provided iterator so it logs the returned atoms for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static PartitionIterator loggingIterator(PartitionIterator iterator, final String id, final boolean fullDetails)
    {
        return new WrappingPartitionIterator(iterator)
        {
            public AtomIterator next()
            {
                return AtomIterators.loggingIterator(super.next(), id, fullDetails);
            }
        };
    }

    /**
     * Serialize each AtomSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public void serialize(PartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                while (iter.hasNext())
                {
                    try (AtomIterator partition = iter.next())
                    {
                        serializePartition(partition, out, version);
                    }
                }
            }

            out.writeBoolean(iter.isForThrift());
            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (AtomIterator partition = iter.next())
                {
                    AtomIteratorSerializer.serializer.serialize(partition, out, version);
                }
            }
            out.writeBoolean(false);
        }

        void serializePartition(AtomIterator partition, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
            Pair<DeletionInfo, Iterator<LegacyLayout.LegacyCell>> pair = LegacyLayout.fromAtomIterator(partition);

            if (!pair.right.hasNext())
            {
                out.writeBoolean(false);
                return;
            }

            out.writeBoolean(true);
            UUIDSerializer.serializer.serialize(partition.metadata().cfId, out, version);

            DeletionTime.serializer.serialize(pair.left.getPartitionDeletion(), out);
            // TODO this is the number of range tombstones, and would normally be followed by serialization
            // of the range tombstone list
            out.writeInt(0);

            // begin cell serialization
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right);
            out.writeInt(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
                {
                    throw new UnsupportedOperationException("Deleted cells are not supported yet");
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    throw new UnsupportedOperationException("Deleted cells are not supported yet");
                    // TODO need to write timestampOfLastDelete, what does that correspond to?
                }

                ByteBufferUtil.writeWithShortLength(cell.name.encode(partition.metadata()), out);
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    out.writeByte(LegacyLayout.EXPIRATION_MASK);  // serialization flags
                    out.writeInt(cell.ttl);
                    out.writeInt(cell.localDeletionTime);
                }
                else
                {
                    out.writeByte(0);  // cell serialization flags
                }
                out.writeLong(cell.timestamp);
                ByteBufferUtil.writeWithLength(cell.value, out);
            }
        }

        public PartitionIterator deserialize(final DataInput in, final int version, final SerializationHelper.Flag flag) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            final boolean isForThrift = in.readBoolean();

            return new AbstractPartitionIterator()
            {
                private AtomIterator next;
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

                public AtomIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = AtomIteratorSerializer.serializer.deserialize(in, version, flag);
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

        public long serializedSize(PartitionIterator iter, int version)
        {
            assert version < MessagingService.VERSION_30;
            TypeSizes sizes = TypeSizes.NATIVE;

            if (!iter.hasNext())
                return sizes.sizeof(false);

            long size = sizes.sizeof(true);
            while (iter.hasNext())
            {
                try (AtomIterator partition = iter.next())
                {
                    size += serializedPartitionSize(partition, version);
                }
            }
            return size;
        }

        long serializedPartitionSize(AtomIterator partition, int version)
        {
            assert version < MessagingService.VERSION_30;
            TypeSizes sizes = TypeSizes.NATIVE;

            long size = ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey(), sizes);
            Pair<DeletionInfo, Iterator<LegacyLayout.LegacyCell>> pair = LegacyLayout.fromAtomIterator(partition);

            if (!pair.right.hasNext())
                return size + sizes.sizeof(false);

            size += sizes.sizeof(true);
            size += UUIDSerializer.serializer.serializedSize(partition.metadata().cfId, version);
            size += DeletionTime.serializer.serializedSize(pair.left.getPartitionDeletion(), sizes);
            // TODO this is the number of range tombstones, and would normally be followed by serialization
            // of the range tombstone list
            size += sizes.sizeof(0);

            // begin cell serialization
            List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right);
            size += sizes.sizeof(cells.size());
            for (LegacyLayout.LegacyCell cell : cells)
            {
                if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
                {
                    throw new UnsupportedOperationException("Deleted cells are not supported yet");
                }
                else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
                {
                    throw new UnsupportedOperationException("Deleted cells are not supported yet");
                    // TODO need to write timestampOfLastDelete, what does that correspond to?
                }

                size += ByteBufferUtil.serializedSizeWithShortLength(cell.name.encode(partition.metadata()), sizes);
                size += 1;  // serialization flags
                if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
                {
                    size += sizes.sizeof(cell.ttl);
                    size += sizes.sizeof(cell.localDeletionTime);
                }
                size += sizes.sizeof(cell.timestamp);
                size += ByteBufferUtil.serializedSizeWithLength(cell.value, sizes);
            }

            return size;
        }
    }
}
