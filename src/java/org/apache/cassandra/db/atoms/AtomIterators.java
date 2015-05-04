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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;
import java.util.*;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Static methods to work with atom iterators.
 */
public abstract class AtomIterators
{
    private static final Logger logger = LoggerFactory.getLogger(AtomIterators.class);

    private AtomIterators() {}

    public interface MergeListener
    {
        public void onMergePartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions);

        public void onMergingRows(Clustering clustering, LivenessInfo mergedInfo, DeletionTime mergedDeletion, Row[] versions);
        public void onMergedComplexDeletion(ColumnDefinition c, DeletionTime mergedComplexDeletion, DeletionTime[] versions);
        public void onMergedCells(Cell mergedCell, Cell[] versions);
        public void onRowDone();

        public void onMergedRangeTombstoneMarkers(Slice.Bound bound, DeletionTime mergedDeletion, RangeTombstoneMarker[] versions);

        public void close();
    }

    /**
     * Returns whether the provided iterator has no data (including no deletion data).
     */
    public static boolean isEmpty(AtomIterator iterator)
    {
        return iterator.partitionLevelDeletion().isLive()
            && iterator.staticRow().isEmpty()
            && !iterator.hasNext();
    }

    /**
     * Returns a iterator that only returns rows with only live content.
     *
     * This is mainly used in the CQL layer when we know we don't care about deletion
     * infos (and since an AtomIterator cannot shadow it's own data, we know everyting
     * returned isn't shadowed by a tombstone).
     */
    public static RowIterator asRowIterator(AtomIterator iter)
    {
        return new RowIteratorFromAtomIterator(iter);
    }

    /**
     * Returns an iterator that is the result of merging other iterators.
     */
    public static AtomIterator merge(List<AtomIterator> iterators)
    {
        assert !iterators.isEmpty();
        if (iterators.size() == 1)
            return iterators.get(0);

        return AtomMergeIterator.create(iterators, null);
    }

    /**
     * Returns an iterator that is the result of merging other iterators, and using
     * specific MergeListener.
     *
     * Note that this method assumes that there is at least 2 iterators to merge.
     */
    public static AtomIterator merge(List<AtomIterator> iterators, MergeListener mergeListener)
    {
        assert mergeListener != null;
        return AtomMergeIterator.create(iterators, mergeListener);
    }

    /**
     * Returns an empty atom iterator for a given partition.
     */
    public static AtomIterator emptyIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final boolean isReverseOrder, final int nowInSec)
    {
        return new AtomIterator()
        {
            public CFMetaData metadata()
            {
                return cfm;
            }

            public boolean isReverseOrder()
            {
                return isReverseOrder;
            }

            public PartitionColumns columns()
            {
                return PartitionColumns.NONE;
            }

            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return DeletionTime.LIVE;
            }

            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }

            public AtomStats stats()
            {
                return AtomStats.NO_STATS;
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public boolean hasNext()
            {
                return false;
            }

            public Atom next()
            {
                throw new NoSuchElementException();
            }

            public void remove()
            {
            }

            public void close()
            {
            }
        };
    }

    public static void digest(AtomIterator iterator, MessageDigest digest)
    {
        // TODO: we're not computing digest the same way that old nodes. This
        // means we'll have digest mismatches during upgrade. We should pass the messaging version of
        // the node this is for (which might mean computing the digest last, and won't work
        // for schema (where we announce the version through gossip to everyone))
        digest.update(iterator.partitionKey().getKey().duplicate());
        iterator.partitionLevelDeletion().digest(digest);
        iterator.columns().digest(digest);
        FBUtilities.updateWithBoolean(digest, iterator.isReverseOrder());
        iterator.staticRow().digest(digest);

        while (iterator.hasNext())
        {
            Atom atom = iterator.next();
            if (atom.kind() == Atom.Kind.ROW)
                ((Row)atom).digest(digest);
            else
                ((RangeTombstoneMarker)atom).digest(digest);
        }
    }

    /**
     * Returns an iterator that concatenate two atom iterators.
     * This method assumes that both iterator are from the same partition and that the atom from
     * {@code iter2} come after the ones of {@code iter1} (that is, that concatenating the iterator
     * make sense).
     */
    public static AtomIterator concat(final AtomIterator iter1, final AtomIterator iter2)
    {
        assert iter1.metadata().cfId.equals(iter2.metadata().cfId)
            && iter1.partitionKey().equals(iter2.partitionKey())
            && iter1.partitionLevelDeletion().equals(iter2.partitionLevelDeletion())
            && iter1.isReverseOrder() == iter2.isReverseOrder()
            && iter1.columns().equals(iter2.columns())
            && iter1.staticRow().equals(iter2.staticRow())
            && iter1.nowInSec() == iter2.nowInSec();

        return new AbstractAtomIterator(iter1.metadata(),
                                        iter1.partitionKey(),
                                        iter1.partitionLevelDeletion(),
                                        iter1.columns(),
                                        iter1.staticRow(),
                                        iter1.isReverseOrder(),
                                        iter1.stats(),
                                        iter1.nowInSec())
        {
            protected Atom computeNext()
            {
                if (iter1.hasNext())
                    return iter1.next();

                return iter2.hasNext() ? iter2.next() : endOfData();
            }

            @Override
            public void close()
            {
                try
                {
                    iter1.close();
                }
                finally
                {
                    iter2.close();
                }
            }
        };
    }

    public static AtomIterator cloningIterator(AtomIterator iterator, final AbstractAllocator allocator)
    {
        return new WrappingAtomIterator(iterator)
        {
            private final CloningRow cloningRow = new CloningRow();
            private final CloningMarker cloningMarker = new CloningMarker();

            public Atom next()
            {
                Atom next = super.next();
                return next.kind() == Atom.Kind.ROW
                     ? cloningRow.setTo((Row)next)
                     : cloningMarker.setTo((RangeTombstoneMarker)next);

            }

            class CloningRow extends WrappingRow
            {
                private final CloningClustering cloningClustering = new CloningClustering();
                private final CloningCell cloningCell = new CloningCell();

                protected Cell filterCell(Cell cell)
                {
                    return cloningCell.setTo(cell);
                }

                @Override
                public Clustering clustering()
                {
                    return cloningClustering.setTo(super.clustering());
                }
            }

            class CloningClustering extends Clustering
            {
                private Clustering wrapped;

                public Clustering setTo(Clustering wrapped)
                {
                    this.wrapped = wrapped;
                    return this;
                }

                public int size()
                {
                    return wrapped.size();
                }

                public ByteBuffer get(int i)
                {
                    ByteBuffer value = wrapped.get(i);
                    return value == null ? null : allocator.clone(value);
                }
            }

            class CloningCell extends AbstractCell
            {
                private Cell wrapped;

                public Cell setTo(Cell wrapped)
                {
                    this.wrapped = wrapped;
                    return this;
                }

                public ColumnDefinition column()
                {
                    return wrapped.column();
                }

                public boolean isCounterCell()
                {
                    return wrapped.isCounterCell();
                }

                public ByteBuffer value()
                {
                    return allocator.clone(wrapped.value());
                }

                public LivenessInfo livenessInfo()
                {
                    return wrapped.livenessInfo();
                }

                public CellPath path()
                {
                    CellPath path = wrapped.path();
                    if (path == null)
                        return null;

                    assert path.size() == 1;
                    return CellPath.create(allocator.clone(path.get(0)));
                }
            }

            class CloningSliceBound extends Slice.Bound
            {
                private Slice.Bound wrapped;

                public Slice.Bound setTo(Slice.Bound wrapped)
                {
                    this.wrapped = wrapped;
                    return this;
                }

                public Kind kind()
                {
                    return wrapped.kind();
                }

                public int size()
                {
                    return wrapped.size();
                }

                public ByteBuffer get(int i)
                {
                    ByteBuffer value = wrapped.get(i);
                    return value == null ? null : allocator.clone(value);
                }

                public Slice.Bound withNewKind(Kind kind)
                {
                    return takeAlias().withNewKind(kind);
                }

                public Slice.Bound takeAlias()
                {
                    ByteBuffer[] values = new ByteBuffer[size()];
                    for (int i = 0; i < size(); i++)
                        values[i] = get(i);
                    return Slice.Bound.create(kind(), values);
                }
            }

            class CloningMarker extends AbstractRangeTombstoneMarker
            {
                private final CloningSliceBound cloningBound = new CloningSliceBound();
                private RangeTombstoneMarker wrapped;

                public RangeTombstoneMarker setTo(RangeTombstoneMarker wrapped)
                {
                    this.wrapped = wrapped;
                    return this;
                }

                public Slice.Bound clustering()
                {
                    return cloningBound.setTo(clustering());
                }

                public DeletionTime deletionTime()
                {
                    return wrapped.deletionTime();
                }
            }
        };
    }

    /**
     * Turns the given iterator into an update.
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    public static PartitionUpdate toUpdate(AtomIterator iterator)
    {
        PartitionUpdate update = new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), iterator.columns(), 1, iterator.nowInSec());

        update.addPartitionDeletion(iterator.partitionLevelDeletion());

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            iterator.staticRow().copyTo(update.staticWriter());

        while (iterator.hasNext())
        {
            Atom atom = iterator.next();
            if (atom.kind() == Atom.Kind.ROW)
                ((Row)atom).copyTo(update.writer());
            else
                ((RangeTombstoneMarker)atom).copyTo(update.markerWriter());
        }

        return update;
    }

    /**
     * Validate that the data of the provided iterator is valid, that is that the values
     * it contains are valid for the type they represent, and more generally that the
     * infos stored are sensible.
     *
     * This is mainly used by scrubber to detect problems in sstables.
     *
     * @param iterator the partition to check.
     * @param filename the name of the file the data is comming from.
     * @return an iterator that returns the same data than {@code iterator} but that
     * checks said data and throws a {@code CorruptedSSTableException} if it detects
     * invalid data.
     */
    public static AtomIterator withValidation(AtomIterator iterator, final String filename)
    {
        return new WrappingAtomIterator(iterator)
        {
            public Atom next()
            {
                Atom next = super.next();
                try
                {
                    next.validateData(metadata());
                    return next;
                }
                catch (MarshalException me)
                {
                    throw new CorruptSSTableException(me, filename);
                }
            }
        };
    }

    /**
     * Wraps the provided iterator so it logs the returned atoms for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static AtomIterator loggingIterator(AtomIterator iterator, final String id, final boolean fullDetails)
    {
        CFMetaData metadata = iterator.metadata();
        logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}, deletion={}",
                    new Object[]{ id,
                                  metadata.ksName,
                                  metadata.cfName,
                                  metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                  iterator.isReverseOrder(),
                                  iterator.partitionLevelDeletion().markedForDeleteAt()});

        return new WrappingAtomIterator(iterator)
        {
            @Override
            public Row staticRow()
            {
                Row row = super.staticRow();
                if (!row.isEmpty())
                    logger.info("[{}] {}", id, row.toString(metadata(), fullDetails));
                return row;
            }

            @Override
            public Atom next()
            {
                Atom next = super.next();
                if (next.kind() == Atom.Kind.ROW)
                    logger.info("[{}] {}", id, ((Row)next).toString(metadata(), fullDetails));
                else
                    logger.info("[{}] {}", id, ((RangeTombstoneMarker)next).toString(metadata()));
                return next;
            }
        };
    }

    /**
     * A wrapper over MergeIterator to implement the AtomIterator interface.
     */
    private static class AtomMergeIterator extends AbstractAtomIterator
    {
        private final IMergeIterator<Atom, MergedAtom> mergeIterator;
        private final MergeListener listener;

        private Atom toReturn;

        private AtomMergeIterator(CFMetaData metadata,
                                  List<AtomIterator> iterators,
                                  PartitionColumns columns,
                                  DeletionTime partitionDeletion,
                                  int nowInSec,
                                  boolean reversed,
                                  MergeListener listener)
        {
            super(metadata,
                  iterators.get(0).partitionKey(),
                  partitionDeletion,
                  columns,
                  mergeStaticRows(metadata, iterators, columns.statics, nowInSec, listener, partitionDeletion),
                  reversed,
                  mergeStats(iterators),
                  nowInSec);

            this.listener = listener;
            this.mergeIterator = MergeIterator.get(iterators,
                                                   reversed ? metadata.comparator.reversed() : metadata.comparator,
                                                   new MergeReducer(metadata, iterators.size(), nowInSec));
        }

        private static AtomMergeIterator create(List<AtomIterator> iterators, MergeListener listener)
        {
            assert inputIsValid(iterators);
            return new AtomMergeIterator(iterators.get(0).metadata(),
                                         iterators,
                                         collectColumns(iterators),
                                         collectPartitionLevelDeletion(iterators, listener),
                                         iterators.get(0).nowInSec(),
                                         iterators.get(0).isReverseOrder(),
                                         listener);
        }

        private static boolean inputIsValid(List<AtomIterator> iterators)
        {
            if (iterators.isEmpty())
                return false;

            AtomIterator first = iterators.get(0);
            for (int i = 1; i < iterators.size(); i++)
            {
                AtomIterator iter = iterators.get(i);
                if (!first.metadata().cfId.equals(iter.metadata().cfId)
                 || !first.partitionKey().equals(iter.partitionKey())
                 || first.isReverseOrder() != iter.isReverseOrder()
                 || first.nowInSec() != iter.nowInSec())
                {
                    return false;
                }
            }
            return true;
        }

        private static DeletionTime collectPartitionLevelDeletion(List<AtomIterator> iterators, MergeListener listener)
        {
            DeletionTime[] versions = listener == null ? null : new DeletionTime[iterators.size()];

            DeletionTime delTime = DeletionTime.LIVE;
            for (int i = 0; i < iterators.size(); i++)
            {
                AtomIterator iter = iterators.get(i);
                DeletionTime iterDeletion = iter.partitionLevelDeletion();
                if (listener != null)
                    versions[i] = iterDeletion;
                if (!delTime.supersedes(iterDeletion))
                    delTime = iterDeletion;
            }
            if (listener != null && !delTime.isLive())
                listener.onMergePartitionLevelDeletion(delTime, versions);
            return delTime;
        }

        private static Row mergeStaticRows(CFMetaData metadata,
                                           List<AtomIterator> iterators,
                                           Columns columns,
                                           int nowInSec,
                                           MergeListener listener,
                                           DeletionTime partitionDeletion)
        {
            if (columns.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            Rows.Merger merger = Rows.Merger.createStatic(metadata, iterators.size(), nowInSec, columns, listener);
            for (int i = 0; i < iterators.size(); i++)
                merger.add(i, iterators.get(i).staticRow());

            // Note that we should call 'takeAlias' on the result in theory, but we know that we
            // won't reuse the merger and so that it's ok not to.
            Row merged = merger.merge(partitionDeletion);
            return merged == null ? Rows.EMPTY_STATIC_ROW : merged;
        }

        private static PartitionColumns collectColumns(List<AtomIterator> iterators)
        {
            PartitionColumns first = iterators.get(0).columns();
            Columns statics = first.statics;
            Columns regulars = first.regulars;
            for (int i = 1; i < iterators.size(); i++)
            {
                PartitionColumns cols = iterators.get(i).columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return statics == first.statics && regulars == first.regulars
                 ? first
                 : new PartitionColumns(statics, regulars);
        }

        private static AtomStats mergeStats(List<AtomIterator> iterators)
        {
            AtomStats stats = AtomStats.NO_STATS;
            for (AtomIterator iter : iterators)
                stats = stats.mergeWith(iter.stats());
            return stats;
        }

        protected Atom computeNext()
        {
            if (toReturn != null)
            {
                Atom a = toReturn;
                toReturn = null;
                return a;
            }

            while (mergeIterator.hasNext())
            {
                MergedAtom merged = mergeIterator.next();
                if (!merged.isSingleAtom())
                {
                    toReturn = merged.getSecondAtom();
                    return merged.getAtom();
                }

                if (merged.getAtom() != null)
                    return merged.getAtom();
            }
            return endOfData();
        }

        public void close()
        {
            // This will close the input iterators
            FileUtils.closeQuietly(mergeIterator);

            if (listener != null)
                listener.close();
        }

        /**
         * Specific reducer for merge operations that rewrite the same reusable
         * row every time. This also skip cells shadowed by range tombstones when writing.
         */
        private class MergeReducer extends MergeIterator.Reducer<Atom, MergedAtom>
        {
            private Atom.Kind nextKind;

            private final MergedAtom mergedAtom = new MergedAtom();

            private final Rows.Merger rowMerger;
            private final RangeTombstoneMarkers.Merger markerMerger;

            private MergeReducer(CFMetaData metadata, int size, int nowInSec)
            {
                this.rowMerger = Rows.Merger.createRegular(metadata, size, nowInSec, columns().regulars, listener);
                this.markerMerger = new RangeTombstoneMarkers.Merger(metadata, size, partitionLevelDeletion(), listener);
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return listener == null;
            }

            public void reduce(int idx, Atom current)
            {
                nextKind = current.kind();
                if (nextKind == Atom.Kind.ROW)
                    rowMerger.add(idx, (Row)current);
                else
                    markerMerger.add(idx, (RangeTombstoneMarker)current);
            }

            protected MergedAtom getReduced()
            {
                return nextKind == Atom.Kind.ROW
                     ? mergedAtom.setTo(rowMerger.merge(markerMerger.activeDeletion()))
                     : markerMerger.merge(mergedAtom);
            }

            protected void onKeyChange()
            {
                mergedAtom.reset();
                if (nextKind == Atom.Kind.ROW)
                    rowMerger.clear();
                else
                    markerMerger.clear();
            }
        }
    }

    /**
     * A MergedAtom is either an Atom (a row or a rangeTombstoneMarker) or a pair
     * of RangeTombstoneMarker.
     *
     * The reason we need this is that when we merge closing markers, we may have
     * to "generate" both a close marker and a new open one. But MergeIterator doesn't
     * allow us to return 2 Atoms that way, so we group them under a MergedAtom and
     * AtomMergeIterator untangled it just afterwards.
     */
    static class MergedAtom
    {
        private Atom first;
        private Atom second;

        public MergedAtom setTo(Atom atom)
        {
            this.first = atom;
            return this;
        }

        public MergedAtom setSecondTo(Atom atom)
        {
            this.second = atom;
            return this;
        }

        public boolean isSingleAtom()
        {
            return second == null;
        }

        public Atom getAtom()
        {
            return first;
        }

        public Atom getSecondAtom()
        {
            return second;
        }

        public MergedAtom reset()
        {
            first = second = null;
            return this;
        }
    }
}
