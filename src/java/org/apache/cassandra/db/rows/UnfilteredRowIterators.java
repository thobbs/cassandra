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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.*;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Static methods to work with atom iterators.
 */
public abstract class UnfilteredRowIterators
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredRowIterators.class);

    private UnfilteredRowIterators() {}

    public interface MergeListener
    {
        public void onMergePartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions);

        public void onMergingRows(Clustering clustering, LivenessInfo mergedInfo, DeletionTime mergedDeletion, Row[] versions);
        public void onMergedComplexDeletion(ColumnDefinition c, DeletionTime mergedComplexDeletion, DeletionTime[] versions);
        public void onMergedCells(Cell mergedCell, Cell[] versions);
        public void onRowDone();

        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions);

        public void close();
    }

    /**
     * Returns a iterator that only returns rows with only live content.
     *
     * This is mainly used in the CQL layer when we know we don't care about deletion
     * infos (and since an UnfilteredRowIterator cannot shadow it's own data, we know everyting
     * returned isn't shadowed by a tombstone).
     */
    public static RowIterator filter(UnfilteredRowIterator iter, int nowInSec)
    {
        return new FilteringIterator(iter, nowInSec);

    }

    /**
     * Returns an iterator that is the result of merging other iterators.
     */
    public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec)
    {
        assert !iterators.isEmpty();
        if (iterators.size() == 1)
            return iterators.get(0);

        return UnfilteredRowMergeIterator.create(iterators, nowInSec, null);
    }

    /**
     * Returns an iterator that is the result of merging other iterators, and using
     * specific MergeListener.
     *
     * Note that this method assumes that there is at least 2 iterators to merge.
     */
    public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec, MergeListener mergeListener)
    {
        assert mergeListener != null;
        return UnfilteredRowMergeIterator.create(iterators, nowInSec, mergeListener);
    }

    /**
     * Returns an empty atom iterator for a given partition.
     */
    public static UnfilteredRowIterator emptyIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final boolean isReverseOrder)
    {
        return new UnfilteredRowIterator()
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

            public RowStats stats()
            {
                return RowStats.NO_STATS;
            }

            public boolean hasNext()
            {
                return false;
            }

            public Unfiltered next()
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

    public static void digest(UnfilteredRowIterator iterator, MessageDigest digest)
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
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                ((Row) unfiltered).digest(digest);
            else
                ((RangeTombstoneMarker) unfiltered).digest(digest);
        }
    }

    /**
     * Returns an iterator that concatenate two atom iterators.
     * This method assumes that both iterator are from the same partition and that the atom from
     * {@code iter2} come after the ones of {@code iter1} (that is, that concatenating the iterator
     * make sense).
     */
    public static UnfilteredRowIterator concat(final UnfilteredRowIterator iter1, final UnfilteredRowIterator iter2)
    {
        assert iter1.metadata().cfId.equals(iter2.metadata().cfId)
            && iter1.partitionKey().equals(iter2.partitionKey())
            && iter1.partitionLevelDeletion().equals(iter2.partitionLevelDeletion())
            && iter1.isReverseOrder() == iter2.isReverseOrder()
            && iter1.columns().equals(iter2.columns())
            && iter1.staticRow().equals(iter2.staticRow());

        return new AbstractUnfilteredRowIterator(iter1.metadata(),
                                        iter1.partitionKey(),
                                        iter1.partitionLevelDeletion(),
                                        iter1.columns(),
                                        iter1.staticRow(),
                                        iter1.isReverseOrder(),
                                        iter1.stats())
        {
            protected Unfiltered computeNext()
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

    public static UnfilteredRowIterator cloningIterator(UnfilteredRowIterator iterator, final AbstractAllocator allocator)
    {
        return new WrappingUnfilteredRowIterator(iterator)
        {
            private final CloningRow cloningRow = new CloningRow();
            private final RangeTombstoneMarker.Builder markerBuilder = new RangeTombstoneMarker.Builder(iterator.metadata().comparator.size());

            public Unfiltered next()
            {
                Unfiltered next = super.next();
                return next.kind() == Unfiltered.Kind.ROW
                     ? cloningRow.setTo((Row)next)
                     : clone((RangeTombstoneMarker)next);
            }

            private RangeTombstoneMarker clone(RangeTombstoneMarker marker)
            {
                markerBuilder.reset();

                RangeTombstone.Bound bound = marker.clustering();
                for (int i = 0; i < bound.size(); i++)
                    markerBuilder.writeClusteringValue(allocator.clone(bound.get(i)));
                markerBuilder.writeBoundKind(bound.kind());
                if (marker.isBoundary())
                {
                    RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
                    markerBuilder.writeBoundaryDeletion(bm.endDeletionTime(), bm.startDeletionTime());
                }
                else
                {
                    markerBuilder.writeBoundDeletion(((RangeTombstoneBoundMarker)marker).deletionTime());
                }
                markerBuilder.endOfMarker();
                return markerBuilder.build();
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

                public ByteBuffer[] getRawValues()
                {
                    throw new UnsupportedOperationException();
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
        };
    }

    /**
     * Turns the given iterator into an update.
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    public static PartitionUpdate toUpdate(UnfilteredRowIterator iterator)
    {
        PartitionUpdate update = new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), iterator.columns(), 1);

        update.addPartitionDeletion(iterator.partitionLevelDeletion());

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            iterator.staticRow().copyTo(update.staticWriter());

        RangeTombstoneMarker.Writer tombstoneWriter = update.markerWriter(iterator.isReverseOrder());
        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                ((Row) unfiltered).copyTo(update.writer());
            else
                ((RangeTombstoneMarker) unfiltered).copyTo(tombstoneWriter);
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
    public static UnfilteredRowIterator withValidation(UnfilteredRowIterator iterator, final String filename)
    {
        return new WrappingUnfilteredRowIterator(iterator)
        {
            public Unfiltered next()
            {
                Unfiltered next = super.next();
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
     * Convert all expired cells to equivalent tombstones.
     * <p>
     * Once a cell expires, it acts exactly as a tombstone and this until it is purged. But in particular that
     * means we don't care about the value of an expired cell, and it is thus equivalent but more efficient to
     * replace the expired cell by an equivalent tombstone (that has no value).
     *
     * @param iterator the iterator in which to conver expired cells.
     * @param nowInSec the current time to use to decide if a cell is expired.
     * @return an iterator that returns the same data than {@code iterator} but with all expired cells converted
     * to equivalent tombstones.
     */
    public static UnfilteredRowIterator convertExpiredCellsToTombstones(UnfilteredRowIterator iterator, final int nowInSec)
    {
        return new FilteringRowIterator(iterator)
        {
            protected FilteringRow makeRowFilter()
            {
                return new FilteringRow()
                {
                    @Override
                    protected Cell filterCell(Cell cell)
                    {
                        Cell filtered = super.filterCell(cell);
                        if (filtered == null)
                            return null;

                        LivenessInfo info = filtered.livenessInfo();
                        if (info.hasTTL() && !filtered.isLive(nowInSec))
                        {
                            // The column is now expired, we can safely return a simple tombstone. Note that as long as the expiring
                            // column and the tombstone put together live longer than GC grace seconds, we'll fulfil our responsibility
                            // to repair. See discussion at
                            // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
                            return Cells.create(filtered.column(),
                                                filtered.isCounterCell(),
                                                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                SimpleLivenessInfo.forDeletion(info.timestamp(), info.localDeletionTime() - info.ttl()),
                                                filtered.path());
                        }
                        else
                        {
                            return filtered;
                        }
                    }
                };
            }
        };
    }

    /**
     * Wraps the provided iterator so it logs the returned atoms for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredRowIterator loggingIterator(UnfilteredRowIterator iterator, final String id, final boolean fullDetails)
    {
        CFMetaData metadata = iterator.metadata();
        logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}, deletion={}",
                    id,
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                    iterator.isReverseOrder(),
                    iterator.partitionLevelDeletion().markedForDeleteAt());

        return new WrappingUnfilteredRowIterator(iterator)
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
            public Unfiltered next()
            {
                Unfiltered next = super.next();
                if (next.kind() == Unfiltered.Kind.ROW)
                    logger.info("[{}] {}", id, ((Row)next).toString(metadata(), fullDetails));
                else
                    logger.info("[{}] {}", id, ((RangeTombstoneMarker)next).toString(metadata()));
                return next;
            }
        };
    }

    /**
     * A wrapper over MergeIterator to implement the UnfilteredRowIterator interface.
     */
    private static class UnfilteredRowMergeIterator extends AbstractUnfilteredRowIterator
    {
        private final IMergeIterator<Unfiltered, Unfiltered> mergeIterator;
        private final MergeListener listener;

        private UnfilteredRowMergeIterator(CFMetaData metadata,
                                           List<UnfilteredRowIterator> iterators,
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
                  mergeStats(iterators));

            this.listener = listener;
            this.mergeIterator = MergeIterator.get(iterators,
                                                   reversed ? metadata.comparator.reversed() : metadata.comparator,
                                                   new MergeReducer(metadata, iterators.size(), reversed, nowInSec));
        }

        private static UnfilteredRowMergeIterator create(List<UnfilteredRowIterator> iterators, int nowInSec, MergeListener listener)
        {
            try
            {
                checkForInvalidInput(iterators);
                return new UnfilteredRowMergeIterator(iterators.get(0).metadata(),
                                                      iterators,
                                                      collectColumns(iterators),
                                                      collectPartitionLevelDeletion(iterators, listener),
                                                      nowInSec,
                                                      iterators.get(0).isReverseOrder(),
                                                      listener);
            }
            catch (RuntimeException | Error e)
            {
                try
                {
                    FBUtilities.closeAll(iterators);
                }
                catch (Exception suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }

        @SuppressWarnings("resource") // We're not really creating any resource here
        private static void checkForInvalidInput(List<UnfilteredRowIterator> iterators)
        {
            if (iterators.isEmpty())
                return;

            UnfilteredRowIterator first = iterators.get(0);
            for (int i = 1; i < iterators.size(); i++)
            {
                UnfilteredRowIterator iter = iterators.get(i);
                assert first.metadata().cfId.equals(iter.metadata().cfId);
                assert first.partitionKey().equals(iter.partitionKey());
                assert first.isReverseOrder() == iter.isReverseOrder();
            }
        }

        @SuppressWarnings("resource") // We're not really creating any resource here
        private static DeletionTime collectPartitionLevelDeletion(List<UnfilteredRowIterator> iterators, MergeListener listener)
        {
            DeletionTime[] versions = listener == null ? null : new DeletionTime[iterators.size()];

            DeletionTime delTime = DeletionTime.LIVE;
            for (int i = 0; i < iterators.size(); i++)
            {
                UnfilteredRowIterator iter = iterators.get(i);
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
                                           List<UnfilteredRowIterator> iterators,
                                           Columns columns,
                                           int nowInSec,
                                           MergeListener listener,
                                           DeletionTime partitionDeletion)
        {
            if (columns.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            Row.Merger merger = Row.Merger.createStatic(metadata, iterators.size(), nowInSec, columns, listener);
            for (int i = 0; i < iterators.size(); i++)
                merger.add(i, iterators.get(i).staticRow());

            // Note that we should call 'takeAlias' on the result in theory, but we know that we
            // won't reuse the merger and so that it's ok not to.
            Row merged = merger.merge(partitionDeletion);
            return merged == null ? Rows.EMPTY_STATIC_ROW : merged;
        }

        private static PartitionColumns collectColumns(List<UnfilteredRowIterator> iterators)
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

        private static RowStats mergeStats(List<UnfilteredRowIterator> iterators)
        {
            RowStats stats = RowStats.NO_STATS;
            for (UnfilteredRowIterator iter : iterators)
                stats = stats.mergeWith(iter.stats());
            return stats;
        }

        protected Unfiltered computeNext()
        {
            while (mergeIterator.hasNext())
            {
                Unfiltered merged = mergeIterator.next();
                if (merged != null)
                    return merged;
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
        private class MergeReducer extends MergeIterator.Reducer<Unfiltered, Unfiltered>
        {
            private Unfiltered.Kind nextKind;

            private final Row.Merger rowMerger;
            private final RangeTombstoneMarker.Merger markerMerger;

            private MergeReducer(CFMetaData metadata, int size, boolean reversed, int nowInSec)
            {
                this.rowMerger = Row.Merger.createRegular(metadata, size, nowInSec, columns().regulars, listener);
                this.markerMerger = new RangeTombstoneMarker.Merger(metadata, size, partitionLevelDeletion(), reversed, listener);
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return listener == null;
            }

            public void reduce(int idx, Unfiltered current)
            {
                nextKind = current.kind();
                if (nextKind == Unfiltered.Kind.ROW)
                    rowMerger.add(idx, (Row)current);
                else
                    markerMerger.add(idx, (RangeTombstoneMarker)current);
            }

            protected Unfiltered getReduced()
            {
                return nextKind == Unfiltered.Kind.ROW
                     ? rowMerger.merge(markerMerger.activeDeletion())
                     : markerMerger.merge();
            }

            protected void onKeyChange()
            {
                if (nextKind == Unfiltered.Kind.ROW)
                    rowMerger.clear();
                else
                    markerMerger.clear();
            }
        }
    }

    private static class FilteringIterator extends AbstractIterator<Row> implements RowIterator
    {
        private final UnfilteredRowIterator iter;
        private final int nowInSec;
        private final TombstoneFilteringRow filter;

        public FilteringIterator(UnfilteredRowIterator iter, int nowInSec)
        {
            this.iter = iter;
            this.nowInSec = nowInSec;
            this.filter = new TombstoneFilteringRow(nowInSec);
        }

        public CFMetaData metadata()
        {
            return iter.metadata();
        }

        public boolean isReverseOrder()
        {
            return iter.isReverseOrder();
        }

        public PartitionColumns columns()
        {
            return iter.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iter.partitionKey();
        }

        public Row staticRow()
        {
            Row row = iter.staticRow();
            return row.isEmpty() ? row : new TombstoneFilteringRow(nowInSec).setTo(row);
        }

        protected Row computeNext()
        {
            while (iter.hasNext())
            {
                Unfiltered next = iter.next();
                if (next.kind() != Unfiltered.Kind.ROW)
                    continue;

                Row row = filter.setTo((Row)next);
                if (!row.isEmpty())
                    return row;
            }
            return endOfData();
        }

        public void close()
        {
            iter.close();
        }
    }
}
