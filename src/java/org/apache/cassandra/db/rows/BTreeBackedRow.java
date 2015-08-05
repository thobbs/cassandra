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
import java.util.function.Predicate;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Immutable implementation of a Row object.
 */
public class BTreeBackedRow extends AbstractRow
{
    private static final ColumnData[] NO_DATA = new ColumnData[0];

    private static final long EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));

    private final Clustering clustering;
    private final Columns columns;
    private final LivenessInfo primaryKeyLivenessInfo;
    private final DeletionTime deletion;

    // The data for each columns present in this row in column sorted order.
    private final Object[] btree;

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    private final int minLocalDeletionTime;

    private BTreeBackedRow(Clustering clustering, Columns columns, LivenessInfo primaryKeyLivenessInfo, DeletionTime deletion, Object[] btree, int minLocalDeletionTime)
    {
        this.clustering = clustering;
        this.columns = columns;
        this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
        this.deletion = deletion;
        this.btree = btree;
        this.minLocalDeletionTime = minLocalDeletionTime;
    }

    // Note that it's often easier/safer to use the sortedBuilder/unsortedBuilder or one of the static creation method below. Only directly useful in a small amount of cases.
    public static BTreeBackedRow create(Clustering clustering, Columns columns, LivenessInfo primaryKeyLivenessInfo, DeletionTime deletion, Object[] btree)
    {
        int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion));
        if (minDeletionTime != Integer.MIN_VALUE)
        {
            for (ColumnData cd : BTree.<ColumnData>iterable(btree))
                minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cd));
        }

        return new BTreeBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
    }

    public static BTreeBackedRow emptyRow(Clustering clustering)
    {
        return new BTreeBackedRow(clustering, Columns.NONE, LivenessInfo.EMPTY, DeletionTime.LIVE, BTree.empty(), Integer.MAX_VALUE);
    }

    public static BTreeBackedRow singleCellRow(Clustering clustering, Cell cell)
    {
        if (cell.column().isSimple())
            return new BTreeBackedRow(clustering, Columns.of(cell.column()), LivenessInfo.EMPTY, DeletionTime.LIVE, BTree.singleton(cell), minDeletionTime(cell));

        ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell[]{ cell }, DeletionTime.LIVE);
        return new BTreeBackedRow(clustering, Columns.of(cell.column()), LivenessInfo.EMPTY, DeletionTime.LIVE, BTree.singleton(complexData), minDeletionTime(cell));
    }

    public static BTreeBackedRow emptyDeletedRow(Clustering clustering, DeletionTime deletion)
    {
        assert !deletion.isLive();
        return new BTreeBackedRow(clustering, Columns.NONE, LivenessInfo.EMPTY, deletion, BTree.empty(), Integer.MIN_VALUE);
    }

    public static BTreeBackedRow noCellLiveRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        return new BTreeBackedRow(clustering, Columns.NONE, primaryKeyLivenessInfo, DeletionTime.LIVE, BTree.empty(), minDeletionTime(primaryKeyLivenessInfo));
    }

    private static int minDeletionTime(Cell cell)
    {
        return cell.isTombstone() ? Integer.MIN_VALUE : cell.localDeletionTime();
    }

    private static int minDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Integer.MAX_VALUE;
    }

    private static int minDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }

    private static int minDeletionTime(ComplexColumnData cd)
    {
        int min = minDeletionTime(cd.complexDeletion());
        for (Cell cell : cd)
        {
            min = Math.min(min, minDeletionTime(cell));
            if (min == Integer.MIN_VALUE)
                break;
        }
        return min;
    }

    private static int minDeletionTime(ColumnData cd)
    {
        return cd.column().isSimple() ? minDeletionTime((Cell) cd) : minDeletionTime((ComplexColumnData)cd);
    }

    private static int minDeletionTime(Object[] btree, LivenessInfo info, DeletionTime rowDeletion)
    {
        int min = Math.min(minDeletionTime(info), minDeletionTime(rowDeletion));
        for (ColumnData cd : BTree.<ColumnData>iterable(btree))
        {
            min = Math.min(min, minDeletionTime(cd));
            if (min == Integer.MIN_VALUE)
                break;
        }
        return min;
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public Columns columns()
    {
        return columns;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return primaryKeyLivenessInfo;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        return (Cell) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, c);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();
        ComplexColumnData cd = getComplexColumnData(c);
        if (cd == null)
            return null;
        return cd.getCell(path);
    }

    public ComplexColumnData getComplexColumnData(ColumnDefinition c)
    {
        assert c.isComplex();
        return (ComplexColumnData) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, c);
    }

    public Iterator<ColumnData> iterator()
    {
        return searchIterator();
    }

    public Iterable<Cell> cells()
    {
        return CellIterator::new;
    }

    public BTreeSearchIterator<ColumnDefinition, ColumnData> searchIterator()
    {
        return BTree.slice(btree, ColumnDefinition.asymmetricColumnDataComparator, BTree.Dir.ASC);
    }

    public Row filter(ColumnFilter filter, CFMetaData metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, CFMetaData metadata)
    {
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = metadata.getDroppedColumns();

        if (filter.includesAllColumns() && (activeDeletion.isLive() || deletion.supersedes(activeDeletion)) && droppedColumns.isEmpty())
            return this;

        boolean mayHaveShadowed = activeDeletion.supersedes(deletion);

        LivenessInfo newInfo = primaryKeyLivenessInfo;
        DeletionTime newDeletion = deletion;
        if (mayHaveShadowed)
        {
            if (activeDeletion.deletes(newInfo.timestamp()))
                newInfo = LivenessInfo.EMPTY;
            // note that mayHaveShadowed means the activeDeletion shadows the row deletion. So if don't have setActiveDeletionToRow,
            // the row deletion is shadowed and we shouldn't return it.
            newDeletion = setActiveDeletionToRow ? activeDeletion : DeletionTime.LIVE;
        }

        Columns columns = filter.fetchedColumns().columns(isStatic());
        Predicate<ColumnDefinition> inclusionTester = columns.inOrderInclusionTester();
        return transformAndFilter(newInfo, newDeletion, (cd) -> {

            ColumnDefinition column = cd.column();
            if (!inclusionTester.test(column))
                return null;

            CFMetaData.DroppedColumn dropped = droppedColumns.get(column.name.bytes);
            if (column.isComplex())
                return ((ComplexColumnData)cd).filter(filter, mayHaveShadowed ? activeDeletion : DeletionTime.LIVE, dropped);

            Cell cell = (Cell)cd;
            return (dropped == null || cell.timestamp() > dropped.droppedTime) && !(mayHaveShadowed && activeDeletion.deletes(cell))
                   ? cell : null;
        });
    }

    public boolean hasComplexDeletion()
    {
        // We start by the end cause we know complex columns sort before simple ones
        for (ColumnData cd : BTree.<ColumnData>iterable(btree, BTree.Dir.DESC))
        {
            if (cd.column().isSimple())
                return false;

            if (!((ComplexColumnData)cd).complexDeletion().isLive())
                return true;
        }
        return false;
    }

    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> cd.column().cellValueType().isCounter()
                                                                            ? cd.markCounterLocalToBeCleared()
                                                                            : cd);
    }

    public boolean hasDeletion(int nowInSec)
    {
        return nowInSec >= minLocalDeletionTime;
    }

    /**
     * Returns a copy of the row where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     *
     * This exists for the Paxos path, see {@link PartitionUpdate#updateAllTimestamp} for additional details.
     */
    public Row updateAllTimestamp(long newTimestamp)
    {
        LivenessInfo newInfo = primaryKeyLivenessInfo.isEmpty() ? primaryKeyLivenessInfo : primaryKeyLivenessInfo.withUpdatedTimestamp(newTimestamp);
        DeletionTime newDeletion = deletion.isLive() ? deletion : new DeletionTime(newTimestamp - 1, deletion.localDeletionTime());

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.updateAllTimestamp(newTimestamp));
    }

    public Row purge(DeletionPurger purger, int nowInSec)
    {
        if (!hasDeletion(nowInSec))
            return this;

        LivenessInfo newInfo = purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo;
        DeletionTime newDeletion = purger.shouldPurge(deletion) ? DeletionTime.LIVE : deletion;

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.purge(purger, nowInSec));
    }

    private Row transformAndFilter(LivenessInfo info, DeletionTime deletion, Function<ColumnData, ColumnData> function)
    {
        Object[] transformed = BTree.transformAndFilter(btree, function);

        if (btree == transformed && info == this.primaryKeyLivenessInfo && deletion == this.deletion)
            return this;

        if (info.isEmpty() && deletion.isLive() && BTree.isEmpty(transformed))
            return null;

        int minDeletionTime = minDeletionTime(transformed, info, deletion);
        return new BTreeBackedRow(clustering, columns, info, deletion, transformed, minDeletionTime);
    }

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo.dataSize()
                     + deletion.dataSize();

        for (ColumnData cd : this)
            dataSize += cd.dataSize();
        return dataSize;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE
                      + clustering.unsharedHeapSizeExcludingData()
                      + BTree.sizeOfStructureOnHeap(btree);

        for (ColumnData cd : this)
            heapSize += cd.unsharedHeapSizeExcludingData();
        return heapSize;
    }

    public static Row.Builder sortedBuilder(Columns columns)
    {
        return new Builder(columns, true);
    }

    public static Row.Builder unsortedBuilder(Columns columns, int nowInSec)
    {
        return new Builder(columns, false, nowInSec);
    }

    // This is only used by PartitionUpdate.CounterMark but other uses should be avoided as much as possible as it breaks our general
    // assumption that Row objects are immutable. This method should go away post-#6506 in particular.
    // This method is in particular not exposed by the Row API on purpose.
    // This method also *assumes* that the cell we're setting already exists.
    public void setValue(ColumnDefinition column, CellPath path, ByteBuffer value)
    {
        ColumnData current = (ColumnData) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, column);
        if (column.isSimple())
            BTree.replaceInSitu(btree, ColumnData.comparator, current, ((Cell) current).withUpdatedValue(value));
        else
            ((ComplexColumnData) current).setValue(path, value);
    }

    public Iterable<Cell> cellsInLegacyOrder(CFMetaData metadata)
    {
        return () -> new CellInLegacyOrderIterator(metadata);
    }

    private class CellIterator extends AbstractIterator<Cell>
    {
        private Iterator<ColumnData> columnData = iterator();
        private Iterator<Cell> complexCells;

        protected Cell computeNext()
        {
            while (true)
            {
                if (complexCells != null)
                {
                    if (complexCells.hasNext())
                        return complexCells.next();

                    complexCells = null;
                }

                if (!columnData.hasNext())
                    return endOfData();

                ColumnData cd = columnData.next();
                if (cd.column().isComplex())
                    complexCells = ((ComplexColumnData)cd).iterator();
                else
                    return (Cell)cd;
            }
        }
    }

    private class CellInLegacyOrderIterator extends AbstractIterator<Cell>
    {
        private final AbstractType<?> comparator;
        private final int firstComplexIdx;
        private int simpleIdx;
        private int complexIdx;
        private Iterator<Cell> complexCells;
        private final Object[] data;

        private CellInLegacyOrderIterator(CFMetaData metadata)
        {
            this.comparator = metadata.getColumnDefinitionNameComparator(isStatic() ? ColumnDefinition.Kind.STATIC : ColumnDefinition.Kind.REGULAR);

            // copy btree into array for simple separate iteration of simple and complex columns
            this.data = new Object[BTree.size(btree)];
            BTree.toArray(btree, data, 0);

            int idx = Iterators.indexOf(Iterators.forArray(data), cd -> cd instanceof ComplexColumnData);
            this.firstComplexIdx = idx < 0 ? data.length : idx;
            this.complexIdx = firstComplexIdx;
        }

        protected Cell computeNext()
        {
            while (true)
            {
                if (complexCells != null)
                {
                    if (complexCells.hasNext())
                        return complexCells.next();

                    complexCells = null;
                }

                if (simpleIdx >= firstComplexIdx)
                {
                    if (complexIdx >= data.length)
                        return endOfData();

                    complexCells = ((ComplexColumnData)data[complexIdx++]).iterator();
                }
                else
                {
                    if (complexIdx >= data.length)
                        return (Cell)data[simpleIdx++];

                    if (comparator.compare(((Cell) data[simpleIdx]).column().name.bytes, ((Cell) data[complexIdx]).column().name.bytes) < 0)
                        return (Cell)data[simpleIdx++];
                    else
                        complexCells = ((ComplexColumnData)data[complexIdx++]).iterator();
                }
            }
        }
    }

    public static class Builder implements Row.Builder
    {
        // a simple marker class that will sort to the beginning of a run of complex cells to store the deletion time
        private static class ComplexColumnDeletion extends BufferCell
        {
            public ComplexColumnDeletion(ColumnDefinition column, DeletionTime deletionTime)
            {
                super(column, deletionTime.markedForDeleteAt(), 0, deletionTime.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.BOTTOM);
            }
        }

        // converts a run of Cell with equal column into a ColumnData
        private static class CellResolver implements BTree.Builder.Resolver
        {
            final int nowInSec;
            private CellResolver(int nowInSec)
            {
                this.nowInSec = nowInSec;
            }

            public ColumnData resolve(Object[] cells, int lb, int ub)
            {
                Cell cell = (Cell) cells[lb];
                ColumnDefinition column = cell.column;
                if (cell.column.isSimple())
                {
                    assert lb + 1 == ub || nowInSec != Integer.MIN_VALUE;
                    while (++lb < ub)
                        cell = Cells.reconcile(cell, (Cell) cells[lb], nowInSec);
                    return cell;
                }

                // TODO: relax this in the case our outer provider is sorted (want to delay until remaining changes are
                // bedded in, as less important; galloping makes it pretty cheap anyway)
                Arrays.sort(cells, lb, ub, (Comparator<Object>) column.cellComparator());
                cell = (Cell) cells[lb];
                DeletionTime deletion = DeletionTime.LIVE;
                if (cell instanceof ComplexColumnDeletion)
                {
                    // TODO: do we need to be robust to multiple of these being provided?
                    deletion = new DeletionTime(cell.timestamp(), cell.localDeletionTime());
                    lb++;
                }

                List<Object> buildFrom = Arrays.asList(cells).subList(lb, ub);
                Object[] btree = BTree.build(buildFrom, UpdateFunction.noOp());
                return new ComplexColumnData(column, btree, deletion);
            }

        };
        protected final Columns columns;

        protected Clustering clustering;
        protected LivenessInfo primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        protected DeletionTime deletion = DeletionTime.LIVE;

        private final boolean isSorted;
        private final BTree.Builder<Cell> cells;
        private final CellResolver resolver;
        private boolean hasComplex = false;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(Columns columns, boolean isSorted)
        {
            this(columns, isSorted, Integer.MIN_VALUE);
        }

        protected Builder(Columns columns, boolean isSorted, int nowInSecs)
        {
            this.columns = columns;
            this.cells = BTree.builder(ColumnData.comparator);
            resolver = new CellResolver(nowInSecs);
            this.isSorted = isSorted;
            this.cells.auto(false);
        }

        public boolean isSorted()
        {
            return isSorted;
        }

        public void newRow(Clustering clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        protected void reset()
        {
            this.clustering = null;
            this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
            this.deletion = DeletionTime.LIVE;
            this.cells.reuse();
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            this.primaryKeyLivenessInfo = info;
        }

        public void addRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        public void addCell(Cell cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;
            cells.add(cell);
            hasComplex |= cell.column.isComplex();
        }

        public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion)
        {
            cells.add(new ComplexColumnDeletion(column, complexDeletion));
            hasComplex = true;
        }

        public Row build()
        {
            if (!isSorted)
                cells.sort();
            // we can avoid resolving if we're sorted and have no complex values
            // (because we'll only have unique simple cells, which are already in their final condition)
            if (!isSorted | hasComplex)
                cells.resolve(resolver);
            Object[] btree = cells.build();
            int minDeletionTime = minDeletionTime(btree, primaryKeyLivenessInfo, deletion);
            Row row = new BTreeBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
            reset();
            return row;
        }

    }
}
