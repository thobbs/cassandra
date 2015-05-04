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
import java.io.IOError;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Functions to deal with the old format.
 */
public abstract class LegacyLayout
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyLayout.class);

    public final static int MAX_CELL_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    private final static int DELETION_MASK        = 0x01;
    private final static int EXPIRATION_MASK      = 0x02;
    private final static int COUNTER_MASK         = 0x04;
    private final static int COUNTER_UPDATE_MASK  = 0x08;
    private final static int RANGE_TOMBSTONE_MASK = 0x10;

    private LegacyLayout() {}

    public static AbstractType<?> makeLegacyComparator(CFMetaData metadata)
    {
        ClusteringComparator comparator = metadata.comparator;
        if (!metadata.isCompound())
        {
            assert comparator.size() == 1;
            return comparator.subtype(0);
        }

        boolean hasCollections = metadata.hasCollectionColumns();
        List<AbstractType<?>> types = new ArrayList<>(comparator.size() + (metadata.isDense() ? 0 : 1) + (hasCollections ? 1 : 0));

        types.addAll(comparator.subtypes());

        if (!metadata.isDense())
        {
            types.add(UTF8Type.instance);
            if (hasCollections)
            {
                Map<ByteBuffer, CollectionType> defined = new HashMap<>();
                for (ColumnDefinition def : metadata.partitionColumns())
                {
                    if (def.type instanceof CollectionType && def.type.isMultiCell())
                        defined.put(def.name.bytes, (CollectionType)def.type);
                }
                types.add(ColumnToCollectionType.getInstance(defined));
            }
        }
        return CompositeType.getInstance(types);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer cellname)
    throws UnknownColumnException
    {
        assert cellname != null;
        if (metadata.isSuper())
        {
            assert superColumnName != null;
            return decodeForSuperColumn(metadata, new SimpleClustering(superColumnName), cellname);
        }

        assert superColumnName == null;
        return decodeCellName(metadata, cellname);
    }

    private static LegacyCellName decodeForSuperColumn(CFMetaData metadata, Clustering clustering, ByteBuffer subcol)
    {
        ColumnDefinition def = metadata.getColumnDefinition(subcol);
        if (def != null)
        {
            // it's a statically defined subcolumn
            return new LegacyCellName(clustering, def, null);
        }

        def = metadata.compactValueColumn();
        assert def != null && def.type instanceof MapType;
        return new LegacyCellName(clustering, def, subcol);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname) throws UnknownColumnException
    {
        return decodeCellName(metadata, cellname, false);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname, boolean readAllAsDynamic) throws UnknownColumnException
    {
        Clustering clustering = decodeClustering(metadata, cellname);

        if (metadata.isSuper())
            return decodeForSuperColumn(metadata, clustering, CompositeType.extractComponent(cellname, 1));

        if (metadata.isDense() || (metadata.isCompactTable() && readAllAsDynamic))
            return new LegacyCellName(clustering, metadata.compactValueColumn(), null);

        ByteBuffer column = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size()) : cellname;
        if (column == null)
            throw new IllegalArgumentException("No column name component found in cell name");

        // Row marker, this is ok
        if (!column.hasRemaining())
            return new LegacyCellName(clustering, null, null);

        ColumnDefinition def = metadata.getColumnDefinition(column);
        if (def == null)
        {
            // If it's a compact table, it means the column is in fact a "dynamic" one
            if (metadata.isCompactTable())
                return new LegacyCellName(new SimpleClustering(column), metadata.compactValueColumn(), null);

            throw new UnknownColumnException(metadata, column);
        }

        ByteBuffer collectionElement = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size() + 1) : null;

        // Note that because static compact columns are translated to static defs in the new world order, we need to force a static
        // clustering if the definition is static (as it might not be in this case).
        return new LegacyCellName(def.isStatic() ? Clustering.STATIC_CLUSTERING : clustering, def, collectionElement);
    }

    public static LegacyBound decodeBound(CFMetaData metadata, ByteBuffer bound, boolean isStart)
    {
        if (!bound.hasRemaining())
            return isStart ? LegacyBound.BOTTOM : LegacyBound.TOP;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(bound)
                                    : Collections.singletonList(bound);

        // Either it's a prefix of the clustering, or it's the bound of a collection range tombstone (and thus has
        // the collection column name)
        assert components.size() <= metadata.comparator.size() || (!metadata.isCompactTable() && components.size() == metadata.comparator.size() + 1);

        List<ByteBuffer> prefix = components.size() <= metadata.comparator.size() ? components : components.subList(0, metadata.comparator.size());
        Slice.Bound sb = Slice.Bound.create(isStart ? Slice.Bound.Kind.INCL_START_BOUND : Slice.Bound.Kind.INCL_END_BOUND,
                                            prefix.toArray(new ByteBuffer[prefix.size()]));

        ColumnDefinition collectionName = components.size() == metadata.comparator.size() + 1
                                        ? metadata.getColumnDefinition(components.get(metadata.comparator.size()))
                                        : null;
        return new LegacyBound(sb, metadata.isCompound() && CompositeType.isStaticName(bound), collectionName);
    }

    public static ByteBuffer encodeCellName(CFMetaData metadata, Clustering clustering, ByteBuffer columnName, ByteBuffer collectionElement)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!metadata.isCompound())
        {
            if (isStatic)
                return columnName;

            assert clustering.size() == 1;
            return clustering.get(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (metadata.isDense() ? 0 : 1) + (collectionElement == null ? 0 : 1);
        ByteBuffer[] values = new ByteBuffer[size];
        for (int i = 0; i < clusteringSize; i++)
        {
            if (isStatic)
            {
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            ByteBuffer v = clustering.get(i);
            // we can have null (only for dense compound tables for backward compatibility reasons) but that
            // means we're done and should stop there as far as building the composite is concerned.
            if (v == null)
                return CompositeType.build(Arrays.copyOfRange(values, 0, i));

            values[i] = v;
        }

        if (!metadata.isDense())
            values[clusteringSize] = columnName;
        if (collectionElement != null)
            values[clusteringSize + 1] = collectionElement;

        return CompositeType.build(isStatic, values);
    }

    public static Clustering decodeClustering(CFMetaData metadata, ByteBuffer value)
    {
        int csize = metadata.comparator.size();
        if (csize == 0)
            return Clustering.EMPTY;

        if (metadata.isCompound() && CompositeType.isStaticName(value))
            return Clustering.STATIC_CLUSTERING;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(value)
                                    : Collections.singletonList(value);

        return new SimpleClustering(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
    }

    public static ByteBuffer encodeClustering(CFMetaData metadata, Clustering clustering)
    {
        if (!metadata.isCompound())
        {
            assert clustering.size() == 1;
            return clustering.get(0);
        }

        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return CompositeType.build(values);
    }

    // For serializing to old wire format
    public static Pair<DeletionInfo, Iterator<LegacyCell>> fromAtomIterator(AtomIterator iterator)
    {
        // we need to extract the range tombstone so materialize the partition. Since this is
        // used for the on-wire format, this is not worst than it used to be.
        final ArrayBackedPartition partition = ArrayBackedPartition.create(iterator);
        DeletionInfo info = partition.deletionInfo();
        Iterator<LegacyCell> cells = fromRowIterator(partition.metadata(), partition.iterator(), partition.staticRow());
        return Pair.create(info, cells);
    }

    // For thrift sake
    public static AtomIterator toAtomIterator(CFMetaData metadata,
                                              DecoratedKey key,
                                              DeletionInfo delInfo,
                                              Iterator<LegacyCell> cells,
                                              int nowInSec)
    {
        return toAtomIterator(metadata, key, LegacyDeletionInfo.from(delInfo), cells, false, nowInSec);
    }

    // For deserializing old wire format
    public static AtomIterator onWireCellstoAtomIterator(CFMetaData metadata,
                                                         DecoratedKey key,
                                                         LegacyDeletionInfo delInfo,
                                                         Iterator<LegacyCell> cells,
                                                         boolean reversed,
                                                         int nowInSec)
    {
        // If the table is a static compact, the "column_metadata" are now internally encoded as
        // static. This has already been recognized by decodeCellName, but it means the cells
        // provided are not in the expected order (the "static" cells are not necessarily at the front).
        // So sort them to make sure toAtomIterator works as expected.
        // Further, if the query is reversed, then the on-wire format still has cells in non-reversed
        // order, but we need to have them reverse in the final AtomIterator. So reverse them.
        if (metadata.isStaticCompactTable() || reversed)
        {
            List<LegacyCell> l = new ArrayList<>();
            Iterators.addAll(l, cells);
            Collections.sort(l, legacyCellComparator(metadata, reversed));
            cells = l.iterator();
        }

        return toAtomIterator(metadata, key, delInfo, cells, reversed, nowInSec);
    }

    private static AtomIterator toAtomIterator(CFMetaData metadata,
                                               DecoratedKey key,
                                               LegacyDeletionInfo delInfo,
                                               Iterator<LegacyCell> cells,
                                               boolean reversed,
                                               int nowInSec)
    {
        // Check if we have some static
        PeekingIterator<LegacyCell> iter = Iterators.peekingIterator(cells);
        Row staticRow = iter.hasNext() && iter.peek().name.clustering == Clustering.STATIC_CLUSTERING
                      ? getNextRow(CellGrouper.staticGrouper(metadata, nowInSec), iter)
                      : Rows.EMPTY_STATIC_ROW;

        Iterator<Row> rows = convertToRows(new CellGrouper(metadata, nowInSec), iter, delInfo);
        Iterator<RangeTombstone> ranges = delInfo.deletionInfo.rangeIterator(reversed);
        final Iterator<Atom> atoms = new RowAndTombstoneMergeIterator(metadata.comparator, reversed)
                                     .setTo(rows, ranges);

        return new AbstractAtomIterator(metadata,
                                        key,
                                        delInfo.deletionInfo.getPartitionDeletion(),
                                        metadata.partitionColumns(),
                                        staticRow,
                                        reversed,
                                        AtomStats.NO_STATS,
                                        nowInSec)
        {
            protected Atom computeNext()
            {
                return atoms.hasNext() ? atoms.next() : endOfData();
            }
        };
    }

    public static Row extractStaticColumns(CFMetaData metadata, DataInput in, Columns statics, int nowInSec) throws IOException
    {
        assert !statics.isEmpty();
        assert metadata.isCompactTable();

        if (metadata.isSuper())
            // TODO: there is in practice nothing to do here, but we need to handle the column_metadata for super columns somewhere else
            throw new UnsupportedOperationException();

        Set<ByteBuffer> columnsToFetch = new HashSet<>(statics.columnCount());
        for (ColumnDefinition column : statics)
            columnsToFetch.add(column.name.bytes);

        StaticRow.Builder builder = StaticRow.builder(statics, nowInSec, metadata.isCounter());

        boolean foundOne = false;
        LegacyAtom atom;
        while ((atom = readLegacyAtom(metadata, in, false)) != null)
        {
            if (atom.isCell())
            {
                LegacyCell cell = atom.asCell();
                if (!columnsToFetch.contains(cell.name.encode(metadata)))
                    continue;

                foundOne = true;
                builder.writeCell(cell.name.column, cell.isCounter(), cell.value, livenessInfo(metadata, cell), null);
            }
            else
            {
                LegacyRangeTombstone tombstone = atom.asRangeTombstone();
                // TODO: we need to track tombstones and potentially ignore cells that are
                // shadowed (or even better, replace them by tombstones).
                throw new UnsupportedOperationException();
            }
        }

        return foundOne ? builder.build() : Rows.EMPTY_STATIC_ROW;
    }

    private static Row getNextRow(CellGrouper grouper, PeekingIterator<? extends LegacyAtom> cells)
    {
        if (!cells.hasNext())
            return null;

        grouper.reset();
        while (cells.hasNext() && grouper.addAtom(cells.peek()))
        {
            // We've added the cell already in the grouper, so just skip it
            cells.next();
        }
        return grouper.getRow();
    }

    private static Iterator<LegacyAtom> asLegacyAtomIterator(Iterator<? extends LegacyAtom> iter)
    {
        return (Iterator<LegacyAtom>)iter;
    }

    private static Iterator<Row> convertToRows(final CellGrouper grouper, final Iterator<LegacyCell> cells, final LegacyDeletionInfo delInfo)
    {
        MergeIterator.Reducer<LegacyAtom, LegacyAtom> reducer = new MergeIterator.Reducer<LegacyAtom, LegacyAtom>()
        {
            private LegacyAtom atom;

            public void reduce(int idx, LegacyAtom current)
            {
                // We're merging cell with range tombstones, so we should always only have a single atom to reduce.
                assert atom == null;
                atom = (LegacyAtom)current;
            }

            protected LegacyAtom getReduced()
            {
                return atom;
            }

            protected void onKeyChange()
            {
                atom = null;
            }
        };
        List<Iterator<LegacyAtom>> iterators = Arrays.asList(asLegacyAtomIterator(cells), asLegacyAtomIterator(delInfo.inRowRangeTombstones()));
        Iterator<LegacyAtom> merged = MergeIterator.get(iterators, grouper.metadata.comparator, reducer);
        final PeekingIterator<LegacyAtom> atoms = Iterators.peekingIterator(merged);

        return new AbstractIterator<Row>()
        {
            protected Row computeNext()
            {
                if (!atoms.hasNext())
                    return endOfData();

                return getNextRow(grouper, atoms);
            }
        };
    }

    public static Iterator<LegacyCell> fromRowIterator(final RowIterator iterator)
    {
        return fromRowIterator(iterator.metadata(), iterator, iterator.staticRow());
    }

    public static Iterator<LegacyCell> fromRowIterator(final CFMetaData metadata, final Iterator<Row> iterator, final Row staticRow)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private Iterator<LegacyCell> currentRow = staticRow.isEmpty()
                                                    ? Collections.<LegacyLayout.LegacyCell>emptyIterator()
                                                    : fromRow(metadata, staticRow);

            protected LegacyCell computeNext()
            {
                if (currentRow.hasNext())
                    return currentRow.next();

                if (!iterator.hasNext())
                    return endOfData();

                currentRow = fromRow(metadata, iterator.next());
                return computeNext();
            }
        };
    }

    private static Iterator<LegacyCell> fromRow(final CFMetaData metadata, final Row row)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private final Iterator<Cell> cells = row.iterator();
            // we don't have (and shouldn't have) row markers for compact tables.
            private boolean hasReturnedRowMarker = metadata.isCompactTable();

            protected LegacyCell computeNext()
            {
                if (!hasReturnedRowMarker)
                {
                    hasReturnedRowMarker = true;
                    LegacyCellName cellName = new LegacyCellName(row.clustering(), null, null);
                    LivenessInfo info = row.partitionKeyLivenessInfo();
                    return new LegacyCell(LegacyCell.Kind.REGULAR, cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER, info.timestamp(), info.localDeletionTime(), info.ttl());
                }

                if (!cells.hasNext())
                    return endOfData();

                Cell cell = cells.next();
                return makeLegacyCell(metadata, row.clustering(), cell);
            }
        };
    }

    private static LegacyCell makeLegacyCell(CFMetaData metadata, Clustering clustering, Cell cell)
    {
        LegacyCell.Kind kind;
        if (cell.isCounterCell())
            kind = LegacyCell.Kind.COUNTER;
        else if (cell.isTombstone())
            kind = LegacyCell.Kind.DELETED;
        else if (cell.isExpiring())
            kind = LegacyCell.Kind.EXPIRING;
        else
            kind = LegacyCell.Kind.REGULAR;

        CellPath path = cell.path();
        assert path == null || path.size() == 1;
        LegacyCellName name = new LegacyCellName(clustering, cell.column(), path == null ? null : path.get(0));
        LivenessInfo info = cell.livenessInfo();
        return new LegacyCell(kind, name, cell.value(), info.timestamp(), info.localDeletionTime(), info.ttl());
    }

    public static RowIterator toRowIterator(final CFMetaData metadata,
                                            final DecoratedKey key,
                                            final Iterator<LegacyCell> cells,
                                            final int nowInSec)
    {
        return AtomIterators.asRowIterator(toAtomIterator(metadata, key, LegacyDeletionInfo.live(), cells, false, nowInSec));
    }

    private static LivenessInfo livenessInfo(CFMetaData metadata, LegacyCell cell)
    {
        return cell.isTombstone()
             ? SimpleLivenessInfo.forDeletion(cell.timestamp, cell.localDeletionTime)
             : SimpleLivenessInfo.forUpdate(cell.timestamp, cell.ttl, cell.localDeletionTime, metadata);
    }

    public static Comparator<LegacyCell> legacyCellComparator(CFMetaData metadata)
    {
        return legacyCellComparator(metadata, false);
    }

    public static Comparator<LegacyCell> legacyCellComparator(final CFMetaData metadata, final boolean reversed)
    {
        final Comparator<LegacyCellName> cellNameComparator = legacyCellNameComparator(metadata, reversed);
        return new Comparator<LegacyCell>()
        {
            public int compare(LegacyCell cell1, LegacyCell cell2)
            {
                LegacyCellName c1 = cell1.name;
                LegacyCellName c2 = cell2.name;

                int c = cellNameComparator.compare(c1, c2);
                if (c != 0)
                    return c;

                // The actual sorting when the cellname is equal doesn't matter, we just want to make
                // sure the cells are not considered equal.
                if (cell1.timestamp != cell2.timestamp)
                    return cell1.timestamp < cell2.timestamp ? -1 : 1;

                if (cell1.localDeletionTime != cell2.localDeletionTime)
                    return cell1.localDeletionTime < cell2.localDeletionTime ? -1 : 1;

                return cell1.value.compareTo(cell2.value);
            }
        };
    }

    public static Comparator<LegacyCellName> legacyCellNameComparator(final CFMetaData metadata, final boolean reversed)
    {
        return new Comparator<LegacyCellName>()
        {
            public int compare(LegacyCellName c1, LegacyCellName c2)
            {
                // Compare clustering first
                if (c1.clustering == Clustering.STATIC_CLUSTERING)
                {
                    if (c2.clustering != Clustering.STATIC_CLUSTERING)
                        return -1;
                }
                else if (c2.clustering == Clustering.STATIC_CLUSTERING)
                {
                    return 1;
                }
                else
                {
                    int c = metadata.comparator.compare(c1.clustering, c2.clustering);
                    if (c != 0)
                        return reversed ? -c : c;
                }

                // Note that when reversed, we only care about the clustering being reversed, so it's ok
                // not to take reversed into account below.

                // Then check the column name
                if (c1.column != c2.column)
                {
                    // A null for the column means it's a row marker
                    if (c1.column == null)
                        return c2.column == null ? 0 : -1;
                    if (c2.column == null)
                        return 1;

                    assert c1.column.isRegular() || c1.column.isStatic();
                    assert c2.column.isRegular() || c2.column.isStatic();
                    if (c1.column.kind != c2.column.kind)
                        return c1.column.isStatic() ? -1 : 1;

                    AbstractType<?> cmp = metadata.getColumnDefinitionNameComparator(c1.column.kind);
                    int c = cmp.compare(c1.column.name.bytes, c2.column.name.bytes);
                    if (c != 0)
                        return c;
                }

                assert (c1.collectionElement == null) == (c2.collectionElement == null);

                if (c1.collectionElement != null)
                {
                    AbstractType<?> colCmp = ((CollectionType)c1.column.type).nameComparator();
                    return colCmp.compare(c1.collectionElement, c2.collectionElement);
                }
                return 0;
            }
        };
    }

    public static LegacyAtom readLegacyAtom(CFMetaData metadata, DataInput in, boolean readAllAsDynamic) throws IOException
    {
        while (true)
        {
            ByteBuffer cellname = ByteBufferUtil.readWithShortLength(in);
            if (!cellname.hasRemaining())
                return null; // END_OF_ROW

            try
            {
                int b = in.readUnsignedByte();
                return (b & RANGE_TOMBSTONE_MASK) != 0
                    ? readLegacyRangeTombstoneBody(metadata, in, cellname)
                    : readLegacyCellBody(metadata, in, cellname, b, SerializationHelper.Flag.LOCAL, readAllAsDynamic);
            }
            catch (UnknownColumnException e)
            {
                // We can get there if we read a cell for a dropped column, and ff that is the case,
                // then simply ignore the cell is fine. But also not that we ignore if it's the
                // system keyspace because for those table we actually remove columns without registering
                // them in the dropped columns
                assert metadata.ksName.equals(SystemKeyspace.NAME) || metadata.getDroppedColumnDefinition(cellname) != null : e.getMessage();
            }
        }
    }

    public static LegacyCell readLegacyCell(CFMetaData metadata, DataInput in, SerializationHelper.Flag flag) throws IOException, UnknownColumnException
    {
        ByteBuffer cellname = ByteBufferUtil.readWithShortLength(in);
        int b = in.readUnsignedByte();
        return readLegacyCellBody(metadata, in, cellname, b, flag, false);
    }

    public static LegacyCell readLegacyCellBody(CFMetaData metadata, DataInput in, ByteBuffer cellname, int mask, SerializationHelper.Flag flag, boolean readAllAsDynamic)
    throws IOException, UnknownColumnException
    {
        // Note that we want to call decodeCellName only after we've deserialized other parts, since it can throw
        // and we want to throw only after having deserialized the full cell.
        if ((mask & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = in.readLong();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            if (flag == SerializationHelper.Flag.FROM_REMOTE || (flag == SerializationHelper.Flag.LOCAL && CounterContext.instance().shouldClearLocal(value)))
                value = CounterContext.instance().clearAllLocal(value);
            return new LegacyCell(LegacyCell.Kind.COUNTER, decodeCellName(metadata, cellname, readAllAsDynamic), value, ts, LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = in.readInt();
            int expiration = in.readInt();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return new LegacyCell(LegacyCell.Kind.EXPIRING, decodeCellName(metadata, cellname, readAllAsDynamic), value, ts, expiration, ttl);
        }
        else
        {
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            LegacyCellName name = decodeCellName(metadata, cellname, readAllAsDynamic);
            return (mask & COUNTER_UPDATE_MASK) != 0
                ? new LegacyCell(LegacyCell.Kind.COUNTER, name, CounterContext.instance().createLocal(ByteBufferUtil.toLong(value)), ts, LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL)
                : ((mask & DELETION_MASK) == 0
                        ? new LegacyCell(LegacyCell.Kind.REGULAR, name, value, ts, LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL)
                        : new LegacyCell(LegacyCell.Kind.DELETED, name, ByteBufferUtil.EMPTY_BYTE_BUFFER, ts, ByteBufferUtil.toInt(value), LivenessInfo.NO_TTL));
        }
    }

    public static LegacyRangeTombstone readLegacyRangeTombstone(CFMetaData metadata, DataInput in) throws IOException
    {
        ByteBuffer boundname = ByteBufferUtil.readWithShortLength(in);
        in.readUnsignedByte();
        return readLegacyRangeTombstoneBody(metadata, in, boundname);
    }

    public static LegacyRangeTombstone readLegacyRangeTombstoneBody(CFMetaData metadata, DataInput in, ByteBuffer boundname) throws IOException
    {
        LegacyBound min = decodeBound(metadata, boundname, true);
        LegacyBound max = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), false);
        DeletionTime dt = DeletionTime.serializer.deserialize(in);
        return new LegacyRangeTombstone(min, max, dt);
    }

    public static Iterator<LegacyCell> deserializeCells(final CFMetaData metadata,
                                                        final DataInput in,
                                                        final int version,
                                                        final SerializationHelper.Flag flag,
                                                        final int size)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private int i = 0;

            protected LegacyCell computeNext()
            {
                if (i >= size)
                    return endOfData();

                ++i;
                try
                {
                    return readLegacyCell(metadata, in, flag);
                }
                catch (UnknownColumnException e)
                {
                    // We can get there if we read a cell for a dropped column, and if that is the case,
                    // then simply ignore the cell is fine. But also not that we ignore if it's the
                    // system keyspace because for those table we actually remove columns without registering
                    // them in the dropped columns
                    if (metadata.ksName.equals(SystemKeyspace.NAME) || metadata.getDroppedColumnDefinition(e.columnName) != null)
                        return computeNext();
                    else
                        throw new IOError(e);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public static class CellGrouper
    {
        public final CFMetaData metadata;
        private final ReusableRow row;
        private final boolean isStatic;
        private Row.Writer writer;
        private Clustering clustering;

        private LegacyRangeTombstone rowDeletion;
        private LegacyRangeTombstone collectionDeletion;

        public CellGrouper(CFMetaData metadata, int nowInSec)
        {
            this(metadata, nowInSec, false);
        }

        private CellGrouper(CFMetaData metadata, int nowInSec, boolean isStatic)
        {
            this.metadata = metadata;
            this.isStatic = isStatic;
            this.row = isStatic ? null : new ReusableRow(metadata.clusteringColumns().size(), metadata.partitionColumns().regulars, nowInSec, metadata.isCounter());

            if (isStatic)
                this.writer = StaticRow.builder(metadata.partitionColumns().statics, nowInSec, metadata.isCounter());
        }

        public static CellGrouper staticGrouper(CFMetaData metadata, int nowInSec)
        {
            return new CellGrouper(metadata, nowInSec, true);
        }

        public void reset()
        {
            this.clustering = null;
            this.rowDeletion = null;
            this.collectionDeletion = null;

            if (!isStatic)
                this.writer = row.writer();
        }

        public boolean addAtom(LegacyAtom atom)
        {
            return atom.isCell()
                 ? addCell(atom.asCell())
                 : addRangeTombstone(atom.asRangeTombstone());
        }

        public boolean addCell(LegacyCell cell)
        {
            if (isStatic)
            {
                if (cell.name.clustering != Clustering.STATIC_CLUSTERING)
                    return false;
            }
            else if (clustering == null)
            {
                clustering = cell.name.clustering.takeAlias();
                clustering.writeTo(writer);
            }
            else if (!clustering.equals(cell.name.clustering))
            {
                return false;
            }

            // Ignore shadowed cells
            if (rowDeletion != null && rowDeletion.deletionTime.deletes(cell.timestamp))
                return true;

            if (cell.name.column == null)
            {
                // It's the row marker
                assert !cell.value.hasRemaining();
                writer.writePartitionKeyLivenessInfo(livenessInfo(metadata, cell));
            }
            else
            {
                if (collectionDeletion != null && collectionDeletion.start.collectionName.equals(cell.name.column.name.bytes) && collectionDeletion.deletionTime.deletes(cell.timestamp))
                    return true;

                CellPath path = cell.name.collectionElement == null ? null : CellPath.create(cell.name.collectionElement);
                writer.writeCell(cell.name.column, cell.isCounter(), cell.value, livenessInfo(metadata, cell), path);
            }
            return true;
        }

        public boolean addRangeTombstone(LegacyRangeTombstone tombstone)
        {
            if (tombstone.isRowDeletion(metadata))
            {
                // If we're already within a row, it can't be the same one
                if (clustering != null)
                    return false;

                clustering = tombstone.start.getAsClustering(metadata).takeAlias();
                clustering.writeTo(writer);
                writer.writeRowDeletion(tombstone.deletionTime);
                rowDeletion = tombstone;
                return true;
            }

            if (tombstone.isCollectionTombstone(metadata))
            {
                if (clustering == null)
                {
                    clustering = tombstone.start.getAsClustering(metadata).takeAlias();
                    clustering.writeTo(writer);
                }
                else if (!clustering.equals(tombstone.start.getAsClustering(metadata)))
                {
                    return false;
                }

                writer.writeComplexDeletion(tombstone.start.collectionName, tombstone.deletionTime);
                if (rowDeletion == null || tombstone.deletionTime.supersedes(rowDeletion.deletionTime))
                    collectionDeletion = tombstone;
                return true;
            }
            return false;
        }

        public Row getRow()
        {
            writer.endOfRow();
            return isStatic ? ((StaticRow.Builder)writer).build() : row;
        }
    }

    public static class LegacyCellName
    {
        public final Clustering clustering;
        public final ColumnDefinition column;
        public final ByteBuffer collectionElement;

        private LegacyCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
        {
            this.clustering = clustering;
            this.column = column;
            this.collectionElement = collectionElement;
        }

        public ByteBuffer encode(CFMetaData metadata)
        {
            return encodeCellName(metadata, clustering, column == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : column.name.bytes, collectionElement);
        }

        public ByteBuffer superColumnSubName()
        {
            assert collectionElement != null;
            return collectionElement;
        }

        public ByteBuffer superColumnName()
        {
            return clustering.get(0);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < clustering.size(); i++)
                sb.append(i > 0 ? ":" : "").append(clustering.get(i) == null ? "null" : ByteBufferUtil.bytesToHex(clustering.get(i)));
            return String.format("Cellname(clustering=%s, column=%s, collElt=%s)", sb.toString(), column == null ? "null" : column.name, collectionElement == null ? "null" : ByteBufferUtil.bytesToHex(collectionElement));
        }
    }

    public static class LegacyBound
    {
        public static final LegacyBound BOTTOM = new LegacyBound(Slice.Bound.BOTTOM, false, null);
        public static final LegacyBound TOP = new LegacyBound(Slice.Bound.TOP, false, null);

        public final Slice.Bound bound;
        public final boolean isStatic;
        public final ColumnDefinition collectionName;

        private LegacyBound(Slice.Bound bound, boolean isStatic, ColumnDefinition collectionName)
        {
            this.bound = bound;
            this.isStatic = isStatic;
            this.collectionName = collectionName;
        }

        public Clustering getAsClustering(CFMetaData metadata)
        {
            assert bound.size() == metadata.comparator.size();
            ByteBuffer[] values = new ByteBuffer[bound.size()];
            for (int i = 0; i < bound.size(); i++)
                values[i] = bound.get(i);
            return new SimpleClustering(values);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(bound.kind()).append("(");
            for (int i = 0; i < bound.size(); i++)
                sb.append(i > 0 ? ":" : "").append(bound.get(i) == null ? "null" : ByteBufferUtil.bytesToHex(bound.get(i)));
            sb.append(")");
            return String.format("Bound(%s, collection=%s)", sb.toString(), collectionName == null ? "null" : collectionName.name);
        }
    }

    public interface LegacyAtom extends Clusterable
    {
        public boolean isCell();

        public ClusteringPrefix clustering();
        public boolean isStatic();

        public LegacyCell asCell();
        public LegacyRangeTombstone asRangeTombstone();
    }

    /**
     * A legacy cell.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyCell implements LegacyAtom
    {
        public enum Kind { REGULAR, EXPIRING, DELETED, COUNTER }

        public final Kind kind;

        public final LegacyCellName name;
        public final ByteBuffer value;

        public final long timestamp;
        public final int localDeletionTime;
        public final int ttl;

        private LegacyCell(Kind kind, LegacyCellName name, ByteBuffer value, long timestamp, int localDeletionTime, int ttl)
        {
            this.kind = kind;
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
            this.localDeletionTime = localDeletionTime;
            this.ttl = ttl;
        }

        public static LegacyCell regular(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.REGULAR, decodeCellName(metadata, superColumnName, name), value, timestamp, LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL);
        }

        public static LegacyCell expiring(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp, int ttl, int nowInSec)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.EXPIRING, decodeCellName(metadata, superColumnName, name), value, timestamp, nowInSec, ttl);
        }

        public static LegacyCell tombstone(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long timestamp, int nowInSec)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.DELETED, decodeCellName(metadata, superColumnName, name), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, nowInSec, LivenessInfo.NO_TTL);
        }

        public static LegacyCell counter(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long value)
        throws UnknownColumnException
        {
            // See UpdateParameters.addCounter() for more details on this
            ByteBuffer counterValue = CounterContext.instance().createLocal(value);
            return counter(decodeCellName(metadata, superColumnName, name), counterValue);
        }

        public static LegacyCell counter(LegacyCellName name, ByteBuffer value)
        {
            return new LegacyCell(Kind.COUNTER, name, value, FBUtilities.timestampMicros(), LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_TTL);
        }

        public ClusteringPrefix clustering()
        {
            return name.clustering;
        }

        public boolean isStatic()
        {
            return name.clustering == Clustering.STATIC_CLUSTERING;
        }

        public boolean isCell()
        {
            return true;
        }

        public LegacyCell asCell()
        {
            return this;
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isCounter()
        {
            return kind == Kind.COUNTER;
        }

        public boolean isExpiring()
        {
            return kind == Kind.EXPIRING;
        }

        public boolean isTombstone()
        {
            return kind == Kind.DELETED;
        }

        public boolean isLive(int nowInSec)
        {
            if (isTombstone())
                return false;

            if (isExpiring())
                return nowInSec < localDeletionTime;

            return true;
        }

        @Override
        public String toString()
        {
            return String.format("LegacyCell(%s, name=%s, v=%s, ts=%s, ldt=%s, ttl=%s)", kind, name, ByteBufferUtil.bytesToHex(value), timestamp, localDeletionTime, ttl);
        }
    }

    /**
     * A legacy range tombstone.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyRangeTombstone implements LegacyAtom
    {
        public final LegacyBound start;
        public final LegacyBound stop;
        public final DeletionTime deletionTime;

        // Do not use directly, use create() instead.
        private LegacyRangeTombstone(LegacyBound start, LegacyBound stop, DeletionTime deletionTime)
        {
            // Because of the way RangeTombstoneList work, we can have a tombstone where only one of
            // the bound has a collectionName. That happens if we have a big tombstone A (spanning one
            // or multiple rows) and a collection tombstone B. In that case, RangeTombstoneList will
            // split this into 3 RTs: the first one from the beginning of A to the beginning of B,
            // then B, then a third one from the end of B to the end of A. To make this simpler, if
            // we detect that case we transform the 1st and 3rd tombstone so they don't end in the middle
            // of a row (which is still correct).
            if ((start.collectionName == null) != (stop.collectionName == null))
            {
                if (start.collectionName == null)
                    stop = new LegacyBound(stop.bound, stop.isStatic, null);
                else
                    start = new LegacyBound(start.bound, start.isStatic, null);
            }
            else if (!Objects.equals(start.collectionName, stop.collectionName))
            {
                // We're in the similar but slightly more complex case where on top of the big tombstone
                // A, we have 2 (or more) collection tombstones B and C within A. So we also end up with
                // a tombstone that goes between the end of B and the start of C.
                start = new LegacyBound(start.bound, start.isStatic, null);
                stop = new LegacyBound(stop.bound, stop.isStatic, null);
            }

            this.start = start;
            this.stop = stop;
            this.deletionTime = deletionTime;
        }

        public ClusteringPrefix clustering()
        {
            return start.bound;
        }

        public boolean isCell()
        {
            return false;
        }

        public boolean isStatic()
        {
            return start.isStatic;
        }

        public LegacyCell asCell()
        {
            throw new UnsupportedOperationException();
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            return this;
        }

        public boolean isCollectionTombstone(CFMetaData metadata)
        {
            return start.collectionName != null;
        }

        public boolean isRowDeletion(CFMetaData metadata)
        {
            if (start.collectionName != null
                || stop.collectionName != null
                || start.bound.size() != metadata.comparator.size()
                || stop.bound.size() != metadata.comparator.size())
                return false;

            for (int i = 0; i < start.bound.size(); i++)
                if (!Objects.equals(start.bound.get(i), stop.bound.get(i)))
                    return false;
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("RT(%s-%s, %s)", start, stop, deletionTime);
        }
    }

    public static class LegacyDeletionInfo
    {
        public static final Serializer serializer = new Serializer();

        public final DeletionInfo deletionInfo;
        private final List<LegacyRangeTombstone> inRowTombstones;

        private LegacyDeletionInfo(DeletionInfo deletionInfo, List<LegacyRangeTombstone> inRowTombstones)
        {
            this.deletionInfo = deletionInfo;
            this.inRowTombstones = inRowTombstones;
        }

        public static LegacyDeletionInfo from(DeletionInfo info)
        {
            return new LegacyDeletionInfo(info, Collections.<LegacyRangeTombstone>emptyList());
        }

        public static LegacyDeletionInfo live()
        {
            return from(DeletionInfo.live());
        }

        public Iterator<LegacyRangeTombstone> inRowRangeTombstones()
        {
            return inRowTombstones.iterator();
        }

        public static class Serializer
        {
            public void serialize(CFMetaData metadata, LegacyDeletionInfo info, DataOutputPlus out, int version) throws IOException
            {
                throw new UnsupportedOperationException();
                //DeletionTime.serializer.serialize(info.topLevel, out);
                //rtlSerializer.serialize(info.ranges, out, version);
            }

            public LegacyDeletionInfo deserialize(CFMetaData metadata, DataInput in, int version) throws IOException
            {
                DeletionTime topLevel = DeletionTime.serializer.deserialize(in);

                int rangeCount = in.readInt();
                if (rangeCount == 0)
                    return from(new DeletionInfo(topLevel));

                RangeTombstoneList ranges = new RangeTombstoneList(metadata.comparator, rangeCount);
                List<LegacyRangeTombstone> inRowTombsones = new ArrayList<>();
                for (int i = 0; i < rangeCount; i++)
                {
                    LegacyBound start = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), true);
                    LegacyBound end = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), false);
                    int delTime =  in.readInt();
                    long markedAt = in.readLong();

                    LegacyRangeTombstone tombstone = new LegacyRangeTombstone(start, end, new SimpleDeletionTime(markedAt, delTime));
                    if (tombstone.isCollectionTombstone(metadata) || tombstone.isRowDeletion(metadata))
                        inRowTombsones.add(tombstone);
                    else
                        ranges.add(start.bound, end.bound, markedAt, delTime);
                }
                return new LegacyDeletionInfo(new DeletionInfo(topLevel, ranges), inRowTombsones);
            }

            public long serializedSize(CFMetaData metadata, LegacyDeletionInfo info, TypeSizes typeSizes, int version)
            {
                throw new UnsupportedOperationException();
                //long size = DeletionTime.serializer.serializedSize(info.topLevel, typeSizes);
                //return size + rtlSerializer.serializedSize(info.ranges, typeSizes, version);
            }
        }
    }

    public static class TombstoneTracker
    {
        private final CFMetaData metadata;
        private final DeletionTime partitionDeletion;
        private final List<LegacyRangeTombstone> openTombstones = new ArrayList<>();

        public TombstoneTracker(CFMetaData metadata, DeletionTime partitionDeletion)
        {
            this.metadata = metadata;
            this.partitionDeletion = partitionDeletion;
        }

        public void update(LegacyAtom atom)
        {
            if (atom.isCell())
            {
                if (openTombstones.isEmpty())
                    return;

                Iterator<LegacyRangeTombstone> iter = openTombstones.iterator();
                while (iter.hasNext())
                {
                    LegacyRangeTombstone tombstone = iter.next();
                    if (metadata.comparator.compare(atom, tombstone.stop.bound) >= 0)
                        iter.remove();
                }
            }

            LegacyRangeTombstone tombstone = atom.asRangeTombstone();
            if (tombstone.deletionTime.supersedes(partitionDeletion) && !tombstone.isRowDeletion(metadata) && !tombstone.isCollectionTombstone(metadata))
                openTombstones.add(tombstone);
        }

        public boolean isShadowed(LegacyAtom atom)
        {
            long timestamp = atom.isCell() ? atom.asCell().timestamp : atom.asRangeTombstone().deletionTime.markedForDeleteAt();

            if (partitionDeletion.deletes(timestamp))
                return true;

            for (LegacyRangeTombstone tombstone : openTombstones)
            {
                if (tombstone.deletionTime.deletes(timestamp))
                    return true;
            }

            return false;
        }
    }

    //public static class LegacyPartition
    //{
    //    public List<LegacyRangeTombstone> rangeTombstones;
    //    public Map<ByteBuffer, LegacyCell> cells;
    //}

    //public static class DecodedCellName
    //{
    //    public final Clustering clustering;
    //    public final ColumnDefinition column;
    //    public final ByteBuffer collectionElement;

    //    private DecodedCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
    //    {
    //        this.clustering = clustering;
    //        this.column = column;
    //        this.collectionElement = collectionElement;
    //    }
    //}

    //public void deserializeCellBody(DataInput in, DeserializedCell cell, ByteBuffer collectionElement, int mask, Flag flag, int expireBefore)
    //throws IOException
    //{
    //    if ((mask & COUNTER_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //        ///long timestampOfLastDelete = in.readLong();
    //        ///long ts = in.readLong();
    //        ///ByteBuffer value = ByteBufferUtil.readWithLength(in);
    //        ///return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
    //    }
    //    else if ((mask & COUNTER_UPDATE_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //    else if ((mask & EXPIRATION_MASK) != 0)
    //    {
    //        assert collectionElement == null;
    //        cell.isCounter = false;
    //        cell.ttl = in.readInt();
    //        cell.localDeletionTime = in.readInt();
    //        cell.timestamp = in.readLong();
    //        cell.value = ByteBufferUtil.readWithLength(in);
    //        cell.path = null;

    //        // Transform expired cell to tombstone (as it basically saves the value)
    //        if (cell.localDeletionTime < expireBefore && flag != Flag.PRESERVE_SIZE)
    //        {
    //            // The column is now expired, we can safely return a simple tombstone. Note that
    //            // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
    //            // we'll fulfil our responsibility to repair.  See discussion at
    //            // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
    //            cell.localDeletionTime = cell.localDeletionTime - cell.ttl;
    //            cell.ttl = 0;
    //            cell.value = null;
    //        }

    //    }
    //    else
    //    {
    //        cell.isCounter = false;
    //        cell.ttl = 0;
    //        cell.path = null;

    //        cell.timestamp = in.readLong();
    //        ByteBuffer value = ByteBufferUtil.readWithLength(in);

    //        boolean isDeleted = (mask & DELETION_MASK) != 0;
    //        cell.value = isDeleted ? null : value;
    //        cell.localDeletionTime = isDeleted ? ByteBufferUtil.toInt(value) : Integer.MAX_VALUE;
    //    }
    //}

    //public void skipCellBody(DataInput in, int mask)
    //throws IOException
    //{
    //    if ((mask & COUNTER_MASK) != 0)
    //        FileUtils.skipBytesFully(in, 16);
    //    else if ((mask & EXPIRATION_MASK) != 0)
    //        FileUtils.skipBytesFully(in, 16);
    //    else
    //        FileUtils.skipBytesFully(in, 8);

    //    int length = in.readInt();
    //    FileUtils.skipBytesFully(in, length);
    //}

    //public abstract IVersionedSerializer<ColumnSlice> sliceSerializer();
    //public abstract IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();

    //public DeletionInfo.Serializer deletionInfoSerializer()
    //{
    //    return deletionInfoSerializer;
    //}

    //public RangeTombstone.Serializer rangeTombstoneSerializer()
    //{
    //    return rangeTombstoneSerializer;
    //}

    //public static class LegacyAtomDeserializer extends AtomDeserializer
    //{
    //    private final Deserializer nameDeserializer;

    //    private RangeTombstone openTombstone;

    //    private final ReusableRangeTombstoneMarker marker;
    //    private final ReusableRow row;

    //    private LegacyLayout.DeserializedCell cell;

    //    public LegacyAtomDeserializer(CFMetaData metadata,
    //                                  DataInput in,
    //                                  LegacyLayout.Flag flag,
    //                                  int expireBefore,
    //                                  Version version,
    //                                  Columns columns)
    //    {
    //        super(metadata, in, flag, expireBefore, version, columns);
    //        this.nameDeserializer = metadata.layout().newDeserializer(in, version);
    //        this.marker = new ReusableRangeTombstoneMarker();
    //        this.row = new ReusableRow(columns);
    //    }

    //    public boolean hasNext() throws IOException
    //    {
    //        return hasUnprocessed() || nameDeserializer.hasNext();
    //    }

    //    public boolean hasUnprocessed() throws IOException
    //    {
    //        return openTombstone != null || nameDeserializer.hasUnprocessed();
    //    }

    //    public int compareNextTo(Clusterable prefix) throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return metadata.comparator.compare(openTombstone.max, prefix);

    //        return nameDeserializer.compareNextTo(prefix);
    //    }

    //    public Atom readNext() throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return marker.setTo(openTombstone.max, false, openTombstone.data);

    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            // TODO: deal with new style RT
    //            openTombstone = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //            return marker.setTo(openTombstone.min, true, openTombstone.data);
    //        }

    //        Row.Writer writer = row.writer();
    //        writer.writeClustering(prefix);

    //        // If there is a row marker, it's the first cell
    //        ByteBuffer columnName = nameDeserializer.getNextColumnName();
    //        if (columnName != null && !columnName.hasRemaining())
    //        {
    //            metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //            writer.writeTimestamp(cell.timestamp());
    //        }
    //        else
    //        {
    //            writer.writeTimestamp(Long.MIN_VALUE);
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if (columns.contains(column))
    //            {
    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //            else
    //            {
    //                metadata.layout().skipCellBody(in, b);
    //            }
    //        }

    //        // Read the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.readNextClustering();
    //            b = in.readUnsignedByte();
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //                    continue;
    //                }

    //                // This is a collection tombstone
    //                RangeTombstone rt = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //                // TODO: we could assert that the min and max are what we think they are. Just in case
    //                // someone thrift side has done something *really* nasty.
    //                writer.writeComplexDeletion(column, rt.data);
    //            }
    //            else
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().skipCellBody(in, b);
    //                    continue;
    //                }

    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //        }
    //        return row;
    //    }

    //    private ColumnDefinition getDefinition(ByteBuffer columnName)
    //    {
    //        // For non-CQL3 layouts, every defined column metadata is handled by the static row
    //        if (!metadata.layout().isCQL3Layout())
    //            return metadata.compactValueColumn();

    //        return metadata.getColumnDefinition(columnName);
    //    }

    //    public void skipNext() throws IOException
    //    {
    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            return;
    //        }

    //        metadata.layout().skipCellBody(in, b);

    //        // Skip the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.skipNext();
    //            b = in.readUnsignedByte();
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //                metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            else
    //                metadata.layout().skipCellBody(in, b);
    //        }
    //    }
    //}

    //public interface Deserializer
    //{
    //    /**
    //     * Whether this deserializer is done or not, i.e. whether we're reached the end of row marker.
    //     */
    //    public boolean hasNext() throws IOException;
    //    /**
    //     * Whether or not some name has been read but not consumed by readNext.
    //     */
    //    public boolean hasUnprocessed() throws IOException;
    //    /**
    //     * Comparare the next name to read to the provided Composite.
    //     * This does not consume the next name.
    //     */
    //    public int compareNextTo(Clusterable composite) throws IOException;
    //    public int compareNextPrefixTo(Clustering prefix) throws IOException;
    //    /**
    //     * Actually consume the next name and return it.
    //     */
    //    public Clustering readNextClustering() throws IOException;
    //    public ByteBuffer getNextColumnName();
    //    public ByteBuffer getNextCollectionElement();

    //    /**
    //     * Skip the next name (consuming it). This skip all the name (clustering, column name and collection element).
    //     */
    //    public void skipNext() throws IOException;
    //}

    //public static class DeserializedCell extends AbstractCell
    //{
    //    public ColumnDefinition column;
    //    private boolean isCounter;
    //    private ByteBuffer value;
    //    private long timestamp;
    //    private int localDeletionTime;
    //    private int ttl;
    //    private CellPath path;

    //    public ColumnDefinition column()
    //    {
    //        return column;
    //    }

    //    public boolean isCounterCell()
    //    {
    //        return isCounter;
    //    }

    //    public ByteBuffer value()
    //    {
    //        return value;
    //    }

    //    public long timestamp()
    //    {
    //        return timestamp;
    //    }

    //    public int localDeletionTime()
    //    {
    //        return localDeletionTime;
    //    }

    //    public int ttl()
    //    {
    //        return ttl;
    //    }

    //    public CellPath path()
    //    {
    //        return path;
    //    }

    //    public Cell takeAlias()
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //}

    // From OnDiskAtom
    //public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serializeForSSTable(OnDiskAtom atom, DataOutputPlus out) throws IOException
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            type.columnSerializer().serialize((Cell)atom, out);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            type.rangeTombstoneSerializer().serializeForSSTable((RangeTombstone)atom, out);
    //        }
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
    //    {
    //        return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
    //    {
    //        Composite name = type.serializer().deserialize(in);
    //        if (name.isEmpty())
    //        {
    //            // SSTableWriter.END_OF_ROW
    //            return null;
    //        }

    //        int b = in.readUnsignedByte();
    //        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
    //            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
    //        else
    //            return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    //    }

    //    public long serializedSizeForSSTable(OnDiskAtom atom)
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            return type.columnSerializer().serializedSize((Cell)atom, TypeSizes.NATIVE);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            return type.rangeTombstoneSerializer().serializedSizeForSSTable((RangeTombstone)atom);
    //        }
    //    }
    //}

}
