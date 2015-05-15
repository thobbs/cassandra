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

import java.nio.ByteBuffer;
import java.io.DataInput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;

/**
 * Helper class to deserialize OnDiskAtom efficiently.
 *
 * More precisely, this class is used by the low-level readers
 * (IndexedSliceReader and SSTableNamesIterator) to ensure we don't
 * do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public abstract class AtomDeserializer
{
    private static final Logger logger = LoggerFactory.getLogger(AtomDeserializer.class);

    protected final CFMetaData metadata;
    protected final DataInput in;
    protected final SerializationHelper helper;

    protected AtomDeserializer(CFMetaData metadata,
                               DataInput in,
                               SerializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static AtomDeserializer create(CFMetaData metadata,
                                          DataInput in,
                                          SerializationHeader header,
                                          SerializationHelper helper,
                                          DeletionTime partitionDeletion,
                                          boolean readAllAsDynamic)
    {
        if (helper.version >= MessagingService.VERSION_30)
            return new CurrentDeserializer(metadata, in, header, helper);
        else
            return new OldFormatDeserializer(metadata, in, helper, partitionDeletion, readAllAsDynamic);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public abstract int compareNextTo(Slice.Bound bound) throws IOException;

    /**
     * Returns whether the next atom is a row or not.
     */
    public abstract boolean nextIsRow() throws IOException;

    /**
     * Returns whether the next atom is the static row or not.
     */
    public abstract boolean nextIsStatic() throws IOException;

    /**
     * Returns the next atom.
     */
    public abstract Atom readNext() throws IOException;

    /**
     * Clears any state in this deserializer.
     */
    public abstract void clearState() throws IOException;

    /**
     * Skips the next atom.
     */
    public abstract void skipNext() throws IOException;

    private static class CurrentDeserializer extends AtomDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
        private boolean isReady;
        private boolean isDone;

        private final ReusableRow row;
        private final ReusableRangeTombstoneMarker marker;

        private CurrentDeserializer(CFMetaData metadata,
                                    DataInput in,
                                    SerializationHeader header,
                                    SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
            this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
            this.row = new ReusableRow(metadata.clusteringColumns().size(), header.columns().regulars, helper.nowInSec, metadata.isCounter());
            this.marker = new ReusableRangeTombstoneMarker(metadata.clusteringColumns().size());
        }

        public boolean hasNext() throws IOException
        {
            if (isReady)
                return true;

            prepareNext();
            return !isDone;
        }

        private void prepareNext() throws IOException
        {
            if (isDone)
                return;

            nextFlags = in.readUnsignedByte();
            if (AtomSerializer.isEndOfPartition(nextFlags))
            {
                isDone = true;
                isReady = false;
                return;
            }

            clusteringDeserializer.prepare(nextFlags);
            isReady = true;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            if (!isReady)
                prepareNext();

            assert !isDone;

            return clusteringDeserializer.compareNextTo(bound);
        }

        public boolean nextIsRow() throws IOException
        {
            if (!isReady)
                prepareNext();

            return AtomSerializer.kind(nextFlags) == Atom.Kind.ROW;
        }

        public boolean nextIsStatic() throws IOException
        {
            // This exists only for the sake of the OldFormatDeserializer
            throw new UnsupportedOperationException();
        }

        public Atom readNext() throws IOException
        {
            isReady = false;
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker.Writer writer = marker.writer();
                clusteringDeserializer.deserializeNextBound(writer);
                AtomSerializer.serializer.deserializeMarkerBody(in, header, helper, nextFlags, writer);
                return marker;
            }
            else
            {
                Row.Writer writer = row.writer();
                clusteringDeserializer.deserializeNextClustering(writer);
                AtomSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, writer);
                return row;
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            clusteringDeserializer.skipNext();
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                AtomSerializer.serializer.skipMarkerBody(in, header, nextFlags);
            }
            else
            {
                AtomSerializer.serializer.skipRowBody(in, header, helper, nextFlags);
            }
        }

        public void clearState()
        {
            isReady = false;
            isDone = false;
        }
    }

    public static class OldFormatDeserializer extends AtomDeserializer
    {
        private final boolean readAllAsDynamic;
        private boolean skipStatic;

        private int nextFlags;
        private boolean isDone;
        private boolean isStart = true;

        private final LegacyLayout.CellGrouper grouper;
        private LegacyLayout.LegacyAtom nextAtom;

        private boolean staticFinished;
        private LegacyLayout.LegacyAtom savedAtom;

        private final LegacyLayout.TombstoneTracker tombstoneTracker;

        private RangeTombstoneMarker closingMarker;

        private OldFormatDeserializer(CFMetaData metadata,
                                      DataInput in,
                                      SerializationHelper helper,
                                      DeletionTime partitionDeletion,
                                      boolean readAllAsDynamic)
        {
            super(metadata, in, helper);
            this.readAllAsDynamic = readAllAsDynamic;
            this.grouper = new LegacyLayout.CellGrouper(metadata, helper.nowInSec);
            this.tombstoneTracker = new LegacyLayout.TombstoneTracker(metadata, partitionDeletion);
        }

        public void setSkipStatic()
        {
            this.skipStatic = true;
        }

        public boolean hasNext() throws IOException
        {
            if (nextAtom != null)
                return true;

            if (isDone)
                return false;

            return deserializeNextAtom();
        }

        private boolean deserializeNextAtom() throws IOException
        {
            if (staticFinished && savedAtom != null)
            {
                nextAtom = savedAtom;
                savedAtom = null;
                return true;
            }

            while (true)
            {
                nextAtom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                if (nextAtom == null)
                {
                    isDone = true;
                    return false;
                }
                else if (tombstoneTracker.isShadowed(nextAtom))
                {
                    // We don't want to return shadowed data because that would fail the contract
                    // of AtomIterator. However the old format could have shadowed data, so filter it here.
                    nextAtom = null;
                    continue;
                }

                tombstoneTracker.update(nextAtom);

                // For static compact tables, the "column_metadata" columns are supposed to be static, but in the old
                // format they are intermingled with other columns. We deal with that with 2 different strategy:
                //  1) for thrift queries, we basically consider everything as a "dynamic" cell. This is ok because
                //     that's basically what we end up with on ThriftResultsMerger has done its thing.
                //  2) otherwise, we make sure to extract the "static" columns first (see AbstractSSTableIterator.readStaticRow
                //     and SSTableAtomIterator.readStaticRow) as a first pass. So, when we do a 2nd pass for dynamic columns
                //     (which in practice we only do for compactions), we want to ignore those extracted static columns.
                if (skipStatic && metadata.isStaticCompactTable() && nextAtom.isCell())
                {
                    LegacyLayout.LegacyCell cell = nextAtom.asCell();
                    if (cell.name.column.isStatic())
                    {
                        nextAtom = null;
                        continue;
                    }
                }

                // We want to fetch the static row as the first thing this deserializer return.
                // However, in practice, it's possible to have range tombstone before the static row cells
                // if that tombstone has an empty start. So if we do, we save it initially so we can get
                // to the static parts (if there is any).
                if (isStart)
                {
                    isStart = false;
                    if (!nextAtom.isCell())
                    {
                        LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
                        if (tombstone.start.bound.size() == 0)
                        {
                            savedAtom = tombstone;
                            nextAtom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                            if (nextAtom == null)
                            {
                                // That was actually the only atom so use it after all
                                nextAtom = savedAtom;
                                savedAtom = null;
                            }
                            else if (!nextAtom.isStatic())
                            {
                                // We don't have anything static. So we do want to send first
                                // the saved atom, so switch
                                LegacyLayout.LegacyAtom atom = nextAtom;
                                nextAtom = savedAtom;
                                savedAtom = atom;
                            }
                        }
                    }
                }

                return true;
            }
        }

        private void checkReady() throws IOException
        {
            if (nextAtom == null)
                hasNext();
            assert !isDone;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            checkReady();
            return metadata.comparator.compare(nextAtom, bound);
        }

        public boolean nextIsRow() throws IOException
        {
            checkReady();
            if (nextAtom.isCell())
                return true;

            LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
            return tombstone.isCollectionTombstone(metadata) || tombstone.isRowDeletion(metadata);
        }

        public boolean nextIsStatic() throws IOException
        {
            checkReady();
            return nextAtom.isStatic();
        }

        public Atom readNext() throws IOException
        {
            if (!nextIsRow())
            {
                LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
                // TODO: this is actually more complex, we can have repeated markers etc....
                if (closingMarker == null)
                    throw new UnsupportedOperationException();
                closingMarker = new SimpleRangeTombstoneMarker(tombstone.stop.bound, tombstone.deletionTime);
                return new SimpleRangeTombstoneMarker(tombstone.start.bound, tombstone.deletionTime);
            }

            LegacyLayout.CellGrouper grouper = nextAtom.isStatic()
                                             ? LegacyLayout.CellGrouper.staticGrouper(metadata, helper.nowInSec)
                                             : this.grouper;

            grouper.reset();
            grouper.addAtom(nextAtom);
            while (deserializeNextAtom() && grouper.addAtom(nextAtom))
            {
            }

            // if this was the first static row, we're done with it. Otherwise, we're also done with static.
            staticFinished = true;
            return grouper.getRow();
        }

        public void skipNext() throws IOException
        {
            readNext();
        }

        public void clearState()
        {
            isDone = false;
            nextAtom = null;
        }
    }
}
