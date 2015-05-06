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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.ColumnsSelection;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

abstract class AbstractSSTableIterator implements SliceableAtomIterator
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractSSTableIterator.class);

    protected final SSTableReader sstable;
    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnsSelection columns;
    protected final SerializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    private final boolean isForThrift;

    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      RowIndexEntry indexEntry,
                                      ColumnsSelection columnsSelection,
                                      int nowInSec,
                                      boolean isForThrift)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columnsSelection;
        this.helper = new SerializationHelper(sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL, nowInSec, columnsSelection);
        this.isForThrift = isForThrift;

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            try
            {
                boolean shouldCloseFile = file == null;
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     and we need to read the partition deletion time.
                //   - we're querying static columns.
                boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !columns.columns().statics.isEmpty();

                // For CQL queries on static compact tables, we only want to consider static value (only those are exposed),
                // but readStaticRow have already read them and might in fact have consumed the whole partition (when reading
                // the legacy file format), so set the reader to null so we don't try to read anything more. We can remove this
                // once we drop support for the legacy file format
                boolean needsReader = sstable.descriptor.version.storeRows() || isForThrift || !sstable.metadata.isStaticCompactTable();

                if (needSeekAtPartitionStart)
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.position);
                    else
                        file.seek(indexEntry.position);

                    ByteBufferUtil.skipShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);

                    // Note that this needs to be called after file != null and after the partitionDeletion has been set, but before readStaticRow
                    // (since it uses it) so we can't move that up (but we'll be able to simplify as soon as we drop support for the old file format).
                    this.reader = needsReader ? createReader(indexEntry, file, needSeekAtPartitionStart, shouldCloseFile) : null;
                    this.staticRow = readStaticRow(sstable, file, helper, columns.columns().statics, isForThrift, nowInSec, reader == null ? null : reader.deserializer);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = needsReader ? createReader(indexEntry, file, needSeekAtPartitionStart, shouldCloseFile) : null;
                }

            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, file.getPath());
            }
        }
    }

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     SerializationHelper helper,
                                     Columns statics,
                                     boolean isForThrift,
                                     int nowInSec,
                                     AtomDeserializer deserializer) throws IOException
    {
        if (!sstable.descriptor.version.storeRows())
        {
            if (!sstable.metadata.isCompactTable())
            {
                assert deserializer != null;
                return deserializer.hasNext() && deserializer.nextIsStatic()
                     ? (Row)deserializer.readNext()
                     : Rows.EMPTY_STATIC_ROW;
            }

            // For compact tables, we use statics for the "column_metadata" definition. However, in the old format, those
            // "column_metadata" are intermingled as any other "cell". In theory, this means that we'd have to do a first
            // pass to extract the static values. However, for thrift, we'll use the ThriftResultsMerger right away which
            // will re-merge static values with dynamic ones, so we can just ignore static and read every cell as a
            // "dynamic" one. For CQL, if the table is a "static compact", then is has only static columns exposed and no
            // dynamic ones. So we do a pass to extract static columns here, but will have no more work to do. Otherwise,
            // the table won't have static columns.
            if (statics.isEmpty() || isForThrift)
                return Rows.EMPTY_STATIC_ROW;

            assert sstable.metadata.isStaticCompactTable() && !isForThrift;

            // As said above, if it's a CQL query and the table is a "static compact", the only exposed columns are the
            // static ones. So we don't have to mark the position to seek back later.
            return LegacyLayout.extractStaticColumns(sstable.metadata, file, statics, nowInSec);
        }

        if (!sstable.header.hasStatic())
            return Rows.EMPTY_STATIC_ROW;

        if (statics.isEmpty())
        {
            AtomSerializer.serializer.skipStaticRow(file, sstable.header, helper);
            return Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            return AtomSerializer.serializer.deserializeStaticRow(file, sstable.header, helper);
        }
    }

    protected abstract Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile);

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return columns.columns();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public AtomStats stats()
    {
        // We could return sstable.header.stats(), but this may not be as accurate than the actual sstable stats (see
        // SerializationHeader.make() for details) so we use the latter instead.
        return new AtomStats(sstable.getMinTimestamp(), sstable.getMinLocalDeletionTime(), sstable.getMinTTL(), sstable.getAvgColumnSetPerRow());
    }

    public int nowInSec()
    {
        return helper.nowInSec;
    }

    public boolean hasNext()
    {
        try
        {
            return reader != null && reader.hasNext();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public Atom next()
    {
        try
        {
            assert reader != null;
            return reader.next();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public Iterator<Atom> slice(Slice slice)
    {
        try
        {
            if (reader == null)
                return Collections.emptyIterator();

            return reader.slice(slice);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        if (reader != null)
        {
            try
            {
                reader.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }
    }

    protected abstract class Reader
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        protected AtomDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        protected DeletionTime openMarker = null;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;
            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = AtomDeserializer.create(sstable.metadata, file, sstable.header, helper, partitionLevelDeletion, isForThrift);
        }

        protected void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
                deserializer.clearState();
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.clustering().isStart() ? marker.deletionTime().takeAlias() : null;
        }

        protected DeletionTime getAndClearOpenMarker()
        {
            DeletionTime toReturn = openMarker;
            openMarker = null;
            return toReturn;
        }

        public abstract boolean hasNext() throws IOException;
        public abstract Atom next() throws IOException;
        public abstract Iterator<Atom> slice(Slice slice) throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    protected abstract class IndexedReader extends Reader
    {
        protected final RowIndexEntry indexEntry;
        protected final List<IndexHelper.IndexInfo> indexes;

        protected int currentIndexIdx = -1;

        // Marks the beginning of the block corresponding to currentIndexIdx.
        protected FileMark mark;

        // !isInit means we have never seeked in the file and thus shouldn't read as we could be anywhere
        protected boolean isInit;

        protected IndexedReader(FileDataInput file, boolean shouldCloseFile, RowIndexEntry indexEntry, boolean isInit)
        {
            super(file, shouldCloseFile);
            this.indexEntry = indexEntry;
            this.indexes = indexEntry.columnsIndex();
            this.isInit = isInit;
        }

        // Should be called when we're at the beginning of blockIdx.
        protected void updateBlock(int blockIdx) throws IOException
        {
            seekToPosition(indexEntry.position + indexes.get(blockIdx).offset);

            currentIndexIdx = blockIdx;
            openMarker = blockIdx > 0 ? indexes.get(blockIdx - 1).endOpenMarker : null;
            mark = file.mark();
        }

        public IndexHelper.IndexInfo currentIndex()
        {
            return indexes.get(currentIndexIdx);
        }

        public IndexHelper.IndexInfo previousIndex()
        {
            return currentIndexIdx <= 1 ? null : indexes.get(currentIndexIdx - 1);
        }
    }
}
