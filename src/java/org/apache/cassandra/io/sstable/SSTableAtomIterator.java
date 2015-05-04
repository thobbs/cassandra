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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.IOError;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Utility class to handle deserializing atom from sstables.
 *
 * Note that this is not a full fledged AtomIterator. It's also not closeable, it is always
 * the job of the user to close the underlying ressources.
 */
public abstract class SSTableAtomIterator extends AbstractIterator<Atom> implements Iterator<Atom>
{
    protected final CFMetaData metadata;
    protected final DataInput in;
    protected final SerializationHelper helper;

    private SSTableAtomIterator(CFMetaData metadata, DataInput in, SerializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static SSTableAtomIterator create(CFMetaData metadata, DataInput in, SerializationHeader header, SerializationHelper helper, DeletionTime partitionDeletion)
    {
        if (helper.version < MessagingService.VERSION_30)
            return new OldFormatIterator(metadata, in, helper, partitionDeletion);
        else
            return new CurrentFormatIterator(metadata, in, header, helper);
    }

    public abstract Row readStaticRow() throws IOException;

    public int nowInSec()
    {
        return helper.nowInSec;
    }

    private static class CurrentFormatIterator extends SSTableAtomIterator
    {
        private final SerializationHeader header;

        private final ReusableRow row;
        private final ReusableRangeTombstoneMarker marker;

        private CurrentFormatIterator(CFMetaData metadata, DataInput in, SerializationHeader header, SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;

            int clusteringSize = metadata.comparator.size();
            Columns regularColumns = header == null ? metadata.partitionColumns().regulars : header.columns().regulars;

            this.row = new ReusableRow(clusteringSize, regularColumns, helper.nowInSec, metadata.isCounter());
            this.marker = new ReusableRangeTombstoneMarker(clusteringSize);
        }

        public Row readStaticRow() throws IOException
        {
            return header.hasStatic()
                ? AtomSerializer.serializer.deserializeStaticRow(in, header, helper)
                : Rows.EMPTY_STATIC_ROW;
        }

        protected Atom computeNext()
        {
            try
            {
                Atom.Kind kind = AtomSerializer.serializer.deserialize(in, header, helper, row.writer(), marker.writer());

                return kind == null
                     ? endOfData()
                     : (kind == Atom.Kind.ROW ? row : marker);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    private static class OldFormatIterator extends SSTableAtomIterator
    {
        private final AtomDeserializer deserializer;

        private OldFormatIterator(CFMetaData metadata, DataInput in, SerializationHelper helper, DeletionTime partitionDeletion)
        {
            super(metadata, in, helper);
            // We use an AtomDeserializer because even though we don't need all it's fanciness, it happens to handle all
            // the details we need for reading the old format.
            this.deserializer = AtomDeserializer.create(metadata, in, null, helper, partitionDeletion, false);
        }

        public Row readStaticRow() throws IOException
        {
            if (metadata.isCompactTable())
            {
                // For static compact tables, in the old format, static columns are intermingled with the other columns, so we
                // need to extract them. Which imply 2 passes (one to extract the static, then one for other value).
                if (metadata.isStaticCompactTable())
                {
                    // Because we don't support streaming from old file version, the only case we should get there is for compaction,
                    // where the DataInput should be a file based one.
                    assert in instanceof FileDataInput;
                    FileDataInput file = (FileDataInput)in;
                    FileMark mark = file.mark();
                    Row staticRow = LegacyLayout.extractStaticColumns(metadata, file, metadata.partitionColumns().statics, helper.nowInSec);
                    file.reset(mark);

                    // We've extracted the static columns, so we must ignore them on the 2nd pass
                    ((AtomDeserializer.OldFormatDeserializer)deserializer).setSkipStatic();
                    return staticRow;
                }
                else
                {
                    return Rows.EMPTY_STATIC_ROW;
                }
            }

            return deserializer.hasNext() && deserializer.nextIsStatic()
                 ? (Row)deserializer.readNext()
                 : Rows.EMPTY_STATIC_ROW;

        }

        protected Atom computeNext()
        {
            try
            {
                if (!deserializer.hasNext())
                    return endOfData();

                Atom atom = deserializer.readNext();
                if (metadata.isStaticCompactTable() && atom.kind() == Atom.Kind.ROW)
                {
                    Row row = (Row)atom;
                    ColumnDefinition def = metadata.getColumnDefinition(LegacyLayout.encodeClustering(metadata, row.clustering()));
                    if (def != null && def.isStatic())
                        return computeNext();
                }
                return atom;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

    }

}
