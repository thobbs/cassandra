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

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Row
{
    private static final Logger logger = LoggerFactory.getLogger(Row.class);
    public static final RowSerializer serializer = new RowSerializer();

    public final DecoratedKey key;
    public final ColumnFamily cf;

    public Row(DecoratedKey key, ColumnFamily cf)
    {
        assert key != null;
        // cf may be null, indicating no data
        this.key = key;
        this.cf = cf;
    }

    public Row(ByteBuffer key, ColumnFamily updates)
    {
        this(StorageService.getPartitioner().decorateKey(key), updates);
    }

    @Override
    public String toString()
    {
        return "Row(" +
               "key=" + key +
               ", cf=" + cf +
               ')';
    }

    public int getLiveCount(IDiskAtomFilter filter, long now)
    {
        return cf == null ? 0 : filter.getLiveCount(cf, now);
    }

    public static class RowSerializer implements IVersionedSerializer<Row>
    {
        public void serialize(Row row, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(row.key.getKey(), out);
            ColumnFamily.serializer.serialize(row.cf, out, version);
        }

        public Row deserialize(DataInput in, int version, ColumnSerializer.Flag flag) throws IOException
        {
            DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
            logger.warn("### deserialized key: {}", key);
            ColumnFamily cf = ColumnFamily.serializer.deserialize(in, flag, version);
            logger.warn("### deserialized cf: {}", cf);
            return new Row(key, cf);
        }

        public Row deserialize(DataInput in, int version) throws IOException
        {
            return deserialize(in, version, ColumnSerializer.Flag.LOCAL);
        }

        public long serializedSize(Row row, int version)
        {
            int keySize = row.key.getKey().remaining();
            return TypeSizes.NATIVE.sizeof((short) keySize) + keySize + ColumnFamily.serializer.serializedSize(row.cf, TypeSizes.NATIVE, version);
        }
    }
}
