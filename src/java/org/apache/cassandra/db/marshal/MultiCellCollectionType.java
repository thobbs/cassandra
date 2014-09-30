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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.transport.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;

/** Collections that are stored internally across multiple cells. */
public abstract class MultiCellCollectionType<T> extends CollectionType<T>
{
    private static final Logger logger = LoggerFactory.getLogger(MultiCellCollectionType.class);

    protected MultiCellCollectionType(Kind kind)
    {
        super(kind);
    }

    protected abstract void appendToStringBuilder(StringBuilder sb);

    public abstract List<ByteBuffer> serializedValues(List<Cell> cells);

    @Override
    public void validateCellValue(ByteBuffer cellValue) throws MarshalException
    {
        valueComparator().validate(cellValue);
    }

    public boolean isMultiCell()
    {
        return true;
    }

    public List<Cell> enforceLimit(List<Cell> cells, int version)
    {
        if (version >= Server.VERSION_3 || cells.size() <= MAX_ELEMENTS)
            return cells;

        logger.error("Detected collection with {} elements, more than the {} limit. Only the first {} elements will be returned to the client. "
                   + "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.", cells.size(), MAX_ELEMENTS, MAX_ELEMENTS);
        return cells.subList(0, MAX_ELEMENTS);
    }

    public ByteBuffer serializeForNativeProtocol(List<Cell> cells, int version)
    {
        cells = enforceLimit(cells, version);
        List<ByteBuffer> values = serializedValues(cells);
        return CollectionSerializer.pack(values, cells.size(), version);
    }
}
