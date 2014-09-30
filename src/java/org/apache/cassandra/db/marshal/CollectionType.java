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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The abstract validator that is the base for maps, sets and lists (both frozen and non-frozen).
 *
 * Please note that this comparator shouldn't be used "manually" (through thrift for instance).
 */
public abstract class CollectionType<T> extends AbstractType<T>
{
    private static final Logger logger = LoggerFactory.getLogger(CollectionType.class);

    public static final int MAX_ELEMENTS = 65535;

    public enum Kind
    {
        MAP, SET, LIST
    }

    public final Kind kind;

    protected CollectionType(Kind kind)
    {
        this.kind = kind;
    }

    public abstract AbstractType<?> nameComparator();
    public abstract AbstractType<?> valueComparator();

    protected abstract void appendToStringBuilder(StringBuilder sb);

    @Override
    public abstract CollectionSerializer<T> getSerializer();

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        appendToStringBuilder(sb);
        return sb.toString();
    }

    public String getString(ByteBuffer bytes)
    {
        return BytesType.instance.getString(bytes);
    }

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    public boolean isCollection()
    {
        return true;
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        CollectionType tprev = (CollectionType) previous;

        if (!this.nameComparator().isCompatibleWith(tprev.nameComparator()))
            return false;

        if (isMultiCell())
            // the value is only used for Cell values
            return this.valueComparator().isValueCompatibleWith(tprev.valueComparator());
        else
            return this.valueComparator().isCompatibleWith(tprev.valueComparator());
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        // for multi-cell collections, value compatibility inherently involves sorting due to the cell name including
        // the collection element, so we should just check isCompatibleWith()
        if (isMultiCell())
            return isCompatibleWith(previous);

        CollectionType tprev = (CollectionType) previous;
        return this.nameComparator().isValueCompatibleWith(tprev.nameComparator()) &&
               this.valueComparator().isValueCompatibleWith(tprev.valueComparator());
    }

    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Collection(this);
    }
}
