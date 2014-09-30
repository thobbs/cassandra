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
import java.util.*;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.ListSerializer;

/** A list that has been serialized into a single blob. The contents of the list cannot change. */
public class FrozenListType<T> extends CollectionType<List<T>> implements IListType<T>
{
    // interning instances
    private static final Map<AbstractType<?>, FrozenListType> instances = new HashMap<>();

    public final AbstractType<T> elements;
    public final ListSerializer<T> serializer;

    public static FrozenListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("FrozenListType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized <T> FrozenListType<T> getInstance(AbstractType<T> elements)
    {
        FrozenListType<T> t = instances.get(elements);
        if (t == null)
        {
            t = new FrozenListType<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private FrozenListType(AbstractType<T> elements)
    {
        super(Kind.LIST);
        this.elements = elements;
        this.serializer = ListSerializer.getInstance(elements.getSerializer());
    }

    public AbstractType<UUID> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<T> valueComparator()
    {
        return elements;
    }

    public ListSerializer<T> getSerializer()
    {
        return serializer;
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return ListType.compareListOrSet(elements, o1, o2);
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public AbstractType<T> getElementsType()
    {
        return elements;
    }
}
