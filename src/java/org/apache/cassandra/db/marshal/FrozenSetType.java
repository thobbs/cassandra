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
import org.apache.cassandra.serializers.SetSerializer;

/** A set that has been serialized into a single blob. The contents of the set cannot change. */
public class FrozenSetType<T> extends CollectionType<Set<T>> implements ISetType<T>
{
    // interning instances
    private static final Map<AbstractType<?>, FrozenSetType> instances = new HashMap<>();

    private final AbstractType<T> elements;
    private final SetSerializer<T> serializer;

    public static FrozenSetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("FrozenSetType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized <T> FrozenSetType<T> getInstance(AbstractType<T> elements)
    {
        FrozenSetType<T> t = instances.get(elements);
        if (t == null)
        {
            t = new FrozenSetType<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    public FrozenSetType(AbstractType<T> elements)
    {
        super(Kind.SET);
        this.elements = elements;
        this.serializer = SetSerializer.getInstance(elements.getSerializer());
    }

    public AbstractType<T> getElementsType()
    {
        return elements;
    }

    public AbstractType<T> nameComparator()
    {
        return elements;
    }

    public AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return ListType.compareListOrSet(elements, o1, o2);
    }

    public SetSerializer<T> getSerializer()
    {
        return serializer;
    }

    public boolean isByteOrderComparable()
    {
        return elements.isByteOrderComparable();
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public boolean contains(ByteBuffer serializedList, ByteBuffer serializedItem)
    {
        T item = elements.getSerializer().deserialize(serializedItem);
        Set<T> items = serializer.deserialize(serializedList);
        for (T possibleMatch : items)
        {
            if (item.equals(possibleMatch))
                return true;
        }
        return false;
    }
}
