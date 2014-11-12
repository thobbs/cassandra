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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * A user defined type.
 *
 * A user type is really just a tuple type on steroids.
 */
public class UserType extends TupleType
{
    public final String keyspace;
    public final ByteBuffer name;
    private final List<ByteBuffer> fieldNames;

    public UserType(String keyspace, ByteBuffer name, List<ByteBuffer> fieldNames, List<AbstractType<?>> fieldTypes)
    {
        super(fieldTypes);
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.fieldNames = fieldNames;
    }

    public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
        String keyspace = params.left.left;
        ByteBuffer name = params.left.right;
        List<ByteBuffer> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType> p : params.right)
        {
            columnNames.add(p.left);
            columnTypes.add(p.right);
        }
        return new UserType(keyspace, name, columnNames, columnTypes);
    }

    public AbstractType<?> fieldType(int i)
    {
        return type(i);
    }

    public List<AbstractType<?>> fieldTypes()
    {
        return types;
    }

    public ByteBuffer fieldName(int i)
    {
        return fieldNames.get(i);
    }

    public List<ByteBuffer> fieldNames()
    {
        return fieldNames;
    }

    public String getNameAsString()
    {
        return UTF8Type.instance.compose(name);
    }

    // Note: the only reason we override this is to provide nicer error message, but since that's not that much code...
    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        ByteBuffer input = bytes.duplicate();
        for (int i = 0; i < size(); i++)
        {
            // we allow the input to have less fields than declared so as to support field addition.
            if (!input.hasRemaining())
                return;

            if (input.remaining() < 4)
                throw new MarshalException(String.format("Not enough bytes to read size of %dth field %s", i, fieldName(i)));

            int size = input.getInt();

            // size < 0 means null value
            if (size < 0)
                continue;

            if (input.remaining() < size)
                throw new MarshalException(String.format("Not enough bytes to read %dth field %s", i, fieldName(i)));

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            types.get(i).validate(field);
        }

        // We're allowed to get less fields than declared, but not more
        if (input.hasRemaining())
            throw new MarshalException("Invalid remaining data after end of UDT value");
    }

    @Override
    public ByteBuffer fromJSONObject(Object parsed) throws MarshalException
    {
        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map map = (Map) parsed;
        List<ByteBuffer> buffers = new ArrayList<>(map.size() / 2);

        for (int i = 0; i < types.size(); i++)
        {
            Object value = map.get(fieldNames.get(i))
        }
        for (Map.Entry<Object, Object> entry : map.entrySet())
        {
            if (entry.getKey() == null)
                throw new MarshalException("Invalid null field name in user-defined type");

            if (entry.getValue() == null)
                throw new MarshalException("Invalid null value in map");

            buffers.add(UTF8Type.instance.fromJSONObject(entry.getKey()));
            buffers.add(values.fromJSONObject(entry.getValue()));
        }
        return CollectionSerializer.pack(buffers, map.size(), Server.CURRENT_VERSION);
    }

    @Override
    public String toJSONString(ByteBuffer buffer)
    {
        // TODO we cannot assume the current protocol version here, since the collection gets serialized prior to function execution
        StringBuilder sb = new StringBuilder("{");
        int size = CollectionSerializer.readCollectionSize(buffer, Server.CURRENT_VERSION);
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            sb.append(keys.toJSONString(CollectionSerializer.readValue(buffer, Server.CURRENT_VERSION)));
            sb.append(": ");
            sb.append(values.toJSONString(CollectionSerializer.readValue(buffer, Server.CURRENT_VERSION)));
        }
        return sb.append("}").toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspace, name, fieldNames, types);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof UserType))
            return false;

        UserType that = (UserType)o;
        return keyspace.equals(that.keyspace) && name.equals(that.name) && fieldNames.equals(that.fieldNames) && types.equals(that.types);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.UserDefined.create(this);
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyUserTypeParameters(keyspace, name, fieldNames, types);
    }
}
