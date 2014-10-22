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
package org.apache.cassandra.cql3.functions;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public class FromJsonFct extends AbstractFunction
{
    public static final String NAME = "fromJson";

    private static final Map<AbstractType<?>, FromJsonFct> instances = new HashMap<>();

    public static synchronized FromJsonFct getInstance(AbstractType<?> returnType)
    {
        FromJsonFct func = instances.get(returnType);
        if (func == null)
        {
            func = new FromJsonFct(returnType);
            instances.put(returnType, func);
        }
        return func;
    }

    private final Decoder decoder;

    private FromJsonFct(AbstractType<?> returnType)
    {
        super(NAME, returnType, UTF8Type.instance);
        decoder = decoderForType(returnType);
    }

    public static Decoder decoderForType(AbstractType<?> type)
    {
        if (type instanceof BytesType)
            return new BlobDecoder();
        else if (type instanceof Int32Type)
            return new IntDecoder();
        else if (type instanceof LongType)
            return new LongDecoder();
        else if (type instanceof IntegerType)
            return new VarintDecoder();
        else if (type instanceof FloatType)
            return new FloatDecoder();
        else if (type instanceof DoubleType)
            return new DoubleDecoder();
        else if (type instanceof DecimalType)
            return new DecimalDecoder();
        else if (type instanceof BooleanType)
            return new BooleanDecoder();
        else if (type instanceof AsciiType)
            return new AsciiDecoder();
        else if (type instanceof UTF8Type)
            return new UTF8Decoder();
        else if (type instanceof InetAddressType)
            return new InetAddressDecoder();
        else if (type instanceof TimeUUIDType)
            return new TimeUUIDDecoder();
        else if (type instanceof TimestampType)
            return new TimestampDecoder();
        else if (type instanceof UUIDType)
            return new UUIDDecoder();
        else if (type instanceof ListType)
            return new ListDecoder(((ListType<?>)type).elements);
        else if (type instanceof SetType)
            return new SetDecoder(((SetType<?>)type).elements);
        else if (type instanceof MapType)
        {
            MapType<?,?> mapType = (MapType<?,?>) type;
            return new MapDecoder(mapType.keys, mapType.values);
        }
        else
            throw new AssertionError(String.format("Type %s is not supported by fromJson()", type.getClass().getName()));
    }

    public ByteBuffer execute(List<ByteBuffer> parameters) throws InvalidRequestException
    {
        assert parameters.size() == 1 : "Unexpectedly got " + parameters.size() + " arguments for fromJson()";
        ByteBuffer argument = parameters.get(0);
        String jsonArg = UTF8Type.instance.getSerializer().deserialize(argument);
        try
        {
            Object object = JSONValue.parseWithException(jsonArg);
            if (object == null)
                return null;
            return decoder.decode(object);
        }
        catch (ParseException exc)
        {
            throw new InvalidRequestException(String.format("Could not decode JSON string '%s': %s", jsonArg, exc.toString()));
        }
    }

    private static interface Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException;
    }

    private static class IntDecoder implements Decoder
    {
        private final TypeSerializer<Integer> serializer = Int32Type.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(((Number) object).intValue());
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected an int value, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
        }
    }

    private static class LongDecoder implements Decoder
    {
        private final TypeSerializer<Long> serializer = LongType.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(((Number) object).longValue());
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected a long value, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
        }
    }

    private static class VarintDecoder implements Decoder
    {
        private final TypeSerializer<BigInteger> serializer = IntegerType.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(new BigInteger(object.toString()));
            }
            catch (NumberFormatException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Value '%s' is not a valid representation of a varint value", object));
            }
        }
    }

    private static class FloatDecoder implements Decoder
    {
        private final TypeSerializer<Float> serializer = FloatType.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(((Number) object).floatValue());
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected a float value, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
        }
    }

    private static class DoubleDecoder implements Decoder
    {
        private final TypeSerializer<Double> serializer = DoubleType.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(((Number) object).doubleValue());
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected a double value, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
        }
    }

    private static class DecimalDecoder implements Decoder
    {
        private final TypeSerializer<BigDecimal> serializer = DecimalType.instance.getSerializer();

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return serializer.serialize(new BigDecimal(object.toString()));
            }
            catch (NumberFormatException exc)
            {
                throw new InvalidRequestException(String.format("Value '%s' is not a valid representation of a decimal value", object));
            }
        }
    }

    private static class UTF8Decoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return UTF8Type.instance.fromString((String) object);
            }
            catch (ClassCastException | MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Value '%s' is not a valid UTF-8 string: %s", object, exc.getMessage()));
            }
        }
    }

    private static class AsciiDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return AsciiType.instance.fromString((String)object);
            }
            catch (ClassCastException | MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Value '%s' is not a valid ASCII string: %s", object, exc.getMessage()));
            }
        }
    }

    private static class BlobDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return BytesType.instance.fromString((String)object);
            }
            catch (ClassCastException | MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Value '%s' is not a valid blob representation: %s", object, exc.getMessage()));
            }
        }
    }

    private static class BooleanDecoder implements Decoder
    {
        private static TypeSerializer<Boolean> serializer = BooleanSerializer.instance;

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            if (!(object instanceof Boolean))
                throw new InvalidRequestException(String.format(
                        "Expected a boolean value, but got a %s: %s", object.getClass().getSimpleName(), object));

            return serializer.serialize((Boolean)object);
        }
    }

    private static class UUIDDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return UUIDType.instance.fromString((String) object);
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(
                        String.format("Expected a string representation of a uuid, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Got invalid string representation of a uuid: '%s'", object));
            }
        }
    }

    private static class TimeUUIDDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return TimeUUIDType.instance.fromString((String) object);
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(
                        String.format("Expected a string representation of a timeuuid, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Got invalid string representation of a timeuuid: '%s'", object));
            }
        }
    }

    private static class TimestampDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            if (object instanceof Long)
                return ByteBufferUtil.bytes((Long) object);

            try
            {
                return TimestampType.instance.fromString((String) object);
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected a long or a datestring representation of a timestamp value, but got a %s: %s",
                        object.getClass().getSimpleName(), object));
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Error creating timestamp value: %s", exc.getMessage()));
            }
        }
    }

    private static class InetAddressDecoder implements Decoder
    {
        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            try
            {
                return InetAddressType.instance.fromString((String) object);
            }
            catch (ClassCastException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Expected a string representation of an inet value, but got a %s: %s", object.getClass().getSimpleName(), object));
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format(
                        "Value '%s' is not a valid inet representation", object));
            }
        }
    }

    private static class MapDecoder implements Decoder
    {
        private final Decoder keyDecoder;
        private final Decoder valueDecoder;

        public MapDecoder(AbstractType<?> keyType, AbstractType<?> valueType)
        {
            keyDecoder = decoderForType(keyType);
            valueDecoder = decoderForType(valueType);
        }

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            if (!(object instanceof Map))
                throw new InvalidRequestException(String.format(
                        "Expected a map, but got a %s: %s", object.getClass().getSimpleName(), object));

            Map<Object, Object> map = (Map<Object, Object>) object;
            List<ByteBuffer> buffers = new ArrayList<>(map.size() * 2);
            for (Map.Entry<Object, Object> entry : map.entrySet())
            {
                if (entry.getKey() == null)
                    throw new InvalidRequestException("Invalid null key in map");

                if (entry.getValue() == null)
                    throw new InvalidRequestException("Invalid null value in map");

                buffers.add(keyDecoder.decode(entry.getKey()));
                buffers.add(valueDecoder.decode(entry.getValue()));
            }
            return CollectionSerializer.pack(buffers, map.size(), Server.CURRENT_VERSION);
        }
    }

    private static class ListDecoder implements Decoder
    {
        private final Decoder elementsDecoder;

        public ListDecoder(AbstractType<?> elementsType)
        {
            elementsDecoder = decoderForType(elementsType);
        }

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            if (!(object instanceof List))
                throw new InvalidRequestException(String.format(
                        "Expected a list, but got a %s: %s", object.getClass().getSimpleName(), object));

            List list = (List) object;
            List<ByteBuffer> buffers = new ArrayList<>(list.size());
            for (Object element : list)
            {
                if (element == null)
                    throw new InvalidRequestException("Invalid null element in list");
                buffers.add(elementsDecoder.decode(element));
            }
            return CollectionSerializer.pack(buffers, list.size(), Server.CURRENT_VERSION);
        }
    }

    private static class SetDecoder implements Decoder
    {
        private final Decoder elementsDecoder;
        private final AbstractType<?> elementsType;

        public SetDecoder(AbstractType<?> elementsType)
        {
            elementsDecoder = decoderForType(elementsType);
            this.elementsType = elementsType;
        }

        public ByteBuffer decode(Object object) throws InvalidRequestException
        {
            if (!(object instanceof List))
                throw new InvalidRequestException(String.format(
                        "Expected a list (representing a set), but got a %s: %s", object.getClass().getSimpleName(), object));

            List list = (List) object;
            TreeSet<ByteBuffer> buffers = new TreeSet<>(elementsType);
            for (Object element : list)
            {
                if (element == null)
                    throw new InvalidRequestException("Invalid null element in set");
                buffers.add(elementsDecoder.decode(element));
            }

            if (buffers.size() != list.size())
                throw new InvalidRequestException("List representation of set contained duplicate elements");

            return CollectionSerializer.pack(buffers, buffers.size(), Server.CURRENT_VERSION);
        }
    }
}
