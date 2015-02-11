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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.map.ObjectMapper;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;

public class FromJsonFct extends AbstractFunction implements ScalarFunction
{
    public static final FunctionName NAME = FunctionName.nativeFunction("fromjson");

    private static final Map<AbstractType<?>, FromJsonFct> instances = new ConcurrentHashMap<>();
    private static final List<AbstractType<?>> fromJsonArgs = Collections.<AbstractType<?>>singletonList(UTF8Type.instance);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static FromJsonFct getInstance(AbstractType<?> returnType)
    {
        FromJsonFct func = instances.get(returnType);
        if (func == null)
        {
            func = new FromJsonFct(returnType);
            instances.put(returnType, func);
        }
        return func;
    }

    private FromJsonFct(AbstractType<?> returnType)
    {
        super(NAME, fromJsonArgs, returnType);
    }

    public boolean isAggregate()
    {
        return false;
    }

    public boolean isNative()
    {
        return true;
    }

    public boolean isPure()
    {
        return true;
    }

    public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
    {
        assert parameters.size() == 1 : "Unexpectedly got " + parameters.size() + " arguments for fromJson()";
        ByteBuffer argument = parameters.get(0);
        if (argument == null)
            return null;

        String jsonArg = UTF8Type.instance.getSerializer().deserialize(argument);
        try
        {
            Object object = objectMapper.readValue(jsonArg, Object.class);
            if (object == null)
                return null;
            return returnType.fromJSONObject(object, protocolVersion);
        }
        catch (IOException exc)
        {
            throw new InvalidRequestException(String.format("Could not decode JSON string '%s': %s", jsonArg, exc.toString()));
        }
        catch (MarshalException exc)
        {
            throw new InvalidRequestException(exc.getMessage());
        }
    }
}
