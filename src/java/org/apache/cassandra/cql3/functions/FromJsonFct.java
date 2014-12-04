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
import org.apache.cassandra.serializers.MarshalException;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

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

    private FromJsonFct(AbstractType<?> returnType)
    {
        super(NAME, returnType, UTF8Type.instance);
    }

    public ByteBuffer execute(List<ByteBuffer> parameters, int protocolVersion) throws InvalidRequestException
    {
        assert parameters.size() == 1 : "Unexpectedly got " + parameters.size() + " arguments for fromJson()";
        ByteBuffer argument = parameters.get(0);
        String jsonArg = UTF8Type.instance.getSerializer().deserialize(argument);
        try
        {
            Object object = JSONValue.parseWithException(jsonArg);
            if (object == null)
                return null;
            return returnType.fromJSONObject(object, protocolVersion);
        }
        catch (ParseException exc)
        {
            throw new InvalidRequestException(String.format("Could not decode JSON string '%s': %s", jsonArg, exc.toString()));
        }
        catch (MarshalException exc)
        {
            throw new InvalidRequestException(exc.getMessage());
        }
    }
}
