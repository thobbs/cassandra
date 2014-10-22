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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.util.HashMap;
import java.util.Map;

public class ToJsonFct extends AbstractFunction
{
    private static final Map<AbstractType<?>, ToJsonFct> instances = new HashMap<>();

    public static synchronized ToJsonFct getInstance(AbstractType<?> fromType)
    {
        ToJsonFct func = instances.get(fromType);
        if (func == null)
        {
            func = new ToJsonFct(fromType);
            instances.put(fromType, func);
        }
        return func;
    }

    private ToJsonFct(AbstractType<?> returnType)
    {
        super("toJson", UTF8Type.instance, returnType);
    }
}
