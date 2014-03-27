package org.apache.cassandra.cql3;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.apache.cassandra.SchemaLoader;

import org.junit.Test;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

public class AliasesTest extends SchemaLoader
{
    @Test
    public void testDefaultAliases()
    {
        processInternal("INSERT INTO \"Keyspace1\".\"MixedTypes\" (key, column1, value) " +
                        "VALUES (c4646502-b5ed-11e3-9a05-e0b9a54a6d93, 123, true)");
    }

    @Test
    public void testDefaultAliasesWithComposites()
    {
        processInternal("INSERT INTO \"Keyspace1\".\"MixedTypesComposite\" (key, key2, key3, column1, column2, column3, value) " +
                        "VALUES (0x1234, c4646502-b5ed-11e3-9a05-e0b9a54a6d93, 123, 0xabcd, c4646502-b5ed-11e3-9a05-e0b9a54a6d93, 456, true)");
    }
}
