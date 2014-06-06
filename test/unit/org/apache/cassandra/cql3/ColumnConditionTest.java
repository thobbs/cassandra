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
package org.apache.cassandra.cql3;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ColumnConditionTest
{
    public static final ByteBuffer ZERO = Int32Type.instance.fromString("0");
    public static final ByteBuffer ONE = Int32Type.instance.fromString("1");
    public static final ByteBuffer TWO = Int32Type.instance.fromString("2");

    private static boolean isSatisfiedBy(ColumnCondition.Bound bound, ByteBuffer conditionValue, ByteBuffer columnValue) throws InvalidRequestException
    {
        Column c = columnValue == null ? null : new Column(ByteBufferUtil.bytes("c"), columnValue);
        return bound.isSatisfiedByValue(conditionValue, c, Int32Type.instance, 1234);
    }

    private static void assertThrowsIRE(ColumnCondition.Bound bound, ByteBuffer conditionValue, ByteBuffer columnValue)
    {
        try
        {
            isSatisfiedBy(bound, conditionValue, columnValue);
            fail("Expected InvalidRequestException was not thrown");
        } catch (InvalidRequestException e) { }
    }

    @Test
    public void testSimpleBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        CFDefinition.Name name = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col", false), CFDefinition.Name.Kind.COLUMN_METADATA, Int32Type.instance);

        // EQ
        ColumnCondition condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.EQ);
        ColumnCondition.Bound bound = condition.bind(Collections.EMPTY_LIST);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, null, null));
        assertFalse(isSatisfiedBy(bound, ONE, null));
        assertFalse(isSatisfiedBy(bound, null, ONE));

        // NEQ
        condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.NEQ);
        bound = condition.bind(Collections.EMPTY_LIST);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, null, null));
        assertTrue(isSatisfiedBy(bound, ONE, null));
        assertTrue(isSatisfiedBy(bound, null, ONE));

        // LT
        condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.LT);
        bound = condition.bind(Collections.EMPTY_LIST);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // LTE
        condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.LTE);
        bound = condition.bind(Collections.EMPTY_LIST);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertFalse(isSatisfiedBy(bound, ZERO, ONE));
        assertTrue(isSatisfiedBy(bound, TWO, ONE));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertTrue(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // GT
        condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.GT);
        bound = condition.bind(Collections.EMPTY_LIST);
        assertFalse(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));

        // GT
        condition = ColumnCondition.condition(name, new Constants.Value(ONE), Relation.Type.GTE);
        bound = condition.bind(Collections.EMPTY_LIST);
        assertTrue(isSatisfiedBy(bound, ONE, ONE));
        assertTrue(isSatisfiedBy(bound, ZERO, ONE));
        assertFalse(isSatisfiedBy(bound, TWO, ONE));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE));
        assertFalse(isSatisfiedBy(bound, ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(isSatisfiedBy(bound, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertThrowsIRE(bound, null, ONE);
        assertFalse(isSatisfiedBy(bound, ONE, null));
    }

    private static List<ByteBuffer> list(ByteBuffer... values)
    {
        return Arrays.asList(values);
    }

    private static boolean listAppliesTo(ColumnCondition.CollectionBound bound, List<ByteBuffer> conditionValues, List<ByteBuffer> columnValues)
    {
        CFMetaData cfm = CFMetaData.compile("create table foo(a int PRIMARY KEY, b int)", "ks");
        List<Column> columns = null;
        if (columnValues != null)
        {
            columns = new ArrayList<>(columnValues.size());
            for (ByteBuffer value : columnValues)
                columns.add(value == null ? null : new Column(ByteBufferUtil.bytes("c"), value));
        }

        return bound.listAppliesTo(ListType.getInstance(Int32Type.instance), cfm, columns == null ? null : columns.iterator(), conditionValues);
    }

    @Test
    // sets use the same check as lists
    public void testListCollectionBoundAppliesTo() throws InvalidRequestException
    {
        CFDefinition.Name name = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col", false), CFDefinition.Name.Kind.COLUMN_METADATA, ListType.getInstance(Int32Type.instance));

        // EQ
        ColumnCondition condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.EQ);
        ColumnCondition.CollectionBound bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.NEQ);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.LT);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.LTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertFalse(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertTrue(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list()));

        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.GT);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertFalse(listAppliesTo(bound, list(ONE), list(ONE)));
        assertFalse(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        condition = ColumnCondition.condition(name, null, new Lists.Value(Arrays.asList(ONE)), Relation.Type.GTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list()));
        assertTrue(listAppliesTo(bound, list(ZERO), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ZERO)));
        assertTrue(listAppliesTo(bound, list(ONE), list(ONE, ONE)));
        assertFalse(listAppliesTo(bound, list(ONE, ONE), list(ONE)));
        assertTrue(listAppliesTo(bound, list(), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list()));

        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ONE)));
        assertFalse(listAppliesTo(bound, list(ONE), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(listAppliesTo(bound, list(ByteBufferUtil.EMPTY_BYTE_BUFFER), list(ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }

    // values should be a list of key, value, key, value, ...
    private static Map<ByteBuffer, ByteBuffer> map(ByteBuffer... values)
    {
        Map<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        for (int i = 0; i < values.length; i += 2)
            map.put(values[i], values[i + 1]);

        return map;
    }

    private static boolean mapAppliesTo(ColumnCondition.CollectionBound bound, Map<ByteBuffer, ByteBuffer> conditionValues, Map<ByteBuffer, ByteBuffer> columnValues)
    {
        ColumnNameBuilder builder = CompositeType.Builder.staticBuilder(CompositeType.getInstance(UTF8Type.instance, Int32Type.instance));
        CFMetaData cfm = CFMetaData.compile("create table foo(a int PRIMARY KEY, b map<int, int>)", "ks");
        List<Column> columns = null;
        if (columnValues != null)
        {
            columns = new ArrayList<>(columnValues.size());
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : columnValues.entrySet())
            {
                ColumnNameBuilder b = builder.copy();
                b.add(ByteBufferUtil.bytes("b"));
                b.add(entry.getKey());
                columns.add(new Column(b.build(), entry.getValue()));
            }
        }

        return bound.mapAppliesTo(MapType.getInstance(Int32Type.instance, Int32Type.instance), cfm, columns.iterator(), conditionValues);
    }

    @Test
    public void testMapCollectionBoundIsSatisfiedByValue() throws InvalidRequestException
    {
        CFDefinition.Name name = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("b", false), CFDefinition.Name.Kind.COLUMN_METADATA, MapType.getInstance(Int32Type.instance, Int32Type.instance));

        Map<ByteBuffer, ByteBuffer> placeholderMap = new TreeMap<>();
        placeholderMap.put(ONE, ONE);
        Maps.Value placeholder = new Maps.Value(placeholderMap);

        // EQ
        ColumnCondition condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.EQ);
        ColumnCondition.CollectionBound bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // NEQ
        condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.NEQ);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LT
        condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.LT);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // LTE
        condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.LTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertFalse(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GT
        condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.GT);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertFalse(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));

        // GTE
        condition = ColumnCondition.condition(name, null, placeholder, Relation.Type.GTE);
        bound = (ColumnCondition.CollectionBound) condition.bind(Collections.EMPTY_LIST);

        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map()));
        assertTrue(mapAppliesTo(bound, map(ZERO, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ZERO, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ZERO), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ZERO)));
        assertTrue(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ONE, TWO, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE, TWO, ONE), map(ONE, ONE)));
        assertTrue(mapAppliesTo(bound, map(), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map()));

        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ONE)));
        assertFalse(mapAppliesTo(bound, map(ONE, ONE), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        assertTrue(mapAppliesTo(bound, map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE), map(ByteBufferUtil.EMPTY_BYTE_BUFFER, ONE)));
        assertTrue(mapAppliesTo(bound, map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER), map(ONE, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
    }
}
