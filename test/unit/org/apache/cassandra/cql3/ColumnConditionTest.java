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

    public static final ByteBuffer A = AsciiType.instance.fromString("a");
    public static final ByteBuffer B = AsciiType.instance.fromString("b");

    private static boolean isSatisfiedBy(ColumnCondition.Bound bound, ByteBuffer conditionValue, ByteBuffer columnValue) throws InvalidRequestException
    {
        Column c = columnValue == null ? null : new Column(ByteBufferUtil.bytes("c"), columnValue);
        return bound.isSatisfiedByValue(conditionValue, c, Int32Type.instance, bound.operator, 1234);
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

        return ColumnCondition.CollectionBound.listAppliesTo(ListType.getInstance(Int32Type.instance), cfm, columns == null ? null : columns.iterator(), conditionValues, bound.operator);
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

        return ColumnCondition.CollectionBound.mapAppliesTo(MapType.getInstance(Int32Type.instance, Int32Type.instance), cfm, columns.iterator(), conditionValues, bound.operator);
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

    private static ColumnCondition.Bound in(CFDefinition.Name name, ByteBuffer... values) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(values.length);
        for (ByteBuffer value : values)
            terms.add(new Constants.Value(value));
        return ColumnCondition.inCondition(name, terms).bind(Collections.EMPTY_LIST);
    }

    @Test
    public void testSimpleBoundEquality() throws InvalidRequestException
    {
        CFDefinition.Name name1 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col1", false), CFDefinition.Name.Kind.COLUMN_METADATA, Int32Type.instance);
        CFDefinition.Name name2 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col2", false), CFDefinition.Name.Kind.COLUMN_METADATA, Int32Type.instance);
        ColumnCondition condition1 = ColumnCondition.condition(name1, new Constants.Value(ONE), Relation.Type.EQ);
        ColumnCondition.Bound bound1 = condition1.bind(Collections.EMPTY_LIST);

        assertTrue(bound1.equals(bound1));

        // different column names
        ColumnCondition condition2 = ColumnCondition.condition(name2, new Constants.Value(ONE), Relation.Type.EQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // different operators
        condition2 = ColumnCondition.condition(name1, new Constants.Value(ONE), Relation.Type.NEQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // both have null values
        condition1 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertTrue(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a null value, the second does not
        condition2 = ColumnCondition.condition(name1, new Constants.Value(ONE), Relation.Type.NEQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a non-null value, the second has a null value
        condition1 = ColumnCondition.condition(name1, new Constants.Value(ONE), Relation.Type.NEQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // IN conditions
        assertTrue(in(name1, ONE).equals(in(name1, ONE)));
        assertTrue(in(name1).equals(in(name1)));

        // different values
        assertFalse(in(name1, ONE).equals(in(name1, TWO)));

        // different lengths
        assertFalse(in(name1, ONE).equals(in(name1, ONE, TWO)));
        assertFalse(in(name1, ONE, TWO).equals(in(name1, ONE)));
        assertFalse(in(name1, ONE, TWO).equals(in(name1)));

        // different orderings
        assertTrue(in(name1, ONE, TWO).equals(in(name1, TWO, ONE)));

        // nulls in the INs
        assertFalse(in(name1, ONE).equals(in(name1, new ByteBuffer[]{null})));
        assertFalse(in(name1, new ByteBuffer[]{null}).equals(in(name1, ONE)));
        assertTrue(in(name1, ONE, null).equals(in(name1, null, ONE)));

        // duplicate values
        assertTrue(in(name1, ONE, ONE).equals(in(name1, ONE)));
    }

    private static ColumnCondition.Bound elementIn(CFDefinition.Name name, ByteBuffer... values) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(values.length);
        for (ByteBuffer value : values)
            terms.add(new Constants.Value(value));
        return ColumnCondition.inCondition(name, new Constants.Value(ONE), terms).bind(Collections.EMPTY_LIST);
    }

    @Test
    public void testElementAccessBoundEquality() throws InvalidRequestException
    {
        List<CFDefinition.Name> names = Arrays.asList(
                new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col1", false), CFDefinition.Name.Kind.COLUMN_METADATA, ListType.getInstance(AsciiType.instance)),
                new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col2", false), CFDefinition.Name.Kind.COLUMN_METADATA, MapType.getInstance(Int32Type.instance, AsciiType.instance)));

        ColumnCondition condition1 = ColumnCondition.condition(names.get(0), new Constants.Value(ONE), new Constants.Value(ONE), Relation.Type.EQ);
        ColumnCondition condition2 = ColumnCondition.condition(names.get(1), new Constants.Value(ONE), new Constants.Value(ONE), Relation.Type.EQ);
        // different column names
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        for (CFDefinition.Name name : names)
        {
            ColumnCondition condition = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(A), Relation.Type.EQ);
            ColumnCondition.Bound bound = condition.bind(Collections.EMPTY_LIST);

            assertTrue(bound.equals(bound));

            // different operators
            condition2 = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(A), Relation.Type.NEQ);
            assertFalse(bound.equals(condition2.bind(Collections.EMPTY_LIST)));

            // different elements
            condition2 = ColumnCondition.condition(name, new Constants.Value(TWO), new Constants.Value(A), Relation.Type.EQ);
            assertFalse(bound.equals(condition2.bind(Collections.EMPTY_LIST)));

            // both have null values
            condition = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(null), Relation.Type.EQ);
            condition2 = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(null), Relation.Type.EQ);
            assertTrue(condition.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

            // the first has a null value, the second does not
            condition2 = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(A), Relation.Type.NEQ);
            assertFalse(condition.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

            // the first has a non-null value, the second has a null value
            condition = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(A), Relation.Type.NEQ);
            condition2 = ColumnCondition.condition(name, new Constants.Value(ONE), new Constants.Value(null), Relation.Type.EQ);
            assertFalse(condition.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

            // IN conditions
            assertTrue(elementIn(name, A).equals(elementIn(name, A)));
            assertTrue(elementIn(name).equals(elementIn(name)));

            // different values
            assertFalse(elementIn(name, A).equals(elementIn(name, B)));

            // different lengths
            assertFalse(elementIn(name, A).equals(elementIn(name, A, B)));
            assertFalse(elementIn(name, A, B).equals(elementIn(name, A)));
            assertFalse(elementIn(name, A, B).equals(elementIn(name)));

            // different orderings
            assertTrue(elementIn(name, A, B).equals(elementIn(name, B, A)));

            // nulls in the INs
            assertFalse(elementIn(name, A).equals(elementIn(name, new ByteBuffer[]{null})));
            assertFalse(elementIn(name, new ByteBuffer[]{null}).equals(elementIn(name, A)));
            assertTrue(elementIn(name, A, null).equals(elementIn(name, null, A)));

            // duplicate values
            assertTrue(elementIn(name, A, A).equals(elementIn(name, A)));
        }
    }

    private static ColumnCondition.Bound listCollectionIn(CFDefinition.Name name, List<ByteBuffer>... inValues) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(inValues.length);
        for (List<ByteBuffer> value : inValues)
            terms.add(new Lists.Value(value));
        return ColumnCondition.inCondition(name, terms).bind(Collections.EMPTY_LIST);
    }

    private static ColumnCondition.Bound listCollectionWithNullIn(CFDefinition.Name name, List<ByteBuffer>... otherInValues) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(1);
        terms.add(new Constants.Value(null));
        for (List<ByteBuffer> value : otherInValues)
            terms.add(new Lists.Value(value));
        return ColumnCondition.inCondition(name, terms).bind(Collections.EMPTY_LIST);
    }

    @Test
    public void testListCollectionBoundEquality() throws InvalidRequestException
    {
        CFDefinition.Name name1 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col1", false), CFDefinition.Name.Kind.COLUMN_METADATA, ListType.getInstance(AsciiType.instance));

        ColumnCondition condition1 = ColumnCondition.condition(name1, new Lists.Value(list(ZERO, ONE, TWO)), Relation.Type.EQ);
        ColumnCondition.Bound bound1 = condition1.bind(Collections.EMPTY_LIST);

        assertTrue(bound1.equals(bound1));

        // different column names
        CFDefinition.Name name2 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col2", false), CFDefinition.Name.Kind.COLUMN_METADATA, ListType.getInstance(AsciiType.instance));
        ColumnCondition condition2 = ColumnCondition.condition(name2, new Lists.Value(list(ZERO, ONE, TWO)), Relation.Type.EQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // different operators
        condition2 = ColumnCondition.condition(name1, new Lists.Value(list(ZERO, ONE, TWO)), Relation.Type.NEQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // both have null values
        condition1 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertTrue(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a null value, the second does not
        condition2 = ColumnCondition.condition(name1, new Lists.Value(list(ZERO, ONE, TWO)), Relation.Type.NEQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a non-null value, the second has a null value
        condition1 = ColumnCondition.condition(name1, new Lists.Value(list(ZERO, ONE, TWO)), Relation.Type.NEQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // IN conditions
        assertTrue(listCollectionIn(name1, list(ONE)).equals(listCollectionIn(name1, list(ONE))));
        assertTrue(listCollectionIn(name1, list(ZERO), list(ONE, TWO)).equals(listCollectionIn(name1, list(ZERO), list(ONE, TWO))));
        assertTrue(listCollectionIn(name1).equals(listCollectionIn(name1)));

        // different values
        assertFalse(listCollectionIn(name1, list(ONE)).equals(listCollectionIn(name1, list(TWO))));
        assertFalse(listCollectionIn(name1, list(ONE, TWO)).equals(listCollectionIn(name1, list(ONE))));

        // different IN lengths
        assertFalse(listCollectionIn(name1, list(ONE)).equals(listCollectionIn(name1, list(ONE), list(TWO))));
        assertFalse(listCollectionIn(name1, list(ONE), list(TWO)).equals(listCollectionIn(name1, list(ONE))));
        assertFalse(listCollectionIn(name1, list(ONE), list(TWO)).equals(listCollectionIn(name1)));

        // different orderings
        assertTrue(listCollectionIn(name1, list(ONE), list(TWO)).equals(listCollectionIn(name1, list(TWO), list(ONE))));

        // nulls in the INs
        assertFalse(listCollectionIn(name1, list(ONE)).equals(listCollectionWithNullIn(name1)));
        assertFalse(listCollectionWithNullIn(name1).equals(listCollectionIn(name1, list(ONE))));
        assertTrue(listCollectionWithNullIn(name1, list(ONE)).equals(listCollectionWithNullIn(name1, list(ONE))));

        // duplicate values
        assertTrue(listCollectionIn(name1, list(ONE), list(ONE)).equals(listCollectionIn(name1, list(ONE))));
    }

    private static ColumnCondition.Bound mapCollectionIn(CFDefinition.Name name, Map<ByteBuffer, ByteBuffer>... inValues) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(inValues.length);
        for (Map<ByteBuffer, ByteBuffer> value : inValues)
            terms.add(new Maps.Value(value));
        return ColumnCondition.inCondition(name, terms).bind(Collections.EMPTY_LIST);
    }

    private static ColumnCondition.Bound mapCollectionWithNullIn(CFDefinition.Name name, Map<ByteBuffer, ByteBuffer>... otherInValues) throws InvalidRequestException
    {
        List<Term> terms = new ArrayList<>(1);
        terms.add(new Constants.Value(null));
        for (Map<ByteBuffer, ByteBuffer> value : otherInValues)
            terms.add(new Maps.Value(value));
        return ColumnCondition.inCondition(name, terms).bind(Collections.EMPTY_LIST);
    }

    @Test
    public void testMapCollectionBoundEquality() throws InvalidRequestException
    {
        CFDefinition.Name name1 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col1", false), CFDefinition.Name.Kind.COLUMN_METADATA, MapType.getInstance(Int32Type.instance, AsciiType.instance));

        ColumnCondition condition1 = ColumnCondition.condition(name1, new Maps.Value(map(ZERO, A, ONE, B)), Relation.Type.EQ);
        ColumnCondition.Bound bound1 = condition1.bind(Collections.EMPTY_LIST);

        assertTrue(bound1.equals(bound1));

        // different column names
        CFDefinition.Name name2 = new CFDefinition.Name("ks", "cf", new ColumnIdentifier("col2", false), CFDefinition.Name.Kind.COLUMN_METADATA, MapType.getInstance(Int32Type.instance, AsciiType.instance));
        ColumnCondition condition2 = ColumnCondition.condition(name2, new Maps.Value(map(ZERO, A, ONE, B)), Relation.Type.EQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // different operators
        condition2 = ColumnCondition.condition(name1, new Maps.Value(map(ZERO, A, ONE, B)), Relation.Type.NEQ);
        assertFalse(bound1.equals(condition2.bind(Collections.EMPTY_LIST)));

        // both have null values
        condition1 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertTrue(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a null value, the second does not
        condition2 = ColumnCondition.condition(name1, new Maps.Value(map(ZERO, A, ONE, B)), Relation.Type.NEQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // the first has a non-null value, the second has a null value
        condition1 = ColumnCondition.condition(name1, new Maps.Value(map(ZERO, A, ONE, B)), Relation.Type.NEQ);
        condition2 = ColumnCondition.condition(name1, new Constants.Value(null), Relation.Type.EQ);
        assertFalse(condition1.bind(Collections.EMPTY_LIST).equals(condition2.bind(Collections.EMPTY_LIST)));

        // IN conditions
        assertTrue(mapCollectionIn(name1, map(ONE, A)).equals(mapCollectionIn(name1, map(ONE, A))));
        assertTrue(mapCollectionIn(name1, map(ZERO, A), map(ONE, A, TWO, B)).equals(mapCollectionIn(name1, map(ZERO, A), map(ONE, A, TWO, B))));
        assertTrue(mapCollectionIn(name1).equals(mapCollectionIn(name1)));

        // different values
        assertFalse(mapCollectionIn(name1, map(ONE, A)).equals(mapCollectionIn(name1, map(TWO, A))));
        assertFalse(mapCollectionIn(name1, map(ONE, A)).equals(mapCollectionIn(name1, map(ONE, B))));
        assertFalse(mapCollectionIn(name1, map(ONE, A, TWO, B)).equals(mapCollectionIn(name1, map(ONE, A))));

        // different IN lengths
        assertFalse(mapCollectionIn(name1, map(ONE, A)).equals(mapCollectionIn(name1, map(ONE, A), map(TWO, B))));
        assertFalse(mapCollectionIn(name1, map(ONE, A), map(TWO, B)).equals(mapCollectionIn(name1, map(ONE, A))));
        assertFalse(mapCollectionIn(name1, map(ONE, A), map(TWO, B)).equals(mapCollectionIn(name1)));

        // different orderings
        assertTrue(mapCollectionIn(name1, map(ONE, A), map(TWO, B)).equals(mapCollectionIn(name1, map(TWO, B), map(ONE, A))));

        // nulls in the INs
        List<ByteBuffer> inWithNull = new ArrayList<>();
        inWithNull.add(null);

        assertFalse(mapCollectionIn(name1, map(ONE, A)).equals(mapCollectionWithNullIn(name1)));
        assertFalse(mapCollectionWithNullIn(name1).equals(mapCollectionIn(name1, map(ONE, A))));
        assertTrue(mapCollectionWithNullIn(name1, map(ONE, A)).equals(mapCollectionWithNullIn(name1, map(ONE, A))));

        // duplicate values
        assertTrue(mapCollectionIn(name1, map(ONE, A), map(ONE, A)).equals(mapCollectionIn(name1, map(ONE, A))));
    }
}