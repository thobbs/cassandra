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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static com.google.common.collect.Lists.newArrayList;

public class MultiColumnRelationTest
{
    static ClientState clientState;
    static String keyspace = "multi_column_relation_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_partition (a int PRIMARY KEY, b int)");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.compound_partition (a int, b int, c int, PRIMARY KEY ((a, b)))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering (a int, b int, c int, PRIMARY KEY (a, b))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.multiple_clustering (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))");
        clientState = ClientState.forInternalCalls();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        } catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @Test(expected=SyntaxException.class)
    public void testEmptyIdentifierTuple() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE () = (1, 2)");
    }

    @Test(expected=SyntaxException.class)
    public void testEmptyValueTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (b, c) > ()");
    }

    @Test(expected=InvalidRequestException.class)
    public void testDifferentTupleLengths() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (b, c) > (1, 2, 3)");
    }

    @Test
    public void testEmptyIN() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ()");
        assertTrue(results.isEmpty());
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionKeyInequality() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (a) > (1)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionKeyEquality() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (a) = (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testRestrictNonPrimaryKey() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (b) = (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testMixEqualityAndInequality() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) = (0) AND (b) > (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testMixMultipleInequalitiesOnSameBound() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (0) AND (b) > (1)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testClusteringColumnsOutOfOrderInInequality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (d, c, b) > (0, 0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testSkipClusteringColumnInEquality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) = (0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testSkipClusteringColumnInInequality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) > (0, 0)");
    }

    @Test
    public void testClusteringColumnEquality() throws Throwable
    {
        execute("INSERT INTO %s.single_clustering (a, b, c) VALUES (0, 0, 0)");
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) = (0)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 0);
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionAndClusteringColumnEquality() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE (a, b) = (0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testClusteringColumnsOutOfOrderInEquality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (d, c, b) = (3, 2, 1)");
    }

    @Test
    public void testSingleClusteringColumnInequality() throws Throwable
    {
        execute("INSERT INTO %s.single_clustering (a, b, c) VALUES (0, 0, 0)");
        execute("INSERT INTO %s.single_clustering (a, b, c) VALUES (0, 1, 0)");
        execute("INSERT INTO %s.single_clustering (a, b, c) VALUES (0, 2, 0)");

        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (0)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 1, 0);
        checkRow(1, results, 0, 2, 0);

        results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) >= (1)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 1, 0);
        checkRow(1, results, 0, 2, 0);

        results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) < (2)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 0);
        checkRow(1, results, 0, 1, 0);

        results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) <= (1)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 0);
        checkRow(1, results, 0, 1, 0);

        results = execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (0) AND (b) < (2)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 0);
    }

    @Test
    public void testMultipleClusteringColumnInequality() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");

        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 1)");

        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) > (0)");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(2, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) >= (0)");
        assertEquals(6, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);
        checkRow(3, results, 0, 1, 0, 0);
        checkRow(4, results, 0, 1, 1, 0);
        checkRow(5, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) > (1, 0)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 1, 1, 0);
        checkRow(1, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) >= (1, 0)");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(2, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (1, 1, 0)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) >= (1, 1, 0)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 1, 1, 0);
        checkRow(1, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) < (1)");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) <= (1)");
        assertEquals(6, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);
        checkRow(3, results, 0, 1, 0, 0);
        checkRow(4, results, 0, 1, 1, 0);
        checkRow(5, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) < (0, 1)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 0, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) <= (0, 1)");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) < (0, 1, 1)");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) <= (0, 1, 1)");
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 0) AND (b) < (1)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c) < (1, 1)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 0, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c, d) < (1, 1, 0)");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 0, 0);

        // reversed
        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) > (0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(3, results.size());
        checkRow(2, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) >= (0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(6, results.size());
        checkRow(5, results, 0, 0, 0, 0);
        checkRow(4, results, 0, 0, 1, 0);
        checkRow(3, results, 0, 0, 1, 1);
        checkRow(2, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) > (1, 0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(2, results.size());
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) >= (1, 0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(3, results.size());
        checkRow(2, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) >= (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(2, results.size());
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) < (1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(3, results.size());
        checkRow(2, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(0, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b) <= (1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(6, results.size());
        checkRow(5, results, 0, 0, 0, 0);
        checkRow(4, results, 0, 0, 1, 0);
        checkRow(3, results, 0, 0, 1, 1);
        checkRow(2, results, 0, 1, 0, 0);
        checkRow(1, results, 0, 1, 1, 0);
        checkRow(0, results, 0, 1, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) < (0, 1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 0, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c) <= (0, 1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(3, results.size());
        checkRow(2, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(0, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) < (0, 1, 1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(2, results.size());
        checkRow(1, results, 0, 0, 0, 0);
        checkRow(0, results, 0, 0, 1, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) <= (0, 1, 1) ORDER BY b DESC, c DESC, d DESC");
        checkRow(2, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(0, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 0) AND (b) < (1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c) < (1, 1) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 0, 0);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c, d) < (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 0, 0);
    }

    @Test
    public void testLiteralIn() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");

        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1, 0), (0, 1, 1))");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 0);
        checkRow(1, results, 0, 0, 1, 1);

        // same query, but reversed order for the IN values.  TODO do we need to respect the IN ordering in the results?
        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1, 1), (0, 1, 0))");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 0);
        checkRow(1, results, 0, 0, 1, 1);


        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 and (b, c) IN ((0, 1))");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 0);
        checkRow(1, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 and (b) IN ((0))");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithShortTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithLongTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1, 2, 3, 4))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithPartitionKey() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (a, b, c, d) IN ((0, 1, 2, 3))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInSkipsClusteringColumn() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (c, d) IN ((0, 1))");
    }
    @Test
    public void testPartitionAndClusteringInClauses() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");

        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (1, 0, 1, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (1, 0, 1, 1)");

        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a IN (0, 1) AND (b, c, d) IN ((0, 1, 0), (0, 1, 1))");
        assertEquals(4, results.size());
        checkRow(0, results, 0, 0, 1, 0);
        checkRow(1, results, 0, 0, 1, 1);
        checkRow(2, results, 1, 0, 1, 0);
        checkRow(3, results, 1, 0, 1, 1);

        // same query, but reversed order for the IN values.  TODO do we need to respect the IN ordering in the results?
        results = execute("SELECT * FROM %s.multiple_clustering WHERE a IN (1, 0) AND (b, c, d) IN ((0, 1, 1), (0, 1, 0))");
        assertEquals(4, results.size());
        checkRow(0, results, 1, 0, 1, 0);
        checkRow(1, results, 1, 0, 1, 1);
        checkRow(2, results, 0, 0, 1, 0);
        checkRow(3, results, 0, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a IN (0, 1) and (b, c) IN ((0, 1))");
        assertEquals(4, results.size());
        checkRow(0, results, 0, 0, 1, 0);
        checkRow(1, results, 0, 0, 1, 1);
        checkRow(2, results, 1, 0, 1, 0);
        checkRow(3, results, 1, 0, 1, 1);

        results = execute("SELECT * FROM %s.multiple_clustering WHERE a IN (0, 1) and (b) IN ((0))");
        assertEquals(6, results.size());
        checkRow(0, results, 0, 0, 0, 0);
        checkRow(1, results, 0, 0, 1, 0);
        checkRow(2, results, 0, 0, 1, 1);
        checkRow(3, results, 1, 0, 0, 0);
        checkRow(4, results, 1, 0, 1, 0);
        checkRow(5, results, 1, 0, 1, 1);
    }

    private static void checkRow(int rowIndex, UntypedResultSet results, Integer... expectedValues)
    {
        List<UntypedResultSet.Row> rows = newArrayList(results.iterator());
        UntypedResultSet.Row row = rows.get(rowIndex);
        Iterator<ColumnSpecification> columns = row.getColumns().iterator();
        for (Integer expected : expectedValues)
            assertEquals((long)expected, row.getInt(columns.next().name.toString()));
    }
}