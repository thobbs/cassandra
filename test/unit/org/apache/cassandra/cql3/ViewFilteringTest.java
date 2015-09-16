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

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import junit.framework.Assert;

import org.apache.cassandra.db.SystemKeyspace;

public class ViewFilteringTest extends CQLTester
{
    int protocolVersion = 4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    private void dropView(String name) throws Throwable
    {
        executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + name);
        views.remove(name);
    }

    @Test
    public void testMVCreationSelectRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY((a, b), c, d))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // IS NOT NULL is required on all PK statements that are not otherwise restricted
        List<String> badStatements = Arrays.asList(
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = ? AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)"
        );

        for (String badStatement : badStatements)
        {
            try
            {
                createView("mv1_test", badStatement);
                Assert.fail("Create MV statement should have failed due to missing IS NOT NULL restriction: " + badStatement);
            }
            catch (InvalidQueryException exc) {}
        }

        List<String> goodStatements = Arrays.asList(
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c > 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c = 1 AND d IN (1, 2, 3) PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) = (1, 1) PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) > (1, 1) PRIMARY KEY ((a, b), c, d)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) IN ((1, 1), (2, 2)) PRIMARY KEY ((a, b), c, d)"
        );

        for (int i = 0; i < goodStatements.size(); i++)
        {
            try
            {
                createView("mv" + i + "_test", goodStatements.get(i));
            }
            catch (Exception e)
            {
                throw new RuntimeException("MV creation failed: " + goodStatements.get(i), e);
            }

            try
            {
                executeNet(protocolVersion, "ALTER MATERIALIZED VIEW mv" + i + "_test WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
            }
            catch (Exception e)
            {
                throw new RuntimeException("MV alter failed: " + goodStatements.get(i), e);
            }
        }

        try
        {
            createView("mv_foo", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)");
            Assert.fail("Partial partition key restriction should not be allowed");
        }
        catch (InvalidQueryException exc) {}
    }

    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (\"theKey\" int, \"theClustering\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 1, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 1, 1, 0);

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"theValue\" IS NOT NULL " +
                "PRIMARY KEY (\"theKey\", \"theClustering\")");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"theValue\" FROM %%s " +
                "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"theValue\" IS NOT NULL " +
                "PRIMARY KEY (\"theKey\", \"theClustering\")");
        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRowsIgnoringOrder(execute("SELECT \"theKey\", \"theClustering\", \"theValue\" FROM " + mvname),
                    row(1, 1, 0)
            );
        }

        executeNet(protocolVersion, "ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"");

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRowsIgnoringOrder(execute("SELECT \"theKey\", \"Col\", \"theValue\" FROM " + mvname),
                    row(1, 1, 0)
            );
        }
    }

    @Test
    public void testPartitionKeyRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where a = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertEmpty(execute("SELECT * FROM mv_test" + i));
        }
    }

    @Test
    public void testCompoundPartitionKeyRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where a = 1 and b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c IS NOT NULL PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 1);
            assertEmpty(execute("SELECT * FROM mv_test" + i));
        }
    }

    @Test
    public void testCompoundPartitionKeyRestrictionsNotIncludeAll() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");
        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

        // only accept rows where a = 1 and b = 1, don't include column d in the selection
        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %%s WHERE a = 1 AND b = 1 AND c IS NOT NULL PRIMARY KEY ((a, b), c)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);

        assertRows(execute("SELECT * FROM mv_test"),
            row(1, 1, 0),
            row(1, 1, 1)
        );

        // insert new rows that do not match the filter
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 0, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 0),
                row(1, 1, 1)
        );

        // insert new row that does match the filter
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 0),
                row(1, 1, 1),
                row(1, 1, 2)
        );

        // update rows that don't match the filter
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 0, 0);
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 0),
                row(1, 1, 1),
                row(1, 1, 2)
        );

        // update a row that does match the filter
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 0),
                row(1, 1, 1),
                row(1, 1, 2)
        );

        // delete rows that don't match the filter
        execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
        execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 0, 0);
        execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
        execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 0),
                row(1, 1, 1),
                row(1, 1, 2)
        );

        // delete a row that does match the filter
        execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
        assertRows(execute("SELECT * FROM mv_test"),
                row(1, 1, 1),
                row(1, 1, 2)
        );

        // delete a partition that matches the filter
        execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 1);
        assertEmpty(execute("SELECT * FROM mv_test"));
    }

    @Test
    public void testClusteringKeyEQRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b = 1 AND c IS NOT NULL PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 2, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                row(0, 1, 0, 0),
                row(0, 1, 1, 0)
            );

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testClusteringKeySliceRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b >= 1 AND c IS NOT NULL PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, -1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0)
            );

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testClusteringKeyINRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IN (1, 2) AND c IS NOT NULL PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, -1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0),
                    row(1, 2, 1, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0),
                    row(1, 2, 1, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0),
                    row(1, 2, 1, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0),
                    row(1, 2, 1, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0),
                    row(1, 2, 1, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0)
            );

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testClusteringKeyMultiColumnRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, -1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND (b, c) >= (1, 0) PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, -1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, -1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, -1);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 0, 1),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 1, 2, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0)
            );

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testClusteringKeyFilteringRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, -1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, -1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 2, 1, 1, 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 2),
                    row(1, 2, 1, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, -1);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, -1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 2),
                    row(1, 2, 1, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(1, 0, 1, 0),
                    row(1, 2, 1, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(0, 0, 1, 0),
                    row(0, 1, 1, 0)
            );

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testPartitionKeyAndClusteringKeyFilteringRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("USE " + keyspace());
            executeNet(protocolVersion, "USE " + keyspace());

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, -1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where b = 1
            createView("mv_test" + i, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b IS NOT NULL AND c = 1 PRIMARY KEY " + mvPrimaryKeys.get(i));

            while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test" + i))
                Thread.sleep(10);

            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0)
            );

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0)
            );

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, -1, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 0, 1, 1, 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 0),
                    row(1, 2, 1, 0)
            );

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 2, 1, 1, 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 2),
                    row(1, 2, 1, 0)
            );

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, -1);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 1);
            execute("DELETE FROM %s WHERE a = ?", 0);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 1, 1, 2),
                    row(1, 2, 1, 0)
            );

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 1);
            assertRowsIgnoringOrder(execute("SELECT a, b, c, d FROM mv_test" + i),
                    row(1, 0, 1, 0),
                    row(1, 2, 1, 0)
            );

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertEmpty(execute("SELECT a, b, c, d FROM mv_test" + i));

            dropView("mv_test" + i);
            dropTable("DROP TABLE %s");
        }
    }
}
