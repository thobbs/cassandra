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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonTest extends CQLTester
{

    /*
    private void insertSingleColumn(String columnName, Object columnValue) throws Throwable
    {
        execute("INSERT INTO %s (thekey, " + columnName + ") VALUES (?, fromJson(?))", 0, columnValue);
    }
    */

    @Test
    public void testToJsonFct() throws Throwable
    {
        createTable("CREATE TABLE %s (thekey int PRIMARY KEY, a ascii, b bigint, c blob, d boolean, e decimal, " +
                                                             "f double, g float, h inet, i int, j text, k timestamp, l uuid," +
                                                             "m varchar, n varint, o timeuuid, p list<int>, q set<int>, r map<int, ascii>)");

        // fails JSON parsing
        assertInvalidMessage("Could not decode JSON string '\u038E\u0394\u03B4\u03E0'",
                "INSERT INTO %s (thekey, a) VALUES (?, fromJson(?))", 0, "\u038E\u0394\u03B4\u03E0");

        // ======== ascii ========
        execute("INSERT INTO %s (thekey, a) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT thekey, a FROM %s WHERE thekey = ?", 0), row(0, "ascii text"));

        assertInvalidMessage("Value '\u1fff\u2013\u33B4\u2014' is not a valid ASCII string",
                "INSERT INTO %s (thekey, a) VALUES (?, fromJson(?))", 0, "\"\\u1fff\\u2013\\u33B4\\u2014\"");

        assertInvalidMessage("Value '123' is not a valid ASCII string",
                "INSERT INTO %s (thekey, a) VALUES (?, fromJson(?))", 0, "123");

        // ======== bigint ========
        execute("INSERT INTO %s (thekey, b) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT thekey, b FROM %s WHERE thekey = ?", 0), row(0, 123123123123L));

        // floats get truncated
        execute("INSERT INTO %s (thekey, b) VALUES (?, fromJson(?))", 0, "123.456");
        assertRows(execute("SELECT thekey, b FROM %s WHERE thekey = ?", 0), row(0, 123L));
        execute("INSERT INTO %s (thekey, b) VALUES (?, fromJson(?))", 0, "123.999");
        assertRows(execute("SELECT thekey, b FROM %s WHERE thekey = ?", 0), row(0, 123L));

        assertInvalidMessage("Expected long, got",
                "INSERT INTO %s (thekey, b) VALUES (?, fromJson(?))", 0, "\"abc\"");

        assertInvalidMessage("Expected long, got",
                "INSERT INTO %s (thekey, b) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        // ======== blob =======
        execute("INSERT INTO %s (thekey, c) VALUES (?, fromJson(?))", 0, "\"00000001\"");
        assertRows(execute("SELECT thekey, c FROM %s WHERE thekey = ?", 0), row(0, ByteBufferUtil.bytes(1)));
    }
}
