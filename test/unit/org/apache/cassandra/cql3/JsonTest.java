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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonTest extends CQLTester
{

    @Test
    public void testToJsonFct() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "asciival ascii, " +
                "bigintval bigint, " +
                "blobval blob, " +
                "booleanval boolean, " +
                "decimalval decimal, " +
                "doubleval double, " +
                "floatval float, " +
                "inetval inet, " +
                "intval int, " +
                "textval text, " +
                "timestampval timestamp, " +
                "timeuuidval timeuuid, " +
                "uuidval uuid," +
                "varcharval varchar, " +
                "varintval varint, " +
                "listval list<int>, " +
                "setval set<uuid>, " +
                "mapval map<int, ascii>)");

        // fails JSON parsing
        assertInvalidMessage("Could not decode JSON string '\u038E\u0394\u03B4\u03E0'",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\u038E\u0394\u03B4\u03E0");

        // ================ ascii ================
        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        assertInvalidMessage("Value '\u1fff\u2013\u33B4\u2014' is not a valid ASCII string",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"\\u1fff\\u2013\\u33B4\\u2014\"");

        assertInvalidMessage("Value '123' is not a valid ASCII string",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "123");

        // ================ bigint ================
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        // floats get truncated
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.456");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123L));
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.999");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123L));

        assertInvalidMessage("Expected a long value, but got a String",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"abc\"");

        assertInvalidMessage("Expected a long value, but got a",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        // ================ blob ================
        execute("INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));

        assertInvalidMessage("Value 'xyzz' is not a valid blob representation",
            "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Value '123' is not a valid blob representation",
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "123");

        // ================ boolean ================
        execute("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));

        execute("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));

        assertInvalidMessage("Expected a boolean value, but got a String",
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "\"abc\"");
        assertInvalidMessage("Expected a boolean value, but got a Long",
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "123");

        // ================ decimal ================
        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));

        // accept strings for numbers that cannot be represented as doubles
        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));

        assertInvalidMessage("Value 'xyzz' is not a valid representation of a decimal value",
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Value 'true' is not a valid representation of a decimal value",
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "true");

        // ================ double ================
        execute("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));

        execute("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));

        assertInvalidMessage("Expected a double value, but got a String",
                "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        // ================ float ================
        execute("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));

        execute("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));

        assertInvalidMessage("Expected a float value, but got a String",
                "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        // ================ inet ================
        execute("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));

        execute("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));

        assertInvalidMessage("Value 'xyzz' is not a valid inet representation",
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a string representation of an inet value, but got a Long",
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "123");

        // ================ int ================
        execute("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        // int overflow (2 ^ 32, or Integer.MAX_INT + 1)
        execute("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "2147483648");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, -2147483648));

        // floats get truncated
        execute("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123.456");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        assertInvalidMessage("Expected an int value, but got a String",
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        // ================ text (varchar) ================
        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, ""));

        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));

        assertInvalidMessage("Value '123' is not a valid UTF-8 string",
                "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "123");

        // ================ timestamp ================
        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));

        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));

        assertInvalidMessage("Expected a long or a datestring representation of a timestamp value, but got a Double",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Error creating timestamp value",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"abcd\"");

        // ================ timeuuid ================
        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("Got invalid string representation of a timeuuid",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-0000-000000000000\"");

        assertInvalidMessage("Expected a string representation of a timeuuid, but got a Long",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "123");

         // ================ uuidval ================
        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("Got invalid string representation of a uuid",
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-zzzz-000000000000\"");

        assertInvalidMessage("Expected a string representation of a uuid, but got a Long",
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "123");

        // ================ varint ================
        execute("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));

        // accept strings for numbers that cannot be represented as longs
        execute("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));

        assertInvalidMessage("Value '123123.123' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123.123");

        assertInvalidMessage("Value 'xyzz' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Value '' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"\"");

        assertInvalidMessage("Value 'true' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "true");

        // ================ lists ================
        execute("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        execute("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a list, but got a Long",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Expected an int value, but got a String",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        assertInvalidMessage("Invalid null element in list",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[null]");

        // ================ sets ================
        execute("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        execute("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("List representation of set contained duplicate elements",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");

        assertInvalidMessage("Expected a list (representing a set), but got a Long",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Got invalid string representation of a uuid",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        assertInvalidMessage("Invalid null element in set",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[null]");

        // ================ maps ================
        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{1: \"a\", 2: \"b\"}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map(1, "a", 2, "b")));

        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a map, but got a Long",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Value '\u1fff\u2013\u33B4\u2014' is not a valid ASCII string",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{1: \"\\u1fff\\u2013\\u33B4\\u2014\"}");

        assertInvalidMessage("Invalid null key in map",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{null: \"a\"}");

        assertInvalidMessage("Invalid null value in map",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{1: null}");
    }
}
