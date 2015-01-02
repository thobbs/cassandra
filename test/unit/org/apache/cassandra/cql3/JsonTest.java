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
    public void testFromJsonFct() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");
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
                "frozenlistval frozen<list<int>>, " +
                "setval set<uuid>, " +
                "frozensetval frozen<set<uuid>>, " +
                "mapval map<ascii, int>," +
                "frozenmapval frozen<map<ascii, int>>," +
                "tupleval frozen<tuple<int, ascii, uuid>>," +
                "udtval frozen<" + typeName + ">)");


        // fromJson() can only be used when the receiver type is known
        assertInvalidMessage("fromJson() cannot be used in the selection clause", "SELECT fromJson(asciival) FROM %s", 0, 0);

        // fails JSON parsing
        assertInvalidMessage("Could not decode JSON string '\u038E\u0394\u03B4\u03E0'",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\u038E\u0394\u03B4\u03E0");

        // ================ ascii ================
        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        assertInvalidMessage("Invalid ASCII character in string literal",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"\\u1fff\\u2013\\u33B4\\u2014\"");

        assertInvalidMessage("Expected an ascii string, but got a Long",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "123");

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));
        execute("UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        execute("DELETE FROM %s WHERE k = fromJson(?)", "0");

        // ================ bigint ================
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        // floats get truncated
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.456");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123L));
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.999");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123L));

        assertInvalidMessage("Expected a bigint value, but got a String",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"abc\"");

        assertInvalidMessage("Expected a bigint value, but got a",
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

        assertInvalidMessage("Unable to make inet address from 'xyzz'",
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

        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        assertInvalidMessage("Expected a UTF-8 string, but got a Long",
                "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "123");

        // ================ timestamp ================
        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));

        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));

        assertInvalidMessage("Expected a long or a datestring representation of a timestamp value, but got a Double",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to coerce 'abcd' to a formatted date",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"abcd\"");

        // ================ timeuuid ================
        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("TimeUUID supports only version 1 UUIDs",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-0000-000000000000\"");

        assertInvalidMessage("Expected a string representation of a timeuuid, but got a Long",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "123");

         // ================ uuidval ================
        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("Unable to make UUID from",
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

        // frozen
        execute("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

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

        assertInvalidMessage("Unable to make UUID from",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        assertInvalidMessage("Invalid null element in set",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[null]");

        // frozen
        execute("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        execute("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        // ================ maps ================
        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a map, but got a Long",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Invalid ASCII character in string literal",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"\\u1fff\\u2013\\u33B4\\u2014\": 1}");

        assertInvalidMessage("Invalid null value in map",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": null}");

        // frozen
        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 1}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        // ================ tuples ================
        execute("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
            row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        execute("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        assertInvalidMessage("Tuple contains extra items",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\", 1, 2, 3]");

        assertInvalidMessage("Tuple is missing items",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\"]");

        assertInvalidMessage("Expected an int value, but got a String",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[\"not an int\", \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");

        // ================ UDTs ================
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // order of fields shouldn't matter
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test nulls
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test missing fields
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );

        assertInvalidMessage("Unknown field", "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"xxx\": 1}");
        assertInvalidMessage("Expected an int value, but got a String",
                "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": \"foobar\"}");
    }

    @Test
    public void testToJsonFct() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");
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
                "frozenlistval frozen<list<int>>, " +
                "setval set<uuid>, " +
                "frozensetval frozen<set<uuid>>, " +
                "mapval map<ascii, int>, " +
                "frozenmapval frozen<map<ascii, int>>, " +
                "tupleval frozen<tuple<int, ascii, uuid>>," +
                "udtval frozen<" + typeName + ">)");

        // toJson() can only be used in selections
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "INSERT INTO %s (k, asciival) VALUES (?, toJson(?))", 0, 0);
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "UPDATE %s SET asciival = toJson(?) WHERE k = ?", 0, 0);
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "DELETE FROM %s WHERE k = fromJson(toJson(?))", 0);

        // ================ ascii ================
        execute("INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "ascii text");
        assertRows(execute("SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), row(0, "\"ascii text\""));

        execute("INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "");
        assertRows(execute("SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), row(0, "\"\""));

        // ================ bigint ================
        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 123123123123L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "123123123123"));

        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 0L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, -123123123123L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "-123123123123"));

        // ================ blob ================
        execute("INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, ByteBufferUtil.bytes(1));
        assertRows(execute("SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), row(0, "\"00000001\""));

        execute("INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertRows(execute("SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), row(0, "\"\""));

        // ================ boolean ================
        execute("INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, true);
        assertRows(execute("SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), row(0, "true"));

        execute("INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, false);
        assertRows(execute("SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), row(0, "false"));

        // ================ decimal ================
        execute("INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, new BigDecimal("123123.123123"));
        assertRows(execute("SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), row(0, "\"123123.123123\""));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, new BigDecimal("-1.23E-12"));
        assertRows(execute("SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), row(0, "\"-1.23E-12\""));

        // ================ double ================
        execute("INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123.123123d);
        assertRows(execute("SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), row(0, "123123.123123"));

        execute("INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123d);
        assertRows(execute("SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), row(0, "123123.0"));

        // ================ float ================
        execute("INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123.123f);
        assertRows(execute("SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), row(0, "123.123"));

        execute("INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123123f);
        assertRows(execute("SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), row(0, "123123.0"));

        // ================ inet ================
        execute("INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, InetAddress.getByName("127.0.0.1"));
        assertRows(execute("SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), row(0, "\"127.0.0.1\""));

        execute("INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, InetAddress.getByName("::1"));
        assertRows(execute("SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), row(0, "\"0:0:0:0:0:0:0:1\""));

        // ================ int ================
        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 123123);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "123123"));

        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, -123123);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "-123123"));

        // ================ text (varchar) ================
        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"\""));

        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "abcd");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"abcd\""));

        // ================ timestamp ================
        execute("INSERT INTO %s (k, timestampval) VALUES (?, ?)", 0, new SimpleDateFormat("y-M-d").parse("2014-01-01"));
        assertRows(execute("SELECT k, toJson(timestampval) FROM %s WHERE k = ?", 0), row(0, "\"2014-01-01 00:00:00.000\""));

        // ================ timeuuid ================
        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, ?)", 0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(timeuuidval) FROM %s WHERE k = ?", 0), row(0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""));

         // ================ uuidval ================
        execute("INSERT INTO %s (k, uuidval) VALUES (?, ?)", 0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(uuidval) FROM %s WHERE k = ?", 0), row(0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""));

        // ================ varint ================
        execute("INSERT INTO %s (k, varintval) VALUES (?, ?)", 0, new BigInteger("123123123123123123123"));
        assertRows(execute("SELECT k, toJson(varintval) FROM %s WHERE k = ?", 0), row(0, "\"123123123123123123123\""));

        // ================ lists ================
        execute("INSERT INTO %s (k, listval) VALUES (?, ?)", 0, list(1, 2, 3));
        assertRows(execute("SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), row(0, "[1, 2, 3]"));

        execute("INSERT INTO %s (k, listval) VALUES (?, ?)", 0, list());
        assertRows(execute("SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozenlistval) VALUES (?, ?)", 0, list(1, 2, 3));
        assertRows(execute("SELECT k, toJson(frozenlistval) FROM %s WHERE k = ?", 0), row(0, "[1, 2, 3]"));

        // ================ sets ================
        execute("INSERT INTO %s (k, setval) VALUES (?, ?)",
                0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, toJson(setval) FROM %s WHERE k = ?", 0),
                row(0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        execute("INSERT INTO %s (k, setval) VALUES (?, ?)", 0, set());
        assertRows(execute("SELECT k, toJson(setval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozensetval) VALUES (?, ?)",
                0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, toJson(frozensetval) FROM %s WHERE k = ?", 0),
                row(0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        // ================ maps ================
        execute("INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, map("a", 1, "b", 2));
        assertRows(execute("SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), row(0, "{\"a\": 1, \"b\": 2}"));

        execute("INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, map());
        assertRows(execute("SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, ?)", 0, map("a", 1, "b", 2));
        assertRows(execute("SELECT k, toJson(frozenmapval) FROM %s WHERE k = ?", 0), row(0, "{\"a\": 1, \"b\": 2}"));

        // ================ tuples ================
        execute("INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
            row(0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        execute("INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, tuple(1, "foobar", null));
        assertRows(execute("SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
                row(0, "[1, \"foobar\", null]")
        );

        // ================ UDTs ================
        execute("INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?, c: ?})", 0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("foo", "bar"));
        assertRows(execute("SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                row(0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"bar\", \"foo\"]}")
        );

        execute("INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?})", 0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                row(0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": null}")
        );
    }

    @Test
    public void testSelectJsonSyntax() throws Throwable
    {
        // tests SELECT JSON statements
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");

        assertRows(execute("SELECT JSON * FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON k, v FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON v, k FROM %s"),
                row("{\"v\": 0, \"k\": 0}"),
                row("{\"v\": 1, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON v as foo, k as bar FROM %s"),
                row("{\"foo\": 0, \"bar\": 0}"),
                row("{\"foo\": 1, \"bar\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v), k FROM %s"),
                row("{\"ttl(v)\": null, \"k\": 0}"),
                row("{\"ttl(v)\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v) as foo, k FROM %s"),
                row("{\"foo\": null, \"k\": 0}"),
                row("{\"foo\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON count(*) FROM %s"),
                row("{\"count\": 2}")
        );

        assertRows(execute("SELECT JSON count(*) as foo FROM %s"),
                row("{\"foo\": 2}")
        );

        assertRows(execute("SELECT JSON toJson(blobAsInt(intAsBlob(v))) FROM %s LIMIT 1"),
                row("{\"system.tojson(system.blobasint(system.intasblob(v)))\": \"0\"}")
        );
    }

    @Test
    public void testInsertJsonSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s (k, v) JSON ?", "{\"k\": 0, \"v\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0)
        );

        execute("INSERT INTO %s (k, v) JSON ?", "{\"k\": 0, \"v\": null}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        execute("INSERT INTO %s (k, v) JSON ?", "{\"v\": 1, \"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 1)
        );

        execute("INSERT INTO %s (k, v) JSON ?", "{\"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s (k, v) JSON ?", new Object[]{null});
        assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s (k, v) JSON ?", "null");
        assertInvalidMessage("Expected a map", "INSERT INTO %s (k, v) JSON ?", "\"notamap\"");
        assertInvalidMessage("Expected a map", "INSERT INTO %s (k, v) JSON ?", "12.34");
        assertInvalidMessage("JSON values map contains unrecognized column",
                "INSERT INTO %s (k, v) JSON ?",
                "{\"k\": 0, \"v\": 0, \"zzz\": 0}");

        assertInvalidMessage("Expected an int value, but got a String",
                "INSERT INTO %s (k, v) JSON ?",
                "{\"k\": 0, \"v\": \"notanint\"}");
    }

    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, \"Foo\" int)");
        execute("INSERT INTO %s (k, \"Foo\") JSON ?", "{\"k\": 0, \"\\\"Foo\\\"\": 0}");
        execute("INSERT INTO %s (k, \"Foo\") JSON ?", "{\"\\\"k\\\"\": 0, \"\\\"Foo\\\"\": 0}");

        // results should preserve and quote case-sensitive identifiers
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"\\\"Foo\\\"\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as foo FROM %s"), row("{\"k\": 0, \"foo\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as \"Bar\" FROM %s"), row("{\"k\": 0, \"\\\"Bar\\\"\": 0}"));

        assertInvalid("INSERT INTO %s (k, \"Foo\") JSON ?", "{\"k\": 0, \"foo\": 0}");
        assertInvalid("INSERT INTO %s (k, \"Foo\") JSON ?", "{\"k\": 0, \"\\\"foo\\\"\": 0}");
    }
}
