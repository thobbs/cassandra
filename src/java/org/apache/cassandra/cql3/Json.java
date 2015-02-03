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

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.nio.ByteBuffer;
import java.util.*;

/** Term-related classes for INSERT JSON support. */
public class Json
{

    public interface Raw extends Term.Raw
    {
        public void setExpectedReceivers(Set<? extends ColumnSpecification> receivers);
    }

    /**
     * Represents a literal JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable (key, col) JSON '{"key": 0, "col": 0}';
     */
    public static class Literal implements Raw
    {
        private AllValues allJsonValues;
        private final String text;
        private Set<? extends ColumnSpecification> expectedReceivers;

        public Literal(String text)
        {
            this.text = text;
        }

        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return TestResult.NOT_ASSIGNABLE;
        }

        /**
         * prepare() has somewhat unique behavior for JSON values.  Although this literal represents the values
         * for multiple receivers, prepare() returns a Terminal for the single, specific receiver that's passed.
         * Callers should call prepare() on this once for each receiver in the INSERT statement.
         */
        @Override
        public Value prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            // only create the AllValuesWrapper once and return it for all calls
            if (allJsonValues == null)
                allJsonValues = new AllValues(text, expectedReceivers);

            return new Value(allJsonValues, receiver);
        }

        public void setExpectedReceivers(Set<? extends ColumnSpecification> expectedReceivers)
        {
            this.expectedReceivers = expectedReceivers;
        }
    }

    /**
     * Represents a marker for a JSON string in an INSERT JSON statement.
     * For example: INSERT INTO mytable (key, col) JSON ?;
     */
    public static class Marker extends AbstractMarker.Raw implements Raw
    {
        private DelayedAllValues allJsonValues;
        private Set<? extends ColumnSpecification> expectedReceivers;

        public Marker(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeReceiver(ColumnSpecification receiver) throws InvalidRequestException
        {
            ColumnIdentifier identifier = new ColumnIdentifier("json_values", true);
            return new ColumnSpecification(receiver.ksName, receiver.cfName, identifier, UTF8Type.instance);
        }

        /**
         * See the comment on Literal.prepare().  The behavior here is the same, except that a NonTerminal will be
         * returned.
         */
        @Override
        public DelayedValue prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            // only create the AllValuesWrapper once and return it for all calls
            if (allJsonValues == null)
                allJsonValues = new DelayedAllValues(bindIndex, makeReceiver(receiver), expectedReceivers);

            return new DelayedValue(allJsonValues, receiver);
        }


        public void setExpectedReceivers(Set<? extends ColumnSpecification> expectedReceivers)
        {
            this.expectedReceivers = expectedReceivers;
        }
    }

    /**
     * A Terminal for a single column.
     */
    public static class Value extends Term.Terminal
    {
        private final AllValues allJsonValues;
        private final ColumnSpecification column;

        public Value(AllValues allJsonValues, ColumnSpecification column)
        {
            this.allJsonValues = allJsonValues;
            this.column = column;
        }

        public ByteBuffer get(int protocolVersion) throws InvalidRequestException
        {
            // get the specific value for this column from the set of all json values
            return allJsonValues.getColumnValue(column, protocolVersion);
        }
    }

    /**
     * A NonTerminal for a single column.
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        private final DelayedAllValues allJsonValues;
        private final ColumnSpecification column;

        public DelayedValue(DelayedAllValues allJsonValues, ColumnSpecification column)
        {
            this.allJsonValues = allJsonValues;
            this.column = column;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            allJsonValues.collectMarkerSpecification(boundNames);
        }

        public boolean containsBindMarker()
        {
            return allJsonValues.containsBindMarker();
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            AllValues boundValues = allJsonValues.bind(options);
            return new Value(boundValues, column);
        }
    }

    private static void handleCaseSensitivity(Map<String, Object> valueMap)
    {
        List<String> toLowercase = new ArrayList<>();
        List<String> toDequote = new ArrayList<>();
        for (String mapKey : valueMap.keySet())
        {
            String lowered = mapKey.toLowerCase(Locale.US);
            if (mapKey.startsWith("\"") && mapKey.endsWith("\""))
                toDequote.add(mapKey);
            else if (!mapKey.equals(lowered))
                toLowercase.add(mapKey);
        }

        for (String quoted : toDequote)
            valueMap.put(quoted.substring(1, quoted.length() - 1), valueMap.remove(quoted));

        for (String uppercase : toLowercase)
            valueMap.put(uppercase.toLowerCase(Locale.US), valueMap.remove(uppercase));
    }

    /** Represents a full set of JSON values in an INSERT JSON statement. */
    private static class AllValues extends Term.Terminal
    {
        private final Map<ColumnIdentifier, Object> columnMap;

        public AllValues(String jsonString, Set<? extends ColumnSpecification> expectedReceivers) throws InvalidRequestException
        {
            try
            {
                Object object = JSONValue.parseWithException(jsonString);
                if (object == null)
                    throw new InvalidRequestException("Got null for INSERT JSON values");

                if (!(object instanceof Map))
                    throw new InvalidRequestException(String.format(
                            "Expected a map for INSERT JSON values, but got a %s: %s", object.getClass().getSimpleName(), object));

                Map<String, Object> valueMap = (Map<String, Object>)object;
                handleCaseSensitivity(valueMap);

                columnMap = new HashMap<>(expectedReceivers.size());
                for (ColumnSpecification spec : expectedReceivers)
                {
                    Object parsedJsonObject = valueMap.remove(spec.name.toString());
                    if (parsedJsonObject == null)
                        columnMap.put(spec.name, null);
                    else
                        columnMap.put(spec.name, parsedJsonObject);
                }

                if (!valueMap.isEmpty())
                {
                    throw new InvalidRequestException(String.format(
                            "JSON values map contains unrecognized column: %s", valueMap.keySet().iterator().next()));
                }
            }
            catch (ParseException exc)
            {
                throw new InvalidRequestException(String.format("Could not decode JSON string '%s': %s", jsonString, exc.toString()));
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(exc.getMessage());
            }
        }

        public ByteBuffer get(int protocolVersion)
        {
            throw new AssertionError("AllValues.get() should not be called directly");
        }

        /** Get the bound value for a specific column in the JSON map of values. */
        public ByteBuffer getColumnValue(ColumnSpecification column, int protocolVersion) throws InvalidRequestException
        {
            Object parsedJsonObject = columnMap.get(column.name);
            if (parsedJsonObject == null)
                return null;

            try
            {
                return column.type.fromJSONObject(parsedJsonObject, protocolVersion);
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Error decoding JSON value for %s: %s", column.name, exc.getMessage()));
            }
        }
    }

    /** Like AllValues, but NonTerminal because a marker was used (e.g. "INSERT INTO ... JSON ?;"). */
    private static class DelayedAllValues extends AbstractMarker
    {
        // Because multiple single-column DelayedValue objects will point to this, collectMarkerSpecification() and
        // bind() may be called multiple times.  Keep track of this to avoid repeating this work.
        private AllValues boundValues;
        private boolean haveCollectedMarkerSpecs = false;
        private final Set<? extends ColumnSpecification> expectedReceivers;

        public DelayedAllValues(int bindIndex, ColumnSpecification receiver, Set<? extends ColumnSpecification> expectedReceivers)
        {
            super(bindIndex, receiver);
            this.expectedReceivers = expectedReceivers;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            if (!haveCollectedMarkerSpecs)
            {
                super.collectMarkerSpecification(boundNames);
                haveCollectedMarkerSpecs = true;
            }
        }

        public AllValues bind(QueryOptions options) throws InvalidRequestException
        {
            if (boundValues != null)
                return boundValues;

            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            boundValues = new AllValues(UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
            return boundValues;
        }
    }
}
