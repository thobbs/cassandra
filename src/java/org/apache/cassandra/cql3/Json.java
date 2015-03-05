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
import org.codehaus.jackson.io.JsonStringEncoder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/** Term-related classes for INSERT JSON support. */
public class Json
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final JsonStringEncoder JSON_STRING_ENCODER = new JsonStringEncoder();

    public static final ColumnIdentifier JSON_COLUMN_ID = new ColumnIdentifier("[json]", true);

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
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            // only create the AllValuesWrapper once and return it for all calls
            if (allJsonValues == null)
                allJsonValues = new AllValues(text, expectedReceivers);

            Value terminal =  allJsonValues.createColumnTerminal(receiver);

            // We got the equivalent of a null literal for this column.  Instead of returning null directly, we need
            // to return something that bind() can be called on, so we'll use an actual null literal constant.
            return terminal == null ? Constants.NULL_LITERAL.prepare(keyspace, receiver)
                                    : terminal;
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
            return new ColumnSpecification(receiver.ksName, receiver.cfName, JSON_COLUMN_ID, UTF8Type.instance);
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
        private final ColumnSpecification column;
        private final Object parsedJsonValue;

        public Value(Object parsedJsonValue, ColumnSpecification column)
        {
            this.parsedJsonValue = parsedJsonValue;
            this.column = column;
        }

        public ByteBuffer get(int protocolVersion) throws InvalidRequestException
        {
            try
            {
                return column.type.fromJSONObject(parsedJsonValue, protocolVersion);
            }
            catch (MarshalException exc)
            {
                throw new InvalidRequestException(String.format("Error decoding JSON value for %s: %s", column.name, exc.getMessage()));
            }
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
            return boundValues.createColumnTerminal(column);
        }
    }

    public static void handleCaseSensitivity(Map<String, Object> valueMap)
    {
        for (String mapKey : new ArrayList<>(valueMap.keySet()))
        {
            // if it's surrounded by quotes, remove them and preserve the case
            if (mapKey.startsWith("\"") && mapKey.endsWith("\""))
            {
                valueMap.put(mapKey.substring(1, mapKey.length() - 1), valueMap.remove(mapKey));
                continue;
            }

            // otherwise, lowercase it if needed
            String lowered = mapKey.toLowerCase(Locale.US);
            if (!mapKey.equals(lowered))
                valueMap.put(lowered, valueMap.remove(mapKey));
        }
    }

    /** Represents a full set of JSON values in an INSERT JSON statement. */
    private static class AllValues extends Term.Terminal
    {
        private final Map<ColumnIdentifier, Object> columnMap;

        public AllValues(String jsonString, Set<? extends ColumnSpecification> expectedReceivers) throws InvalidRequestException
        {
            try
            {
                Map<String, Object> valueMap = objectMapper.readValue(jsonString, Map.class);

                if (valueMap == null)
                    throw new InvalidRequestException("Got null for INSERT JSON values");

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
            catch (IOException exc)
            {
                throw new InvalidRequestException(String.format("Could not decode JSON string as a map: %s. (String was: %s)", exc.toString(), jsonString));
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

        /**
         *  Creates a JSON.Value terminal for a specific column.
         */
        Value createColumnTerminal(ColumnSpecification column)
        {
            Object parsedJsonObject = columnMap.get(column.name);
            if (parsedJsonObject == null)
                return null;

            return new Value(parsedJsonObject, column);
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
