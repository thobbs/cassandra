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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Static helper methods and classes for tuples.
 */
public class Tuples
{
    private static final Logger logger = LoggerFactory.getLogger(Tuples.class);

    /**
     * A raw, literal tuple.  When prepared, this will become a Tuples.Value or Tuples.DelayedValue, depending
     * on whether the tuple holds NonTerminals.
     */
    public static class Literal implements Term.MultiColumnRaw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            if (elements.size() != receivers.size())
                throw new InvalidRequestException(String.format("Expected %d elements in value tuple, but got %d: %s", receivers.size(), elements.size(), this));

            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (int i = 0; i < elements.size(); i++)
            {
                Term t = elements.get(i).prepare(receivers.get(i));
                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }
            DelayedValue value = new DelayedValue(values);
            return allTerminal ? value.bind(Collections.<ByteBuffer>emptyList()) : value;
        }

        public Term prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.Literal instances require a list of receivers for prepare()");
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            // tuples shouldn't be assignable to anything right now
            return false;
        }

        @Override
        public String toString()
        {
            return tupleToString(elements);
        }
    }

    /**
     * A tuple of terminal values (e.g (123, 'abc')).
     */
    public static class Value extends Term.InTerminal
    {
        public final ByteBuffer[] elements;

        public Value(ByteBuffer[] elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer bytes, CompositeType type)
        {
            return new Value(type.split(bytes));
        }

        public ByteBuffer get()
        {
            return CompositeType.build(elements);
        }

        public List<ByteBuffer> getElements()
        {
            return Arrays.asList(elements);
        }
    }

    /**
     * Similar to Value, but contains at least one NonTerminal, such as a non-pure functions or bind marker.
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        public final List<Term> elements;

        public DelayedValue(List<Term> elements)
        {
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            for (Term term : elements)
                if (term.containsBindMarker())
                    return true;

            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            for (Term term : elements)
                term.collectMarkerSpecification(boundNames);
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer[] buffers = new ByteBuffer[elements.size()];
            for (int i=0; i < elements.size(); i++)
            {
                ByteBuffer bytes = elements.get(i).bindAndGet(values);
                if (bytes == null)
                    throw new InvalidRequestException("Tuples may not contain null values");

                buffers[i] = elements.get(i).bindAndGet(values);
            }
            return new Value(buffers);
        }

        @Override
        public String toString()
        {
            return tupleToString(elements);
        }
    }

    /**
     * A raw placeholder for a tuple of values for different multiple columns, each of which may have a different type.
     * For example, "SELECT ... WHERE (col1, col2) > ?'.
     *
     * Because multiple types can be used, a CompositeType is used to represent the values.
     */
    public static class Raw extends AbstractMarker.Raw implements Term.MultiColumnRaw
    {
        public Raw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeReceiver(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("(");
            for (int i = 0; i < receivers.size(); i++)
            {
                ColumnSpecification receiver = receivers.get(i);
                inName.append(receiver.name);
                if (i < receivers.size() - 1)
                    inName.append(",");
                types.add(receiver.type);
            }
            inName.append(')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            CompositeType type = CompositeType.getInstance(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, type);
        }

        public AbstractMarker prepare(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new Tuples.Marker(bindIndex, makeReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.Raw.prepare() requires a list of receivers");
        }
    }

    /**
     * A raw marker for an IN list of tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class INRaw extends AbstractMarker.Raw
    {
        public INRaw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeInReceiver(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("in(");
            for (int i = 0; i < receivers.size(); i++)
            {
                ColumnSpecification receiver = receivers.get(i);
                inName.append(receiver.name);
                if (i < receivers.size() - 1)
                    inName.append(",");

                if (receiver.type instanceof CollectionType)
                    throw new InvalidRequestException("Collection columns do not support IN relations");
                types.add(receiver.type);
            }
            inName.append(')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            CompositeType type = CompositeType.getInstance(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, ListType.getInstance(type));
        }

        public AbstractMarker prepare(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new Lists.Marker(bindIndex, makeInReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.INRaw.prepare() requires a list of receivers");
        }
    }

    /**
     * Represents a marker for a single tuple, like "SELECT ... WHERE (a, b, c) > ?"
     */
    public static class Marker extends AbstractMarker
    {
        public Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer value = values.get(bindIndex);
            if (value == null)
                return null;

            return value == null ? null : Value.fromSerialized(value, (CompositeType)receiver.type);
        }
    }

    public static String tupleToString(List<?> items)
    {

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < items.size(); i++)
        {
            sb.append(items.get(i));
            if (i < items.size() - 1)
                sb.append(", ");
        }
        sb.append(')');
        return sb.toString();
    }
}