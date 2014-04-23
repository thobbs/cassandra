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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Static helper methods and classes for tuples.
 */
public class Tuples
{
    private static final Logger logger = LoggerFactory.getLogger(Tuples.class);

    public static class Literal implements Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            logger.info("#### preparing Tuples.Literal {} for {} receivers", this, receivers.size());
            if (elements.size() != receivers.size())
                throw new InvalidRequestException(String.format("Expected %d elements in value tuple, but got %d: %s", receivers.size(), elements.size(), this));

            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            Iterator<? extends ColumnSpecification> specIterator = receivers.iterator();
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            for (Term.Raw rt : elements)
            {
                ColumnSpecification spec = specIterator.next();
                Term t = rt.prepare(spec);

                if (t.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid tuple literal for %s: bind variables are not supported inside tuple literals", spec));

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
                types.add(spec.type);
            }
            CompositeType type = CompositeType.getInstance(types);
            DelayedValue value = new DelayedValue(values, type);
            return allTerminal ? value.bind(Collections.<ByteBuffer>emptyList()) : value;
        }

        public Term prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.Literal instances require a list of receivers for prepare()");
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return false;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < elements.size(); i++)
            {
                sb.append(elements.get(i).toString());
                if (i != elements.size() - 1)
                    sb.append(", ");
            }
            sb.append(')');
            return sb.toString();
        }
    }

    /**
     * A tuple of constant values (e.g (123, 'abc')).
     */
    public static class Value extends Term.Terminal
    {
        public final ByteBuffer[] elements;
        public final CompositeType type;

        public Value(ByteBuffer[] elements, CompositeType type)
        {
            this.elements = elements;
            this.type = type;
        }

        public static Value fromSerialized(ByteBuffer bytes, CompositeType type)
        {
            return new Value(type.split(bytes), type);
        }

        public ByteBuffer get()
        {
            logger.info("#### in Tuples.Value.get(), elements is {}, will return {}", elements, ByteBufferUtil.bytesToHex(CompositeType.build(elements)));
            return CompositeType.build(elements);
        }
    }

    public static class DelayedValue extends Term.NonTerminal
    {
        public final List<Term> elements;
        public final CompositeType type;

        public DelayedValue(List<Term> elements, CompositeType type)
        {
            this.elements = elements;
            this.type = type;
            logger.info("#### Tuples.DelayedValue elements and type is {}: {}", elements, type);
        }

        public boolean containsBindMarker()
        {
            // False since we don't support them in collection
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer[] buffers = new ByteBuffer[elements.size()];
            for (int i=0; i < elements.size(); i++)
            {
                buffers[i] = elements.get(i).bindAndGet(values);
                logger.info("#### in bind, element {} bound to {}", i, ByteBufferUtil.bytesToHex(buffers[i]));
            }
            return new Value(buffers, type);
        }
    }

    /**
     * A raw placeholder for a tuple of values for different multiple columns, each of which may have a different type.
     * For example, "SELECT ... WHERE (col1, col2) > ?'.
     *
     * Because multiple types can be used, a CompositeType is used to represent the values.
     */
    public static class Raw extends AbstractMarker.Raw
    {
        public Raw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeInReceiver(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("(");
            for (ColumnSpecification receiver : receivers)
            {
                inName.append(receiver.name);
                inName.append(",");
                types.add(receiver.type);
            }
            inName.setCharAt(inName.length() - 1, ')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            CompositeType type = CompositeType.getInstance(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, type);
        }

        public AbstractMarker prepare(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new Tuples.Marker(bindIndex, makeInReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.Raw.prepare() requires a list of receivers");
        }
    }

    public static class INRaw extends AbstractMarker.Raw
    {
        public INRaw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeInReceiver(List<ColumnSpecification> receivers) throws InvalidRequestException
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("in(");
            for (ColumnSpecification receiver : receivers)
            {
                inName.append(receiver.name);
                inName.append(",");

                if (receiver.type instanceof CollectionType)
                    throw new InvalidRequestException("Collection columns do not support IN relations");
                types.add(receiver.type);
            }
            inName.setCharAt(inName.length() - 1, ')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            CompositeType type = CompositeType.getInstance(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, ListType.getInstance(type));
        }

        public AbstractMarker prepare(List<ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new Tuples.Marker(bindIndex, makeInReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.INRaw.prepare() requires a list of receivers");
        }
    }

    public static class Marker extends AbstractMarker
    {
        public Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof ListType;
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer value = values.get(bindIndex);
            if (value == null)
                return null;

            return value == null ? null : Value.fromSerialized(value, (CompositeType)receiver.type);
        }
    }
}