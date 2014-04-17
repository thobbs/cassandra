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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.IndexOperator;

import java.nio.ByteBuffer;
import java.util.*;

public abstract class MultiColumnRestriction implements Restriction
{
    public boolean isMultiColumn()
    {
        return true;
    }

    public static class EQ extends MultiColumnRestriction implements Restriction.EQ
    {
        protected Map<CFDefinition.Name, Term> valuesByName;

        private final boolean onToken;

        public EQ(Map<CFDefinition.Name, Term> valuesByName, boolean onToken)
        {
            this.valuesByName = valuesByName;
            this.onToken = onToken;
        }

        public List<ByteBuffer> values(CFDefinition.Name name, List<ByteBuffer> variables) throws InvalidRequestException
        {
            return Collections.singletonList(valuesByName.get(name).bindAndGet(variables));
        }

        public boolean isSlice()
        {
            return false;
        }

        public boolean isEQ()
        {
            return true;
        }

        public boolean isIN()
        {
            return false;
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)%s", valuesByName, onToken ? "*" : "");
        }
    }

    public static abstract class IN extends MultiColumnRestriction implements Restriction.IN
    {
        protected List<CFDefinition.Name> identifiers;

        public static MultiColumnRestriction.IN create(List<CFDefinition.Name> identifiers, List<List<Term>> values)
        {
            return new MultiColumnRestriction.IN.WithValues(identifiers, values);
        }

        public static MultiColumnRestriction.IN create(List<CFDefinition.Name> identifiers, Term value) throws InvalidRequestException
        {
            assert value instanceof Lists.Marker; // we shouldn't have got there otherwise
            return new  MultiColumnRestriction.IN.WithMarker(identifiers, (Lists.Marker)value);
        }

        public boolean isSlice()
        {
            return false;
        }

        public boolean isEQ()
        {
            return false;
        }

        public boolean isIN()
        {
            return true;
        }

        // Used when we need to know if it's a IN with just one value before we have
        // the bind variables. This is ugly and only there for backward compatiblity
        // because we used to treate IN with 1 value like an EQ and need to preserve
        // this behavior.
        public abstract boolean canHaveOnlyOneValue();

        public boolean isOnToken()
        {
            return false;
        }

        private static class WithValues extends MultiColumnRestriction.IN
        {
            private final List<List<Term>> values;

            private WithValues(List<CFDefinition.Name> identifiers, List<List<Term>> values)
            {
                this.identifiers = identifiers;
                this.values = values;
            }

            public List<ByteBuffer> values(CFDefinition.Name name, List<ByteBuffer> variables) throws InvalidRequestException
            {
                List<ByteBuffer> buffers = new ArrayList<>(values.size());
                for (List<Term> nestedValues : values)
                    for (Term value : nestedValues)
                        buffers.add(value.bindAndGet(variables));
                return buffers;
            }

            public boolean canHaveOnlyOneValue()
            {
                return values.size() == 1;
            }

            @Override
            public String toString()
            {
                return String.format("IN(%s)", values);
            }
        }

        private static class WithMarker extends MultiColumnRestriction.IN
        {
            private final Lists.Marker marker;

            private WithMarker(List<CFDefinition.Name> identifiers, Lists.Marker marker)
            {
                this.identifiers = identifiers;
                this.marker = marker;
            }

            public List<ByteBuffer> values(CFDefinition.Name name, List<ByteBuffer> variables) throws InvalidRequestException
            {
                Lists.Value lval = marker.bind(variables);
                if (lval == null)
                    throw new InvalidRequestException("Invalid null value for IN restriction");
                return lval.elements;
            }

            public boolean canHaveOnlyOneValue()
            {
                return false;
            }

            @Override
            public String toString()
            {
                return "IN ?";
            }
        }
    }

    public static class Slice extends MultiColumnRestriction implements Restriction.Slice
    {
        private Map<CFDefinition.Name, Term[]> boundsByName;
        private Map<CFDefinition.Name, boolean[]> boundInclusiveByName;
        public CFDefinition.Name finalColumnName = null;

        private final List<Term[]> bounds;
        private final List<boolean[]> boundInclusive;
        private final boolean onToken;
        public Slice(boolean onToken)
        {
            this.boundsByName = new HashMap<>();
            this.boundInclusiveByName = new HashMap<>();

            this.bounds = new ArrayList<>();
            this.boundInclusive = new ArrayList<>();
            this.onToken = onToken;
        }

        public boolean isSlice()
        {
            return true;
        }

        public boolean isEQ()
        {
            return false;
        }

        public boolean isIN()
        {
            return false;
        }

        public List<ByteBuffer> values(CFDefinition.Name name, List<ByteBuffer> variables) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isOnToken()
        {
            return onToken;
        }

        public void setFinalColumn(CFDefinition.Name name)
        {
            finalColumnName = name;
        }

        public CFDefinition.Name getFinalColumn()
        {
            return finalColumnName;
        }

        public boolean hasBound(CFDefinition.Name name, Bound b)
        {
            Term[] bounds = boundsByName.get(name);
            if (bounds == null)
                return false;
            return bounds[b.idx] != null;
        }

        public ByteBuffer bound(CFDefinition.Name name, Bound b, List<ByteBuffer> variables) throws InvalidRequestException
        {
            Term[] bounds = boundsByName.get(name);
            return bounds[b.idx].bindAndGet(variables);
        }

        public boolean isInclusive(CFDefinition.Name name, Bound b)
        {
            Term[] bounds = boundsByName.get(name);
            if (bounds == null)
                return true;
            return bounds[b.idx] == null || boundInclusiveByName.get(name)[b.idx];
        }

        public Relation.Type getRelation(CFDefinition.Name name, Bound eocBound, Bound inclusiveBound)
        {
            switch (eocBound)
            {
                case START:
                    return boundInclusiveByName.get(name)[inclusiveBound.idx] ? Relation.Type.GTE : Relation.Type.GT;
                case END:
                    return boundInclusiveByName.get(name)[inclusiveBound.idx] ? Relation.Type.LTE : Relation.Type.LT;
            }
            throw new AssertionError();
        }

        public IndexOperator getIndexOperator(CFDefinition.Name name, Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusiveByName.get(name)[b.idx] ? IndexOperator.GTE : IndexOperator.GT;
                case END:
                    return boundInclusiveByName.get(name)[b.idx] ? IndexOperator.LTE : IndexOperator.LT;
            }
            throw new AssertionError();
        }

        public void setBound(CFDefinition.Name name, Relation.Type type, Term t) throws InvalidRequestException
        {
            Bound b;
            boolean inclusive;
            switch (type)
            {
                case GT:
                    b = Bound.START;
                    inclusive = false;
                    break;
                case GTE:
                    b = Bound.START;
                    inclusive = true;
                    break;
                case LT:
                    b = Bound.END;
                    inclusive = false;
                    break;
                case LTE:
                    b = Bound.END;
                    inclusive = true;
                    break;
                default:
                    throw new AssertionError();
            }

            Term[] bounds = boundsByName.get(name);
            if (bounds == null)
            {
                bounds = new Term[2];
                boundsByName.put(name, bounds);
            }

            boolean[] boundInclusive = boundInclusiveByName.get(name);
            if (boundInclusive == null)
            {
                boundInclusive = new boolean[2];
                boundInclusiveByName.put(name, boundInclusive);
            }

            if (bounds[b.idx] != null)
                throw new InvalidRequestException(String.format(
                        "Column \"%s\" is restricted by multiple %s inequalities", name, b.toString().toLowerCase()));

            bounds[b.idx] = t;
            boundInclusive[b.idx] = inclusive;
        }

        @Override
        public String toString()
        {
            // TODO
            return "";
            /*
            return String.format("SLICE(%s %s, %s %s)%s", boundInclusive[0] ? ">=" : ">",
                    bounds[0],
                    boundInclusive[1] ? "<=" : "<",
                    bounds[1],
                    onToken ? "*" : "");
            */
        }
    }
}
