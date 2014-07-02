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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import static com.google.common.collect.Lists.newArrayList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CQL3 condition.
 */
public class ColumnCondition
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnCondition.class);

    public final CFDefinition.Name column;

    // For collection, when testing the equality of a specific element, null otherwise.
    private final Term collectionElement;

    private final Term value; // a single value or a marker for a list of IN values
    private final List<Term> inValues;

    public final Relation.Type operator;

    private ColumnCondition(CFDefinition.Name column, Term collectionElement, Term value, List<Term> inValues, Relation.Type op)
    {
        this.column = column;
        this.collectionElement = collectionElement;
        this.value = value;
        this.inValues = inValues;
        this.operator = op;

        if (!operator.equals(Relation.Type.IN))
            assert inValues == null;
    }

    public static ColumnCondition condition(CFDefinition.Name column, Term value, Relation.Type op)
    {
        return new ColumnCondition(column, null, value, null, op);
    }

    public static ColumnCondition condition(CFDefinition.Name column, Term collectionElement, Term value, Relation.Type op)
    {
        return new ColumnCondition(column, collectionElement, value, null, op);
    }

    public static ColumnCondition inCondition(CFDefinition.Name column, List<Term> inValues)
    {
        return new ColumnCondition(column, null, null, inValues, Relation.Type.IN);
    }

    public static ColumnCondition inCondition(CFDefinition.Name column, Term collectionElement, List<Term> inValues)
    {
        return new ColumnCondition(column, collectionElement, null, inValues, Relation.Type.IN);
    }

    public static ColumnCondition inCondition(CFDefinition.Name column, Term inMarker)
    {
        return new ColumnCondition(column, null, inMarker, null, Relation.Type.IN);
    }

    public static ColumnCondition inCondition(CFDefinition.Name column, Term collectionElement, Term inMarker)
    {
        return new ColumnCondition(column, collectionElement, inMarker, null, Relation.Type.IN);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (collectionElement != null)
            collectionElement.collectMarkerSpecification(boundNames);

        if (operator.equals(Relation.Type.IN) && inValues != null)
        {
            for (Term value : inValues)
                value.collectMarkerSpecification(boundNames);
        }
        else
        {
            value.collectMarkerSpecification(boundNames);
        }
    }

    public ColumnCondition.Bound bind(List<ByteBuffer> variables) throws InvalidRequestException
    {
        boolean isInCondition = operator.equals(Relation.Type.IN);
        if (column.type instanceof CollectionType)
        {
            if (collectionElement == null)
                return isInCondition ? new CollectionInBound(this, variables) : new CollectionBound(this, variables);
            else
                return isInCondition ? new ElementAccessInBound(this, variables) : new ElementAccessBound(this, variables);
        }
        return isInCondition ? new SimpleInBound(this, variables) : new SimpleBound(this, variables);
    }

    public static abstract class Bound
    {
        public final CFDefinition.Name column;
        public final Relation.Type operator;

        protected Bound(CFDefinition.Name column, Relation.Type operator)
        {
            this.column = column;
            this.operator = operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException;

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        protected ColumnNameBuilder copyOrUpdatePrefix(CFMetaData cfm, ColumnNameBuilder rowPrefix)
        {
            return column.kind == CFDefinition.Name.Kind.STATIC ? cfm.getStaticColumnNameBuilder() : rowPrefix.copy();
        }

        public ByteBuffer getSimpleColumnName(ColumnNameBuilder rowPrefix, ColumnFamily current)
        {
            ColumnNameBuilder prefix = copyOrUpdatePrefix(current.metadata(), rowPrefix);
            return column.kind == CFDefinition.Name.Kind.VALUE_ALIAS
                    ? prefix.build()
                    : prefix.add(column.name.key).build();
        }

        protected boolean isSatisfiedByValue(ByteBuffer value, Column c, AbstractType<?> type, Relation.Type operator, long now) throws InvalidRequestException
        {
            ByteBuffer columnValue = (c == null || !c.isLive(now)) ? null : c.value();
            return compareWithOperator(operator, type, value, columnValue);
        }

        protected boolean compareWithOperator(Relation.Type operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue) throws InvalidRequestException
        {
            if (value == null)
            {
                switch (operator)
                {
                    case EQ:
                        return otherValue == null;
                    case NEQ:
                        return otherValue != null;
                    default:
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                }
            }
            else if (otherValue == null)
            {
                // the condition value is not null, so only NEQ can return true
                return operator.equals(Relation.Type.NEQ);
            }

            int comparison = type.compare(otherValue, value);
            switch (operator)
            {
                case EQ:
                    return comparison == 0;
                case LT:
                    return comparison < 0;
                case LTE:
                    return comparison <= 0;
                case GT:
                    return comparison > 0;
                case GTE:
                    return comparison >= 0;
                case NEQ:
                    return comparison != 0;
                default:
                    throw new AssertionError();
            }
        }

        protected Iterator<Column> collectionColumns(ColumnNameBuilder collectionPrefix, ColumnFamily cf, final long now)
        {
            // We are testing for collection equality, so we need to have the expected values *and* only those.
            ColumnSlice[] collectionSlice = new ColumnSlice[]{ new ColumnSlice(collectionPrefix.build(), collectionPrefix.buildAsEndOfRange()) };
            // Filter live columns, this makes things simpler afterwards
            return Iterators.filter(cf.iterator(collectionSlice), new Predicate<Column>()
            {
                public boolean apply(Column c)
                {
                    // we only care about live columns
                    return c.isLive(now);
                }
            });
        }

        protected static boolean inValuesAreEqual(List<ByteBuffer> thisValues, List<ByteBuffer> thatValues, final AbstractType<?> columnType)
        {
            Comparator<ByteBuffer> comparator = new Comparator<ByteBuffer>() {
                @Override
                public int compare(ByteBuffer o1, ByteBuffer o2) {
                    if (o1 == null)
                        return o2 == null ? 0 : -1;
                    if (o2 == null)
                        return 1;

                    return columnType.compare(o1, o2);
                }
            };

            TreeSet<ByteBuffer> thisSet = new TreeSet<>(comparator);
            thisSet.addAll(thisValues);

            TreeSet<ByteBuffer> thatSet = new TreeSet<>(comparator);
            thatSet.addAll(thatValues);

            return thisSet.equals(thatSet);
        }
    }

    private static class SimpleBound extends Bound
    {
        public final ByteBuffer value;

        private SimpleBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert !condition.operator.equals(Relation.Type.IN);
            this.value = condition.value.bindAndGet(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            return isSatisfiedByValue(value, current.getColumn(getSimpleColumnName(rowPrefix, current)), column.type, operator, now);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof SimpleBound))
                return false;

            SimpleBound that = (SimpleBound)o;
            if (!column.equals(that.column))
                return false;

            if (!operator.equals(that.operator))
                return false;

            return value == null || that.value == null
                   ? value == null && that.value == null
                   : column.type.compare(value, that.value) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, value, operator);
        }
    }

    private static class SimpleInBound extends Bound
    {
        public final List<ByteBuffer> inValues;

        private SimpleInBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert condition.operator.equals(Relation.Type.IN);
            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(variables).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(variables));
            }
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            Column col = current.getColumn(getSimpleColumnName(rowPrefix, current));
            for (ByteBuffer value : inValues)
            {
                if (isSatisfiedByValue(value, col, column.type, Relation.Type.EQ, now))
                    return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof SimpleInBound))
                return false;

            SimpleInBound that = (SimpleInBound)o;
            if (!column.equals(that.column))
                return false;

            return Bound.inValuesAreEqual(this.inValues, that.inValues, column.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, inValues, operator);
        }
    }

    private static class ElementAccessBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final ByteBuffer value;

        private ElementAccessBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            assert !condition.operator.equals(Relation.Type.IN);
            this.collectionElement = condition.collectionElement.bindAndGet(variables);
            this.value = condition.value.bindAndGet(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(current.metadata(), rowPrefix).add(column.name.key);
            if (column.type instanceof MapType)
            {
                Column item = current.getColumn(collectionPrefix.add(collectionElement).build());
                return isSatisfiedByValue(value, item, ((MapType) column.type).values, operator, now);
            }

            assert column.type instanceof ListType;
            ByteBuffer columnValue = getListItem(collectionColumns(collectionPrefix, current, now), getListIndex(collectionElement));
            return compareWithOperator(operator, ((ListType) column.type).elements, value, columnValue);
        }

        static int getListIndex(ByteBuffer collectionElement) throws InvalidRequestException
        {
            int idx = ByteBufferUtil.toInt(collectionElement);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Invalid negative list index %d", idx));
            return idx;
        }

        static ByteBuffer getListItem(Iterator<Column> iter, int index) throws InvalidRequestException
        {
            int adv = Iterators.advance(iter, index);
            if (adv == index && iter.hasNext())
                return iter.next().value();
            else
                return null;
        }

        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof ElementAccessBound))
                return false;

            ElementAccessBound that = (ElementAccessBound)o;
            if (!column.equals(that.column))
                return false;

            if (!operator.equals(that.operator))
                return false;

            if ((collectionElement == null) != (that.collectionElement == null))
                return false;

            if (collectionElement != null)
            {
                assert column.type instanceof ListType || column.type instanceof MapType;
                if (getElementComparator(column).compare(collectionElement, that.collectionElement) != 0)
                    return false;
            }

            if (value == null || that.value == null)
                return value == that.value;
            else
                return getValueComparator(column).compare(value, that.value) == 0;
        }

        static AbstractType<?> getElementComparator(CFDefinition.Name column)
        {
            return column.type instanceof ListType
                    ? Int32Type.instance
                    : ((MapType)column.type).keys;
        }

        static AbstractType<?> getValueComparator(CFDefinition.Name column)
        {
            return (column.type instanceof ListType)
                    ? ((ListType) column.type).elements
                    : ((MapType) column.type).values;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, collectionElement, value, operator);
        }
    }

    static class ElementAccessInBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final List<ByteBuffer> inValues;

        private ElementAccessInBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            this.collectionElement = condition.collectionElement.bindAndGet(variables);

            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(variables).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(variables));
            }
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(current.metadata(), rowPrefix).add(column.name.key);
            if (column.type instanceof MapType)
            {
                Column item = current.getColumn(collectionPrefix.add(collectionElement).build());
                AbstractType<?> valueType = ((MapType) column.type).values;
                for (ByteBuffer value : inValues)
                {
                    if (isSatisfiedByValue(value, item, valueType, Relation.Type.EQ, now))
                        return true;
                }
                return false;
            }

            assert column.type instanceof ListType;
            ByteBuffer columnValue = ElementAccessBound.getListItem(collectionColumns(collectionPrefix, current, now),
                                                                    ElementAccessBound.getListIndex(collectionElement));

            AbstractType<?> valueType = ((ListType) column.type).elements;
            for (ByteBuffer value : inValues)
            {
                if (compareWithOperator(Relation.Type.EQ, valueType, value, columnValue))
                    return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof ElementAccessInBound))
                return false;

            ElementAccessInBound that = (ElementAccessInBound)o;
            if (!column.equals(that.column))
                return false;

            if ((collectionElement == null) != (that.collectionElement == null))
                return false;

            if (collectionElement != null)
            {
                assert column.type instanceof ListType || column.type instanceof MapType;
                if (ElementAccessBound.getElementComparator(column).compare(collectionElement, that.collectionElement) != 0)
                    return false;
            }

            return Bound.inValuesAreEqual(this.inValues, that.inValues, ElementAccessBound.getValueComparator(column));
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, collectionElement, inValues, operator);
        }
    }

    static class CollectionBound extends Bound
    {
        public final Term.Terminal value;

        private CollectionBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert !condition.operator.equals(Relation.Type.IN);
            this.value = condition.value.bind(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            CFMetaData cfm = current.metadata();
            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(cfm, rowPrefix).add(column.name.key);

            Iterator<Column> iter = collectionColumns(collectionPrefix, current, now);
            if (value == null)
            {
                if (!operator.equals(Relation.Type.EQ) && !operator.equals(Relation.Type.NEQ))
                    throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                return !iter.hasNext();
            }

            return valueAppliesTo(type, cfm, iter, value, operator);
        }

        static boolean valueAppliesTo(CollectionType type, CFMetaData cfm, Iterator<Column> iter, Term.Terminal value, Relation.Type operator)
        {
            if (value == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST: return listAppliesTo((ListType)type, cfm, iter, ((Lists.Value)value).elements, operator);
                case SET: return setAppliesTo((SetType)type, cfm, iter, ((Sets.Value)value).elements, operator);
                case MAP: return mapAppliesTo((MapType)type, cfm, iter, ((Maps.Value)value).map, operator);
            }
            throw new AssertionError();
        }

        private static ByteBuffer collectionKey(CFMetaData cfm, Column c)
        {
            ByteBuffer[] bbs = ((CompositeType)cfm.comparator).split(c.name());
            return bbs[bbs.length - 1];
        }

        private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Column> iter, Iterator<ByteBuffer> conditionIter, Relation.Type operator)
        {
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return operator.equals(Relation.Type.GT) || operator.equals(Relation.Type.GTE) || operator.equals(Relation.Type.NEQ);

                int comparison = type.compare(iter.next().value(), conditionIter.next());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return operator.equals(Relation.Type.LT) || operator.equals(Relation.Type.LTE) || operator.equals(Relation.Type.NEQ);

            // they're equal
            return operator == Relation.Type.EQ || operator == Relation.Type.LTE || operator == Relation.Type.GTE;
        }

        private static boolean evaluateComparisonWithOperator(int comparison, Relation.Type operator)
        {
            // called when comparison != 0
            switch (operator)
            {
                case EQ:
                    return false;
                case LT:
                case LTE:
                    return comparison < 0;
                case GT:
                case GTE:
                    return comparison > 0;
                case NEQ:
                    return true;
                default:
                    throw new AssertionError();
            }
        }

        static boolean listAppliesTo(ListType type, CFMetaData cfm, Iterator<Column> iter, List<ByteBuffer> elements, Relation.Type operator)
        {
            return setOrListAppliesTo(type.elements, iter, elements.iterator(), operator);
        }

        static boolean setAppliesTo(SetType type, CFMetaData cfm, Iterator<Column> iter, Set<ByteBuffer> elements, Relation.Type operator)
        {
            return setOrListAppliesTo(type.elements, iter, elements.iterator(), operator);
        }

        static boolean mapAppliesTo(MapType type, CFMetaData cfm, Iterator<Column> iter, Map<ByteBuffer, ByteBuffer> elements, Relation.Type operator)
        {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> conditionIter = elements.entrySet().iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return operator.equals(Relation.Type.GT) || operator.equals(Relation.Type.GTE) || operator.equals(Relation.Type.NEQ);

                Map.Entry<ByteBuffer, ByteBuffer> conditionEntry = conditionIter.next();
                Column c = iter.next();

                // compare the keys
                int comparison = type.keys.compare(collectionKey(cfm, c), conditionEntry.getKey());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                // compare the values
                comparison = type.values.compare(c.value(), conditionEntry.getValue());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return operator.equals(Relation.Type.LT) || operator.equals(Relation.Type.LTE) || operator.equals(Relation.Type.NEQ);

            // they're equal
            return operator == Relation.Type.EQ || operator == Relation.Type.LTE || operator == Relation.Type.GTE;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CollectionBound))
                return false;

            CollectionBound that = (CollectionBound)o;
            if (!column.equals(that.column))
                return false;

            if (!operator.equals(that.operator))
                return false;

            // Slightly inefficient because it serialize the collection just for the sake of comparison.
            // We could improve by adding an equals() method to Lists.Value, Sets.Value and Maps.Value but
            // this method is only called when there is 2 conditions on the same collection to make sure
            // both are not incompatible, so overall it's probably not worth the effort.
            ByteBuffer thisVal = value.get();
            ByteBuffer thatVal = that.value.get();
            return thisVal == null || thatVal == null
                    ? thisVal == null && thatVal == null
                    : column.type.compare(thisVal, thatVal) == 0;
        }

        @Override
        public int hashCode()
        {
            ByteBuffer valueBuffer = value == null ? null : value.get();
            return Objects.hashCode(column, valueBuffer, operator);
        }
    }

    public static class CollectionInBound extends Bound
    {
        public final List<Term.Terminal> inValues;

        private CollectionInBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert condition.operator.equals(Relation.Type.IN);
            inValues = new ArrayList<>();
            if (condition.inValues == null)
            {
                // We have a list of serialized collections that need to be deserialized for later comparisons
                CollectionType collectionType = (CollectionType) column.type;
                Lists.Marker inValuesMarker = (Lists.Marker) condition.value;
                if (column.type instanceof ListType)
                {
                    ListType deserializer = ListType.getInstance(collectionType.nameComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(variables).elements)
                        this.inValues.add(Lists.Value.fromSerialized(buffer, deserializer));
                }
                else if (column.type instanceof MapType)
                {
                    MapType deserializer = MapType.getInstance(collectionType.nameComparator(), collectionType.valueComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(variables).elements)
                        this.inValues.add(Maps.Value.fromSerialized(buffer, deserializer));
                }
                else if (column.type instanceof SetType)
                {
                    SetType deserializer = SetType.getInstance(collectionType.valueComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(variables).elements)
                        this.inValues.add(Sets.Value.fromSerialized(buffer, deserializer));
                }
            }
            else
            {
                for (Term value : condition.inValues)
                    this.inValues.add(value.bind(variables));
            }
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            CFMetaData cfm = current.metadata();
            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(cfm, rowPrefix).add(column.name.key);

            // copy iterator contents so that we can properly reuse them for each comparison with an IN value
            List<Column> columns = newArrayList(collectionColumns(collectionPrefix, current, now));
            for (Term.Terminal value : inValues)
            {
                if (CollectionBound.valueAppliesTo(type, cfm, columns.iterator(), value, Relation.Type.EQ))
                    return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CollectionInBound))
                return false;

            CollectionInBound that = (CollectionInBound)o;
            if (!column.equals(that.column))
                return false;

            // see note in CollectionBound.equals() about the efficiency of this
            List<ByteBuffer> thisValues = new ArrayList<>(this.inValues.size());
            for (Term.Terminal term : this.inValues)
                thisValues.add(term == null ? null : term.get());

            List<ByteBuffer> thatValues = new ArrayList<>(this.inValues.size());
            for (Term.Terminal term : that.inValues)
                thatValues.add(term == null ? null : term.get());

            return inValuesAreEqual(thisValues, thatValues, column.type);
        }

        @Override
        public int hashCode()
        {
            List<ByteBuffer> inValueBuffers = null;
            if (inValues != null)
            {
                inValueBuffers = new ArrayList<>(inValues.size());
                for (Term.Terminal term : inValues)
                    inValueBuffers.add(term.get());
            }
            return Objects.hashCode(column, inValueBuffers, operator);
        }
    }

    public static class Raw
    {
        private final Term.Raw value;
        private final List<Term.Raw> inValues;
        private final AbstractMarker.INRaw inMarker;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        private final Relation.Type operator;

        private Raw(Term.Raw value, List<Term.Raw> inValues, AbstractMarker.INRaw inMarker, Term.Raw collectionElement, Relation.Type op)
        {
            this.value = value;
            this.inValues = inValues;
            this.inMarker = inMarker;
            this.collectionElement = collectionElement;
            this.operator = op;
        }

        /** A condition on a column. For example: "IF col = 'foo'" */
        public static Raw simpleCondition(Term.Raw value, Relation.Type op)
        {
            return new Raw(value, null, null, null, op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        public static Raw simpleInCondition(List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, null, Relation.Type.IN);
        }

        /** An IN condition on a column with a single marker. For example: "IF col IN ?" */
        public static Raw simpleInCondition(AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, null, Relation.Type.IN);
        }

        /** A condition on a collection element.  For example: "IF col['key'] = 'foo'" */
        public static Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Relation.Type op)
        {
            return new Raw(value, null, null, collectionElement, op);
        }

        /** An IN condition on a collection element.  For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        public static Raw collectionInCondition(Term.Raw collectionElement, List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, collectionElement, Relation.Type.IN);
        }

        /** An IN condition on a collection element with a single marker.  For example: "IF col['key'] IN ?" */
        public static Raw collectionInCondition(Term.Raw collectionElement, AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, collectionElement, Relation.Type.IN);
        }

        public ColumnCondition prepare(CFDefinition.Name receiver) throws InvalidRequestException
        {
            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException("Condtions on counters are not supported");

            if (collectionElement == null)
            {
                if (operator.equals(Relation.Type.IN))
                {
                    if (inValues == null)
                        return ColumnCondition.inCondition(receiver, inMarker.prepare(receiver));

                    List<Term> terms = new ArrayList<>(inValues.size());
                    for (Term.Raw value : inValues)
                        terms.add(value.prepare(receiver));
                    return ColumnCondition.inCondition(receiver, terms);
                }
                else
                {
                    return ColumnCondition.condition(receiver, value.prepare(receiver), operator);
                }
            }

            if (!(receiver.type.isCollection()))
                throw new InvalidRequestException(String.format("Invalid element access syntax for non-collection column %s", receiver.name));

            ColumnSpecification elementSpec, valueSpec;
            switch ((((CollectionType)receiver.type).kind))
            {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    valueSpec = Lists.valueSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    valueSpec = Maps.valueSpecOf(receiver);
                    break;
                case SET:
                    throw new InvalidRequestException(String.format("Invalid element access syntax for set column %s", receiver.name));
                default:
                    throw new AssertionError();
            }

            if (operator.equals(Relation.Type.IN))
            {
                if (inValues == null)
                    return ColumnCondition.inCondition(receiver, collectionElement.prepare(elementSpec), inMarker.prepare(valueSpec));

                List<Term> terms = new ArrayList<>(inValues.size());
                for (Term.Raw value : inValues)
                    terms.add(value.prepare(valueSpec));
                return ColumnCondition.inCondition(receiver, collectionElement.prepare(elementSpec), terms);
            }
            else
            {
                return ColumnCondition.condition(receiver, collectionElement.prepare(elementSpec), value.prepare(valueSpec), operator);
            }
        }
    }
}
