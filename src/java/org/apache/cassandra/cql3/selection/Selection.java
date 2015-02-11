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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.base.Predicate;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.json.simple.JSONValue;

public abstract class Selection
{
    /**
     * A predicate that returns <code>true</code> for static columns.
     */
    private static final Predicate<ColumnDefinition> STATIC_COLUMN_FILTER = new Predicate<ColumnDefinition>()
    {
        public boolean apply(ColumnDefinition def)
        {
            return def.isStatic();
        }
    };

    private static final ColumnIdentifier jsonId = new ColumnIdentifier("json", false);

    private final CFMetaData cfm;
    private final Collection<ColumnDefinition> columns;
    private final ResultSet.Metadata metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;
    protected final boolean isJson;
    protected final ResultSet.Metadata jsonMetadata;

    protected Selection(CFMetaData cfm,
                        Collection<ColumnDefinition> columns,
                        List<ColumnSpecification> metadata,
                        boolean collectTimestamps,
                        boolean collectTTLs,
                        boolean isJson)
    {
        this.cfm = cfm;
        this.columns = columns;
        this.metadata = new ResultSet.Metadata(metadata);
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
        this.isJson = isJson;

        if (isJson)
        {
            ColumnSpecification firstColumn = metadata.get(0);
            ColumnSpecification jsonSpec = new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, jsonId, UTF8Type.instance);
            jsonMetadata = new ResultSet.Metadata(Arrays.asList(jsonSpec));
        }
        else
        {
            jsonMetadata = null;
        }
    }

    // Overriden by SimpleSelection when appropriate.
    public boolean isWildcard()
    {
        return false;
    }    

    /**
     * Checks if this selection contains static columns.
     * @return <code>true</code> if this selection contains static columns, <code>false</code> otherwise;
     */
    public boolean containsStaticColumns()
    {
        if (!cfm.hasStaticColumns())
            return false;

        if (isWildcard())
            return true;

        return !Iterables.isEmpty(Iterables.filter(columns, STATIC_COLUMN_FILTER));
    }

    /**
     * Checks if this selection contains only static columns.
     * @return <code>true</code> if this selection contains only static columns, <code>false</code> otherwise;
     */
    public boolean containsOnlyStaticColumns()
    {
        if (!containsStaticColumns())
            return false;

        if (isWildcard())
            return false;

        for (ColumnDefinition def : getColumns())
        {
            if (!def.isPartitionKey() && !def.isStatic())
                return false;
        }

        return true;
    }

    /**
     * Checks if this selection contains a collection.
     *
     * @return <code>true</code> if this selection contains a collection, <code>false</code> otherwise.
     */
    public boolean containsACollection()
    {
        if (!cfm.comparator.hasCollections())
            return false;

        for (ColumnDefinition def : getColumns())
            if (def.type.isCollection() && def.type.isMultiCell())
                return true;

        return false;
    }

    /**
     * Returns the index of the specified column.
     *
     * @param def the column definition
     * @return the index of the specified column
     */
    public int indexOf(final ColumnDefinition def)
    {
        return Iterators.indexOf(getColumns().iterator(), new Predicate<ColumnDefinition>()
           {
               public boolean apply(ColumnDefinition n)
               {
                   return def.name.equals(n.name);
               }
           });
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return isJson ? jsonMetadata : metadata;
    }

    public static Selection wildcard(CFMetaData cfm, boolean isJson)
    {
        List<ColumnDefinition> all = new ArrayList<ColumnDefinition>(cfm.allColumns().size());
        Iterators.addAll(all, cfm.allColumnsInSelectOrder());
        return new SimpleSelection(cfm, all, true, isJson);
    }

    public static Selection forColumns(CFMetaData cfm, Collection<ColumnDefinition> columns, boolean isJson)
    {
        return new SimpleSelection(cfm, columns, false, isJson);
    }

    public int addColumnForOrdering(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return false;
    }

    private static boolean processesSelection(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
                return true;
        }
        return false;
    }

    public static Selection fromSelectors(CFMetaData cfm, List<RawSelector> rawSelectors, boolean isJson) throws InvalidRequestException
    {
        List<ColumnDefinition> defs = new ArrayList<ColumnDefinition>();

        SelectorFactories factories =
                SelectorFactories.createFactoriesAndCollectColumnDefinitions(RawSelector.toSelectables(rawSelectors, cfm), cfm, defs);
        List<ColumnSpecification> metadata = collectMetadata(cfm, rawSelectors, factories);

        return processesSelection(rawSelectors) ? new SelectionWithProcessing(cfm, defs, metadata, factories, isJson)
                                                : new SimpleSelection(cfm, defs, metadata, false, isJson);
    }

    private static List<ColumnSpecification> collectMetadata(CFMetaData cfm,
                                                             List<RawSelector> rawSelectors,
                                                             SelectorFactories factories)
    {
        List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
        Iterator<RawSelector> iter = rawSelectors.iterator();
        for (Selector.Factory factory : factories)
        {
            ColumnSpecification colSpec = factory.getColumnSpecification(cfm);
            ColumnIdentifier alias = iter.next().alias;
            metadata.add(alias == null ? colSpec : colSpec.withAlias(alias));
        }
        return metadata;
    }

    protected abstract Selectors newSelectors() throws InvalidRequestException;

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public Collection<ColumnDefinition> getColumns()
    {
        return columns;
    }

    public ResultSetBuilder resultSetBuilder(long now) throws InvalidRequestException
    {
        return new ResultSetBuilder(now);
    }

    public abstract boolean isAggregate();

    protected List<ByteBuffer> rowToJson(List<ByteBuffer> row, int protocolVersion)
    {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < metadata.names.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            ColumnSpecification spec = metadata.names.get(i);
            String columnName = spec.name.toString();
            if (!columnName.equals(columnName.toLowerCase(Locale.US)))
                columnName = "\"" + columnName + "\"";

            ByteBuffer buffer = row.get(i);
            sb.append('"');
            sb.append(JSONValue.escape(columnName));
            sb.append("\": ");
            if (buffer == null)
                sb.append("null");
            else
                sb.append(spec.type.toJSONString(buffer, protocolVersion));
        }
        sb.append("}");
        return Collections.singletonList(UTF8Type.instance.getSerializer().serialize(sb.toString()));
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("columns", columns)
                .add("metadata", metadata)
                .add("collectTimestamps", collectTimestamps)
                .add("collectTTLs", collectTTLs)
                .add("isJson", isJson)
                .toString();
    }

    public class ResultSetBuilder
    {
        private final ResultSet resultSet;

        /**
         * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
         * its own <code>Selectors</code> instance.
         */
        private final Selectors selectors;

        /*
         * We'll build CQL3 row one by one.
         * The currentRow is the values for the (CQL3) columns we've fetched.
         * We also collect timestamps and ttls for the case where the writetime and
         * ttl functions are used. Note that we might collect timestamp and/or ttls
         * we don't care about, but since the array below are allocated just once,
         * it doesn't matter performance wise.
         */
        List<ByteBuffer> current;
        final long[] timestamps;
        final int[] ttls;
        final long now;

        private ResultSetBuilder(long now) throws InvalidRequestException
        {
            this.resultSet = new ResultSet(getResultMetadata().copy(), new ArrayList<List<ByteBuffer>>());
            this.selectors = newSelectors();
            this.timestamps = collectTimestamps ? new long[columns.size()] : null;
            this.ttls = collectTTLs ? new int[columns.size()] : null;
            this.now = now;
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(Cell c)
        {
            current.add(isDead(c) ? null : value(c));
            if (timestamps != null)
            {
                timestamps[current.size() - 1] = isDead(c) ? Long.MIN_VALUE : c.timestamp();
            }
            if (ttls != null)
            {
                int ttl = -1;
                if (!isDead(c) && c instanceof ExpiringCell)
                    ttl = c.getLocalDeletionTime() - (int) (now / 1000);
                ttls[current.size() - 1] = ttl;
            }
        }

        private boolean isDead(Cell c)
        {
            return c == null || !c.isLive(now);
        }

        public void newRow(int protocolVersion) throws InvalidRequestException
        {
            if (current != null)
            {
                selectors.addInputRow(protocolVersion, this);
                if (!selectors.isAggregate())
                {
                    resultSet.addRow(selectors.getOutputRow(protocolVersion));
                    selectors.reset();
                }
            }
            current = new ArrayList<ByteBuffer>(columns.size());
        }

        public ResultSet build(int protocolVersion) throws InvalidRequestException
        {
            if (current != null)
            {
                selectors.addInputRow(protocolVersion, this);
                resultSet.addRow(selectors.getOutputRow(protocolVersion));
                selectors.reset();
                current = null;
            }

            if (resultSet.isEmpty() && selectors.isAggregate())
            {
                resultSet.addRow(selectors.getOutputRow(protocolVersion));
            }
            return resultSet;
        }

        private ByteBuffer value(Cell c)
        {
            return (c instanceof CounterCell)
                ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                : c.value();
        }
    }

    private static interface Selectors
    {
        public boolean isAggregate();

        /**
         * Adds the current row of the specified <code>ResultSetBuilder</code>.
         *
         * @param rs the <code>ResultSetBuilder</code>
         * @throws InvalidRequestException
         */
        public void addInputRow(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException;

        public List<ByteBuffer> getOutputRow(int protocolVersion) throws InvalidRequestException;

        public void reset();
    }

    // Special cased selection for when no function is used (this save some allocations).
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(CFMetaData cfm, Collection<ColumnDefinition> columns, boolean isWildcard, boolean isJson)
        {
            this(cfm, columns, new ArrayList<ColumnSpecification>(columns), isWildcard, isJson);
        }

        public SimpleSelection(CFMetaData cfm,
                               Collection<ColumnDefinition> columns,
                               List<ColumnSpecification> metadata,
                               boolean isWildcard,
                               boolean isJson)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(cfm, columns, metadata, false, false, isJson);
            this.isWildcard = isWildcard;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }

        public boolean isAggregate()
        {
            return false;
        }

        protected Selectors newSelectors()
        {
            return new Selectors()
            {
                private List<ByteBuffer> current;

                public void reset()
                {
                    current = null;
                }

                public List<ByteBuffer> getOutputRow(int protocolVersion)
                {
                    return isJson ? rowToJson(current, protocolVersion) : current;
                }

                public void addInputRow(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
                {
                    current = rs.current;
                }

                public boolean isAggregate()
                {
                    return false;
                }
            };
        }
    }

    private static class SelectionWithProcessing extends Selection
    {
        private final SelectorFactories factories;

        public SelectionWithProcessing(CFMetaData cfm,
                                       Collection<ColumnDefinition> columns,
                                       List<ColumnSpecification> metadata,
                                       SelectorFactories factories,
                                       boolean isJson) throws InvalidRequestException
        {
            super(cfm,
                  columns,
                  metadata,
                  factories.containsWritetimeSelectorFactory(),
                  factories.containsTTLSelectorFactory(),
                  isJson);

            this.factories = factories;

            if (factories.doesAggregation() && !factories.containsOnlyAggregateFunctions())
                throw new InvalidRequestException("the select clause must either contains only aggregates or none");
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return factories.usesFunction(ksName, functionName);
        }

        @Override
        public int addColumnForOrdering(ColumnDefinition c)
        {
            int index = super.addColumnForOrdering(c);
            factories.addSelectorForOrdering(c, index);
            return index;
        }

        public boolean isAggregate()
        {
            return factories.containsOnlyAggregateFunctions();
        }

        protected Selectors newSelectors() throws InvalidRequestException
        {
            return new Selectors()
            {
                private final List<Selector> selectors = factories.newInstances();

                public void reset()
                {
                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        selectors.get(i).reset();
                    }
                }

                public boolean isAggregate()
                {
                    return factories.containsOnlyAggregateFunctions();
                }

                public List<ByteBuffer> getOutputRow(int protocolVersion) throws InvalidRequestException
                {
                    List<ByteBuffer> outputRow = new ArrayList<>(selectors.size());

                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        outputRow.add(selectors.get(i).getOutput(protocolVersion));
                    }

                    return isJson ? rowToJson(outputRow, protocolVersion) : outputRow;
                }

                public void addInputRow(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
                {
                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        selectors.get(i).addInput(protocolVersion, rs);
                    }
                }
            };
        }

    }
}
