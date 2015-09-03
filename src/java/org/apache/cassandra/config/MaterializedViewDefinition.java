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

package org.apache.cassandra.config;

import java.util.*;
import java.util.stream.Collectors;

import org.antlr.runtime.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.view.MaterializedView;
import org.apache.cassandra.exceptions.SyntaxException;

public class MaterializedViewDefinition
{
    public final String keyspace;
    public final String baseCfName;
    public final String viewName;
    // The order of partititon columns and clustering columns is important, so we cannot switch these two to sets
    public final List<ColumnIdentifier> partitionColumns;
    public final List<ColumnIdentifier> clusteringColumns;
    public final Set<ColumnIdentifier> included;
    public final boolean includeAll;

    public SelectStatement.RawStatement select;
    public String whereClause;

    public MaterializedViewDefinition(MaterializedViewDefinition def)
    {
        this(def.keyspace, def.baseCfName, def.viewName, new ArrayList<>(def.partitionColumns), new ArrayList<>(def.clusteringColumns),
             new HashSet<>(def.included), def.select, def.whereClause);
    }

    /**
     * @param baseCfName        Name of the column family from which this view is based
     * @param viewName          Name of the view
     * @param partitionColumns  List of all of the partition columns, in the order they are defined
     * @param clusteringColumns List of all of the clustering columns, in the order they are defined
     * @param included
     */
    public MaterializedViewDefinition(String keyspace, String baseCfName, String viewName, List<ColumnIdentifier> partitionColumns,
                                      List<ColumnIdentifier> clusteringColumns, Set<ColumnIdentifier> included,
                                      SelectStatement.RawStatement select, String whereClause)
    {
        assert partitionColumns != null && !partitionColumns.isEmpty();
        assert included != null;
        this.keyspace = keyspace;
        this.baseCfName = baseCfName;
        this.viewName = viewName;
        this.partitionColumns = partitionColumns;
        this.clusteringColumns = clusteringColumns;
        this.includeAll = included.isEmpty();
        this.included = included;
        this.select = select;
        this.whereClause = whereClause;
    }

    /**
     * @return true if the view specified by this definition will include the column, false otherwise
     */
    public boolean includes(ColumnIdentifier column)
    {
        return includeAll
               || partitionColumns.contains(column)
               || clusteringColumns.contains(column)
               || included.contains(column);
    }

    /**
     * Replace the column {@param from} with {@param to} in this materialized view definition's partition,
     * clustering, or included columns.
     */
    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (!includeAll && included.contains(from))
        {
            included.remove(from);
            included.add(to);
        }

        int partitionIndex = partitionColumns.indexOf(from);
        if (partitionIndex >= 0)
            partitionColumns.set(partitionIndex, to);

        int clusteringIndex = clusteringColumns.indexOf(from);
        if (clusteringIndex >= 0)
            clusteringColumns.set(clusteringIndex, to);

        // convert whereClause to Relations, rename ids in Relations, then convert back to whereClause
        List<Relation> relations = whereClauseToRelations(whereClause);
        ColumnIdentifier.Raw fromRaw = new ColumnIdentifier.Raw(from.toString(), true);
        ColumnIdentifier.Raw toRaw = new ColumnIdentifier.Raw(to.toString(), true);
        List<Relation> newRelations = relations.stream() .map(r -> r.renameIdentifier(fromRaw, toRaw)) .collect(Collectors.toList());

        this.whereClause = MaterializedView.relationsToWhereClause(newRelations);
        String rawSelect = MaterializedView.buildSelectStatement(baseCfName, included, whereClause);
        this.select = (SelectStatement.RawStatement) QueryProcessor.parseStatement(rawSelect);
    }

    /** Parses the whereClause to convert it into a list of Relations. */
    private static List<Relation> whereClauseToRelations(String whereClause)
    {
        ErrorCollector errorCollector = new ErrorCollector(whereClause);
        CharStream stream = new ANTLRStringStream(whereClause);
        CqlLexer lexer = new CqlLexer(stream);
        lexer.addErrorListener(errorCollector);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(errorCollector);

        try
        {
            List<Relation> relations = parser.whereClause();

            // The errorCollector has queued up any errors that the lexer and parser may have encountered
            // along the way, if necessary, we turn the last error into exceptions here.
            errorCollector.throwFirstSyntaxError();

            return relations;
        }
        catch (RecognitionException | SyntaxException exc)
        {
            throw new RuntimeException("Unexpected error parsing materialized view's where clause while handling column rename: ", exc);
        }
    }
}
