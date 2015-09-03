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

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.view.MaterializedView;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

public class CreateMaterializedViewStatement extends SchemaAlteringStatement
{
    private final CFName baseName;
    private final List<RawSelector> selectClause;
    private final List<Relation> whereClause;
    private final List<ColumnIdentifier.Raw> partitionKeys;
    private final List<ColumnIdentifier.Raw> clusteringKeys;
    public final CFProperties properties = new CFProperties();
    private final boolean ifNotExists;

    public CreateMaterializedViewStatement(CFName viewName,
                                           CFName baseName,
                                           List<RawSelector> selectClause,
                                           List<Relation> whereClause,
                                           List<ColumnIdentifier.Raw> partitionKeys,
                                           List<ColumnIdentifier.Raw> clusteringKeys,
                                           boolean ifNotExists)
    {
        super(viewName);
        this.baseName = baseName;
        this.selectClause = selectClause;
        this.whereClause = whereClause;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.ifNotExists = ifNotExists;
    }


    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (!baseName.hasKeyspace())
            baseName.setKeyspace(keyspace(), true);
        state.hasColumnFamilyAccess(keyspace(), baseName.getColumnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // We do validation in announceMigration to reduce doubling up of work
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        // We need to make sure that:
        //  - primary key includes all columns in base table's primary key
        //  - make sure that the select statement does not have anything other than columns
        //    and their names match the base table's names
        //  - make sure that primary key does not include any collections
        //  - make sure there is no where clause in the select statement
        //  - make sure there is not currently a table or view
        //  - make sure baseTable gcGraceSeconds > 0

        properties.validate();

        if (properties.useCompactStorage)
            throw new InvalidRequestException("Cannot use 'COMPACT STORAGE' when defining a materialized view");

        // We enforce the keyspace because if the RF is different, the logic to wait for a
        // specific replica would break
        if (!baseName.getKeyspace().equals(keyspace()))
            throw new InvalidRequestException("Cannot create a materialized view on a table in a separate keyspace");

        CFMetaData cfm = ThriftValidation.validateColumnFamily(baseName.getKeyspace(), baseName.getColumnFamily());

        if (cfm.isCounter())
            throw new InvalidRequestException("Materialized views are not supported on counter tables");

        if (cfm.isMaterializedView())
            throw new InvalidRequestException("Materialized views cannot be created against other materialized views");

        if (cfm.params.gcGraceSeconds == 0)
        {
            throw new InvalidRequestException(String.format("Cannot create materialized view '%s' for base table " +
                                                            "'%s' with gc_grace_seconds of 0, since this value is " +
                                                            "used to TTL undelivered updates. Setting gc_grace_seconds" +
                                                            " too low might cause undelivered updates to expire " +
                                                            "before being replayed.", cfName.getColumnFamily(),
                                                            baseName.getColumnFamily()));
        }

        Set<ColumnIdentifier> included = new HashSet<>();
        for (RawSelector selector : selectClause)
        {
            Selectable.Raw selectable = selector.selectable;
            if (selectable instanceof Selectable.WithFieldSelection.Raw)
                throw new InvalidRequestException("Cannot select out a part of type when defining a materialized view");
            if (selectable instanceof Selectable.WithFunction.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            if (selectable instanceof Selectable.WritetimeOrTTL.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            ColumnIdentifier identifier = (ColumnIdentifier) selectable.prepare(cfm);
            if (selector.alias != null)
                throw new InvalidRequestException(String.format("Cannot alias column '%s' as '%s' when defining a materialized view", identifier.toString(), selector.alias.toString()));

            ColumnDefinition cdef = cfm.getColumnDefinition(identifier);

            if (cdef == null)
                throw new InvalidRequestException("Unknown column name detected in CREATE MATERIALIZED VIEW statement : "+identifier);

            if (cdef.isStatic())
                ClientWarn.warn(String.format("Unable to include static column '%s' in Materialized View SELECT statement", identifier));
            else
                included.add(identifier);
        }

        Set<ColumnIdentifier.Raw> targetPrimaryKeys = new HashSet<>();
        for (ColumnIdentifier.Raw identifier : Iterables.concat(partitionKeys, clusteringKeys))
        {
            if (!targetPrimaryKeys.add(identifier))
                throw new InvalidRequestException("Duplicate entry found in PRIMARY KEY: "+identifier);

            ColumnDefinition cdef = cfm.getColumnDefinition(identifier.prepare(cfm));

            if (cdef == null)
                throw new InvalidRequestException("Unknown column name detected in CREATE MATERIALIZED VIEW statement : "+identifier);

            if (cfm.getColumnDefinition(identifier.prepare(cfm)).type.isMultiCell())
                throw new InvalidRequestException(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", identifier));

            if (cdef.isStatic())
                throw new InvalidRequestException(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", identifier));
        }

        // build the select statement
        Map<ColumnIdentifier.Raw, Boolean> orderings = Collections.emptyMap();
        SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, false, true, false);
        SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(baseName, parameters, selectClause, whereClause, null);
        ClientState state = ClientState.forInternalCalls();
        state.setKeyspace(keyspace());
        rawSelect.prepareKeyspace(state);
        rawSelect.setBoundVariables(Collections.emptyList());
        ParsedStatement.Prepared prepared = rawSelect.prepare(true);
        SelectStatement select = (SelectStatement) prepared.statement;
        StatementRestrictions restrictions = select.getRestrictions();

        if (!restrictions.nonPKRestrictedColumns().isEmpty())
        {
            throw new InvalidRequestException(String.format(
                    "Non-primary key columns cannot be restricted in the SELECT statement used for materialized view " +
                    "creation (got restrictions on: %s)",
                    ColumnDefinition.toIdentifiers(new ArrayList<>(restrictions.nonPKRestrictedColumns()))));
        }

        List<MaterializedView.WhereExpression> expressions = relationsToExpressions(whereClause);

        Set<ColumnIdentifier> basePrimaryKeyCols = new HashSet<>();
        for (ColumnDefinition definition : Iterables.concat(cfm.partitionKeyColumns(), cfm.clusteringColumns()))
            basePrimaryKeyCols.add(definition.name);

        List<ColumnIdentifier> targetClusteringColumns = new ArrayList<>();
        List<ColumnIdentifier> targetPartitionKeys = new ArrayList<>();

        // This is only used as an intermediate state; this is to catch whether multiple non-PK columns are used
        boolean hasNonPKColumn = false;
        for (ColumnIdentifier.Raw raw : partitionKeys)
        {
            hasNonPKColumn = getColumnIdentifier(cfm, basePrimaryKeyCols, hasNonPKColumn, raw, targetPartitionKeys,
                                                 restrictions.notNullColumns(), restrictions);
        }

        for (ColumnIdentifier.Raw raw : clusteringKeys)
        {
            hasNonPKColumn = getColumnIdentifier(cfm, basePrimaryKeyCols, hasNonPKColumn, raw, targetClusteringColumns,
                                                 restrictions.notNullColumns(), restrictions);
        }

        // We need to include all of the primary key columns from the base table in order to make sure that we do not
        // overwrite values in the materialized view. We cannot support "collapsing" the base table into a smaller
        // number of rows in the view because if we need to generate a tombstone, we have no way of knowing which value
        // is currently being used in the view and whether or not to generate a tombstone.
        // In order to not surprise our users, we require that they include all of the columns. We provide them with
        // a list of all of the columns left to include.
        boolean missingClusteringColumns = false;
        StringBuilder columnNames = new StringBuilder();
        for (ColumnDefinition def : cfm.allColumns())
        {
            if (!def.isPrimaryKeyColumn()) continue;

            ColumnIdentifier identifier = def.name;
            if (!targetClusteringColumns.contains(identifier) && !targetPartitionKeys.contains(identifier))
            {
                if (missingClusteringColumns)
                    columnNames.append(',');
                else
                    missingClusteringColumns = true;
                columnNames.append(identifier);
            }
        }
        if (missingClusteringColumns)
            throw new InvalidRequestException(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)",
                                                            columnFamily(), baseName.getColumnFamily(), columnNames.toString()));

        if (targetPartitionKeys.isEmpty())
            throw new InvalidRequestException("Must select at least a column for a Materialized View");

        if (targetClusteringColumns.isEmpty())
            throw new InvalidRequestException("No columns are defined for Materialized View other than primary key");

        MaterializedViewDefinition definition = new MaterializedViewDefinition(keyspace(),
                                                                               baseName.getColumnFamily(),
                                                                               columnFamily(),
                                                                               targetPartitionKeys,
                                                                               targetClusteringColumns,
                                                                               included,
                                                                               rawSelect,
                                                                               expressions);

        CFMetaData indexCf = MaterializedView.getCFMetaData(definition, cfm, properties);
        try
        {
            MigrationManager.announceNewColumnFamily(indexCf, isLocalOnly);
        }
        catch (AlreadyExistsException e)
        {
            if (ifNotExists)
                return false;
            throw e;
        }

        CFMetaData newCfm = cfm.copy();
        newCfm.materializedViews(newCfm.getMaterializedViews().with(definition));

        MigrationManager.announceColumnFamilyUpdate(newCfm, false, isLocalOnly);

        return true;
    }

    private static List<MaterializedView.WhereExpression> relationsToExpressions(List<Relation> whereClause)
    {
        List<MaterializedView.WhereExpression> expressions = new ArrayList<>(whereClause.size());
        for (Relation rel : whereClause)
        {
            List<String> identifiers;
            if (rel.isMultiColumn())
            {
                MultiColumnRelation relation = (MultiColumnRelation) rel;
                identifiers = new ArrayList<>(relation.getEntities().size());
                for (ColumnIdentifier.Raw raw : relation.getEntities())
                    identifiers.add(raw.toString());
            }
            else
            {
                SingleColumnRelation relation = (SingleColumnRelation) rel;
                identifiers = Collections.singletonList(relation.getEntity().toString());
            }

            List<String> values = new ArrayList<>();
            if (rel.isIN())
            {
                values.add(rel.getInValues().stream()
                                            .map(rawTerm -> ((Term.Literal) rawTerm).getRawText())
                                            .collect(Collectors.joining(", ", "(", ")")));
            }
            else
            {
                values.add(((Term.Literal) rel.getValue()).getRawText());
            }

            expressions.add(new MaterializedView.WhereExpression(identifiers, rel.operator(), values));
        }
        return expressions;
    }

    private static boolean getColumnIdentifier(CFMetaData cfm,
                                               Set<ColumnIdentifier> basePK,
                                               boolean hasNonPKColumn,
                                               ColumnIdentifier.Raw raw,
                                               List<ColumnIdentifier> columns,
                                               Set<ColumnIdentifier> notNullColumns,
                                               StatementRestrictions restrictions)
    {
        ColumnIdentifier identifier = raw.prepare(cfm);
        ColumnDefinition def = cfm.getColumnDefinition(identifier);

        boolean isPk = basePK.contains(identifier);
        if (!isPk && hasNonPKColumn)
        {
            throw new InvalidRequestException(String.format("Cannot include more than one non-primary key column '%s' in materialized view partition key", identifier));
        }

        if (!notNullColumns.contains(identifier) && !restrictions.isRestricted(def))
            throw new InvalidRequestException(String.format("Primary key column '%s' is required to be filtered by 'IS NOT NULL'", identifier));

        columns.add(identifier);
        return !isPk;
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
