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

import java.util.Objects;
import java.util.UUID;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class ViewDefinition
{
    public final String ksName;
    public final String viewName;
    public final UUID baseId;
    public final boolean includeAll;
    // The order of partititon columns and clustering columns is important, so we cannot switch these two to sets
    public final CFMetaData metadata;

    public ViewDefinition(ViewDefinition def)
    {
        this(def.ksName, def.viewName, def.baseId, def.includeAll, def.metadata);
    }

    /**
     * @param viewName          Name of the view
     * @param baseId            Internal ID of the table which this view is based off of
     * @param includeAll        Whether to include all columns or not
     */
    public ViewDefinition(String ksName, String viewName, UUID baseId, boolean includeAll, CFMetaData metadata)
    {
        this.ksName = ksName;
        this.viewName = viewName;
        this.baseId = baseId;
        this.includeAll = includeAll;
        this.metadata = metadata;
    }

    /**
     * @return true if the view specified by this definition will include the column, false otherwise
     */
    public boolean includes(ColumnIdentifier column)
    {
        return metadata.getColumnDefinition(column) != null;
    }

    public ViewDefinition copy()
    {
        return new ViewDefinition(ksName, viewName, baseId, includeAll, metadata.copy());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ViewDefinition))
            return false;

        ViewDefinition other = (ViewDefinition) o;
        return Objects.equals(ksName, other.ksName)
               && Objects.equals(viewName, other.viewName)
               && Objects.equals(baseId, other.baseId)
               && Objects.equals(includeAll, other.includeAll)
               && Objects.equals(metadata, other.metadata);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
               .append(ksName)
               .append(viewName)
               .append(baseId)
               .append(includeAll)
               .append(metadata)
               .toHashCode();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
               .append("ksName", ksName)
               .append("viewName", viewName)
               .append("baseId", baseId)
               .append("includeAll", includeAll)
               .append("metadata", metadata)
               .toString();
    }
}
