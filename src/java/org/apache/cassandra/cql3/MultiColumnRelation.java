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

import java.util.List;

public class MultiColumnRelation extends Relation
{
    private final List<ColumnIdentifier> entities;
    private final List<Term.Raw> values;
    private final List<List<Term.Raw>> inValues;
    private final AbstractMarker.INRaw inMarker;

    private MultiColumnRelation(List<ColumnIdentifier> entities, Type relationType, List<Term.Raw> values, List<List<Term.Raw>> inValues, AbstractMarker.INRaw inMarker)
    {
        this.entities = entities;
        this.relationType = relationType;
        this.values = values;
        this.inValues = inValues;
        this.inMarker = inMarker;
    }

    public MultiColumnRelation(List<ColumnIdentifier> entities, Type relationType, List<Term.Raw> values)
    {
        this(entities, relationType, values, null, null);
    }

    public List<ColumnIdentifier> getEntities()
    {
        return entities;
    }

    public List<Term.Raw> getValues()
    {
        return values;
    }

    public List<List<Term.Raw>> getInValues()
    {
        return inValues;
    }

    public AbstractMarker.INRaw getInMarker()
    {
        return inMarker;
    }

    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    public String toString()
    {
        if (relationType == Type.IN)
        {
            // TODO
            return "(...) IN (...)";
            // return String.format("%s IN %s", entity, inValues);
        }
        else
        {
            StringBuilder sb = new StringBuilder("(");
            for (int i=0; i < entities.size(); i++)
            {
                sb.append(entities.get(i));
                if (i != entities.size() - 1)
                    sb.append(", ");
            }

            sb.append(") ");
            sb.append(relationType);
            sb.append(" (");

            for (int i=0; i < values.size(); i++)
            {
                sb.append(values.get(i));
                if (i != values.size() - 1)
                    sb.append(", ");
            }

            sb.append(")");
            return sb.toString();
        }
    }
}