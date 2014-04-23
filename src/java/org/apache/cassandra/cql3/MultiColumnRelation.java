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
    private final Tuples.Raw marker;
    private final Tuples.Literal values;

    private final List<Tuples.Literal> inValues;
    private final Tuples.INRaw inMarker;

    private MultiColumnRelation(List<ColumnIdentifier> entities, Type relationType, Tuples.Literal values, Tuples.Raw marker, List<Tuples.Literal> inValues, Tuples.INRaw inMarker)
    {
        this.entities = entities;
        this.relationType = relationType;
        this.values = values;
        this.marker = marker;

        this.inValues = inValues;
        this.inMarker = inMarker;
    }

    public static MultiColumnRelation createNonInRelation(List<ColumnIdentifier> entities, Type relationType, Tuples.Literal literal)
    {
        return new MultiColumnRelation(entities, relationType, literal, null, null, null);
    }

    public static MultiColumnRelation createNonInRelation(List<ColumnIdentifier> entities, Type relationType, Tuples.Raw marker)
    {
        return new MultiColumnRelation(entities, relationType, null, marker, null, null);
    }

    public static MultiColumnRelation createInRelation(List<ColumnIdentifier> entities, Type relationType, List<Tuples.Literal> inValues)
    {
        return new MultiColumnRelation(entities, relationType, null, null, inValues, null);
    }

    public static MultiColumnRelation createSingleMarkerInRelation(List<ColumnIdentifier> entities, Type relationType, Tuples.INRaw inMarker)
    {
        return new MultiColumnRelation(entities, relationType, null, null, null, inMarker);
    }

    public List<ColumnIdentifier> getEntities()
    {
        return entities;
    }

    public Tuples.Literal getValues()
    {
        return values;
    }

    public Tuples.Raw getMarker()
    {
        return marker;
    }

    public List<Tuples.Literal> getInValues()
    {
        return inValues;
    }

    public Tuples.INRaw getInMarker()
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
            StringBuilder sb = new StringBuilder("(");
            for (int i=0; i < entities.size(); i++)
            {
                sb.append(entities.get(i));
                if (i != entities.size() - 1)
                    sb.append(", ");
            }

            sb.append(") IN (");
            for (int i = 0; i < inValues.size(); i++)
            {
                sb.append(inValues.get(i));
                if (i != inValues.size() - 1)
                    sb.append(", ");
            }
            sb.append(")");
            return sb.toString();
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
            sb.append(" ");
            sb.append(values);
            return sb.toString();
        }
    }
}