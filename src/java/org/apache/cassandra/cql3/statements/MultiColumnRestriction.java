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

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Term;

import java.util.List;

public interface MultiColumnRestriction extends Restriction
{
    public static class EQ extends SingleColumnRestriction.EQ
    {
        public EQ(Term value, boolean onToken)
        {
            super(value, onToken);
        }

        public boolean isMultiColumn()
        {
            return true;
        }
    }

    public static class InWithValues extends SingleColumnRestriction.InWithValues
    {
        public InWithValues(List<Term> values)
        {
            super(values);
        }

        public boolean isMultiColumn()
        {
            return true;
        }
    }

    public static class InWithMarker extends SingleColumnRestriction.InWithMarker
    {
        public InWithMarker(AbstractMarker marker)
        {
            super(marker);
        }

        public boolean isMultiColumn()
        {
            return true;
        }
    }

    public static class Slice extends SingleColumnRestriction.Slice
    {
        public Slice(boolean onToken)
        {
            super(onToken);
        }

        public boolean isMultiColumn()
        {
            return true;
        }
    }
}
