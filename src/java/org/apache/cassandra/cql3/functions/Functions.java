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
package org.apache.cassandra.cql3.functions;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class Functions
{
    private Functions() {}

    private static final String TOKEN_FUNCTION_NAME = "token";

    // If we ever allow this to be populated at runtime, this will need to be thread safe.
    // private static final ArrayListMultimap<String, Function.Factory> declared = ArrayListMultimap.create();
    private static final ArrayListMultimap<String, Function> declared = ArrayListMultimap.create();

    static
    {
        // All method sharing the same name must have the same returnType. We could find a way to make that clear.
        declare(TimeuuidFcts.nowFct);
        declare(TimeuuidFcts.minTimeuuidFct);
        declare(TimeuuidFcts.maxTimeuuidFct);
        declare(TimeuuidFcts.dateOfFct);
        declare(TimeuuidFcts.unixTimestampOfFct);
        declare(UuidFcts.uuidFct);

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // Note: because text and varchar ends up being synonymous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type == CQL3Type.Native.VARCHAR || type == CQL3Type.Native.BLOB)
                continue;

            declare(BytesConversionFcts.makeToBlobFunction(type.getType()));
            declare(BytesConversionFcts.makeFromBlobFunction(type.getType()));
        }
        declare(BytesConversionFcts.VarcharAsBlobFct);
        declare(BytesConversionFcts.BlobAsVarcharFact);
    }

    private static void declare(Function fun)
    {
        declared.put(fun.name(), fun);
    }

    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                receiverCf,
                new ColumnIdentifier("arg" + i +  "(" + fun.name().toLowerCase() + ")", true),
                fun.argTypes().get(i));
    }

    public static Function get(String keyspace, String name, List<? extends AssignmentTestable> providedArgs,
                               String receiverKs, String receiverCf)
            throws InvalidRequestException
    {
        return get(keyspace, name, providedArgs, receiverKs, receiverCf, null);
    }

    public static Function get(String keyspace,
                               String name,
                               List<? extends AssignmentTestable> providedArgs,
                               String receiverKs,
                               String receiverCf,
                               AbstractType receiverType)
            throws InvalidRequestException
    {
        if (name.equalsIgnoreCase(TOKEN_FUNCTION_NAME))
        {
            return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));
        }
        else if (name.equalsIgnoreCase(FromJsonFct.NAME))
        {
            // TODO need to check for this being used incorrectly earlier, otherwise this will get hit
            assert receiverType != null : "fromJson should not be called without a known receiver type";
            return FromJsonFct.getInstance(receiverType);
        }

        List<Function> candidates = declared.get(name);
        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.get(0);
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
        }

        List<Function> compatibles = null;
        for (Function toTest : candidates)
        {
            AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKs, receiverCf);
            switch (r)
            {
                case EXACT_MATCH:
                    // We always favor exact matches
                    return toTest;
                case WEAKLY_ASSIGNABLE:
                    if (compatibles == null)
                        compatibles = new ArrayList<>();
                    compatibles.add(toTest);
                    break;
            }
        }

        if (compatibles == null || compatibles.isEmpty())
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                    name, toString(candidates)));

        if (compatibles.size() > 1)
            throw new InvalidRequestException(String.format("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                    name, toString(compatibles)));

        return compatibles.get(0);
    }

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignmentTestable> providedArgs,
                                      String receiverKs,
                                      String receiverCf)
            throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argTypes().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.testAssignment(keyspace, expected).isAssignable())
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static AssignmentTestable.TestResult matchAguments(String keyspace,
                                                               Function fun,
                                                               List<? extends AssignmentTestable> providedArgs,
                                                               String receiverKs,
                                                               String receiverCf)
    {
        if (providedArgs.size() != fun.argTypes().size())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);
            if (provided == null)
            {
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
            if (argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static String toString(List<Function> funs)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < funs.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(funs.get(i));
        }
        return sb.toString();
    }
}
