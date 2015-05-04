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
package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Objects;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;

/**
 * Common interface to single partition queries (by slice and by name).
 *
 * For use by MultiPartitionPager.
 */
public class SinglePartitionPager extends AbstractQueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionPager.class);

    private final SinglePartitionReadCommand<?> command;
    private final ClientState cstate;

    private volatile Clustering lastReturned;

    public SinglePartitionPager(SinglePartitionReadCommand<?> command, ConsistencyLevel consistencyLevel, ClientState cstate, boolean localQuery, PagingState state)
    {
        super(consistencyLevel, localQuery, command.metadata(), command.limits());
        this.command = command;
        this.cstate = cstate;

        if (state != null)
        {
            lastReturned = LegacyLayout.decodeClustering(cfm, state.cellName);
            restoreState(command.partitionKey(), state.remaining, state.remainingInPartition);
        }
    }

    public ByteBuffer key()
    {
        return command.partitionKey().getKey();
    }

    public DataLimits limits()
    {
        return command.limits();
    }

    public PagingState state()
    {
        return lastReturned == null
             ? null
             : new PagingState(null, LegacyLayout.encodeClustering(command.metadata(), lastReturned), maxRemaining(), remainingInPartition());
    }

    protected DataIterator queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestValidationException, RequestExecutionException
    {
        SinglePartitionReadCommand pageCmd = command.forPaging(lastReturned, pageSize);
        return localQuery ? pageCmd.executeLocally() : pageCmd.execute(consistencyLevel, cstate);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null)
            lastReturned = last.clustering().takeAlias();
    }
}
