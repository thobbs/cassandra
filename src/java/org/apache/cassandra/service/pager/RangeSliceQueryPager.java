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

import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pages a RangeSliceCommand whose predicate is a slice query.
 *
 * Note: this only work for CQL3 queries for now (because thrift queries expect
 * a different limit on the rows than on the columns, which complicates it).
 */
public class RangeSliceQueryPager extends AbstractQueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(RangeSliceQueryPager.class);

    private final PartitionRangeReadCommand command;
    private volatile DecoratedKey lastReturnedKey;
    private volatile Clustering lastReturnedClustering;

    public RangeSliceQueryPager(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        super(consistencyLevel, localQuery, command.metadata(), command.limits());
        this.command = command;
        assert !command.isNamesQuery();

        if (state != null)
        {
            lastReturnedKey = StorageService.getPartitioner().decorateKey(state.partitionKey);
            lastReturnedClustering = LegacyLayout.decodeClustering(cfm, state.cellName);
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), LegacyLayout.encodeClustering(command.metadata(), lastReturnedClustering), maxRemaining(), remainingInPartition());
    }

    protected DataIterator queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        DataRange range;
        DataLimits limits;
        if (lastReturnedKey == null)
        {
            range = command.dataRange();
            limits = command.limits().forPaging(pageSize);
        }
        else
        {
            // We want to include the last returned key only if we haven't achieved our per-partition limit, otherwise, don't bother.
            boolean includeLastKey = remainingInPartition() > 0;
            AbstractBounds<RowPosition> bounds = makeKeyBounds(lastReturnedKey, includeLastKey);
            if (includeLastKey)
            {
                range = command.dataRange().forPaging(bounds, command.metadata().comparator, lastReturnedClustering, false);
                limits = command.limits().forPaging(pageSize, lastReturnedKey.getKey(), remainingInPartition());
            }
            else
            {
                range = command.dataRange().forSubRange(bounds);
                limits = command.limits().forPaging(pageSize);
            }
        }

        PartitionRangeReadCommand pageCmd = new PartitionRangeReadCommand(command.metadata(),
                                                                          command.nowInSec(),
                                                                          command.columnFilter(),
                                                                          limits,
                                                                          range);

        return localQuery ? pageCmd.executeLocally() : pageCmd.execute(consistencyLevel, null);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null)
        {
            lastReturnedKey = key;
            lastReturnedClustering = last.clustering().takeAlias();
        }
    }

    private AbstractBounds<RowPosition> makeKeyBounds(RowPosition lastReturnedKey, boolean includeLastKey)
    {
        AbstractBounds<RowPosition> bounds = command.dataRange().keyRange();
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return includeLastKey
                 ? new Bounds<RowPosition>(lastReturnedKey, bounds.right)
                 : new Range<RowPosition>(lastReturnedKey, bounds.right);
        }
        else
        {
            return includeLastKey
                 ? new IncludingExcludingBounds<RowPosition>(lastReturnedKey, bounds.right)
                 : new ExcludingBounds<RowPosition>(lastReturnedKey, bounds.right);
        }
    }
}
