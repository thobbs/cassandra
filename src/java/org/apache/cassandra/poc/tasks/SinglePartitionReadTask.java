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
package org.apache.cassandra.poc.tasks;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.poc.Adapters;
import org.apache.cassandra.poc.EventLoop;
import org.apache.cassandra.poc.Task;
import org.apache.cassandra.poc.events.Event;
import org.apache.cassandra.poc.events.LocalReadResponse;
import org.apache.cassandra.poc.events.RemoteResponse;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import uk.co.real_logic.agrona.TimerWheel.Timer;

public class SinglePartitionReadTask extends Task<PartitionIterator>
{
    private Timer requestTimer;
    private Timer speculateTimer;

    private List<ReadResponse> responses;

    private final SinglePartitionReadCommand command;
    private final ConsistencyLevel cl;

    private InetAddress extraReplica;

    public SinglePartitionReadTask(SinglePartitionReadCommand command, ConsistencyLevel cl)
    {
        this.command = command;
        this.cl = cl;
    }

    /*
     * TODO: short read protection
     * TODO: read repair
     */

    @Override
    public Status start(EventLoop eventLoop)
    {
        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        List<InetAddress> liveReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.partitionKey());
        List<InetAddress> targetReplicas = cl.filterForQuery(keyspace, liveReplicas, ReadRepairDecision.NONE);

        // throw UAE early if we don't have enough replicas.
        cl.assureSufficientLiveNodes(keyspace, targetReplicas);

        responses = new ArrayList<>(targetReplicas.size() + 1);

        // decide if we are going to speculate
        SpeculativeRetryParam retry = command.metadata().params.speculativeRetry;
        if (liveReplicas.size() > targetReplicas.size())
        {
            InetAddress edgeReplica = liveReplicas.get(targetReplicas.size());
            switch (retry.kind())
            {
                case ALWAYS:
                    targetReplicas.add(edgeReplica);
                    break;
                case PERCENTILE:
                case CUSTOM:
                    extraReplica = edgeReplica;
                    long threshold = retry.kind() == SpeculativeRetryParam.Kind.PERCENTILE
                                   ? Schema.instance.getColumnFamilyStoreInstance(command.metadata().cfId).sampleLatencyNanos
                                   : (long) retry.threshold();
                    speculateTimer = eventLoop.scheduleTimer(this, threshold, TimeUnit.NANOSECONDS);
                    break;
            }
        }

        // schedule local and remote read requests
        scheduleRequests(eventLoop, targetReplicas);

        // set the timer (shared for all reqs)
        requestTimer = eventLoop.scheduleTimer(this, DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);

        return Status.PROCESSING;
    }

    // schedule local and remote data and digest requests
    private void scheduleRequests(EventLoop eventLoop, List<InetAddress> replicas)
    {
        if (replicas.size() == 1)
        {
            scheduleDataRequest(eventLoop, replicas.get(0));
            return;
        }

        boolean scheduledDataRequest = false;

        // if local node is one of the replicas, make sure it's the one that does the full DATA request
        boolean hasLocalAsReplica = Iterables.any(replicas, Predicates.equalTo(FBUtilities.getBroadcastAddress()));
        if (hasLocalAsReplica)
        {
            scheduleDataRequest(eventLoop, FBUtilities.getBroadcastAddress());
            scheduledDataRequest = true;
        }

        for (InetAddress replica : replicas)
        {
            if (replica.equals(FBUtilities.getBroadcastAddress()))
                continue;

            if (scheduledDataRequest)
            {
                scheduleDigestRequest(eventLoop, replica);
            }
            else
            {
                scheduleDataRequest(eventLoop, replica);
                scheduledDataRequest = true;
            }
        }
    }

    private void scheduleDataRequest(EventLoop eventLoop, InetAddress replica)
    {
        if (replica.equals(FBUtilities.getBroadcastAddress()))
            Adapters.readLocally(eventLoop, this, command);
        else
            Adapters.sendMessage(eventLoop, this, command.createMessage(MessagingService.instance().getVersion(replica)), replica);
    }

    // always remote
    private void scheduleDigestRequest(EventLoop eventLoop, InetAddress replica)
    {
        Adapters.sendMessage(eventLoop, this, command.copy().setIsDigestQuery(true).createMessage(MessagingService.instance().getVersion(replica)), replica);
    }

    @Override
    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        if (event instanceof LocalReadResponse)
        {
            return handleResponse(eventLoop, FBUtilities.getBroadcastAddress(), ((LocalReadResponse) event).response);
        }
        else if (event instanceof RemoteResponse)
        {
            @SuppressWarnings("unchecked")
            MessageIn<ReadResponse> message = (MessageIn<ReadResponse>) ((RemoteResponse) event).message;
            return handleResponse(eventLoop, message.from, message.payload);
        }

        throw new IllegalArgumentException();
    }

    private Status handleResponse(EventLoop eventLoop, InetAddress from, ReadResponse response)
    {
        responses.add(response);

        int blockFor = cl.blockFor(Keyspace.open(command.metadata().ksName));
        if (responses.size() < blockFor)
            return Status.PROCESSING;

        if (speculateTimer != null)
            speculateTimer.cancel();
        requestTimer.cancel();

        if (responses.size() == 1)
            return complete(UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec()));

        List<UnfilteredPartitionIterator> iterators = new ArrayList<>(responses.size());
        responses.forEach(r -> iterators.add(r.makeIterator(command)));

        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);

        // TODO FIXME: null is a lie
        return complete(counter.applyTo(UnfilteredPartitionIterators.mergeAndFilter(iterators, command.nowInSec(), null)));
    }

    @Override
    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        return timer == speculateTimer ? speculate(eventLoop) : timeOut();
    }

    private Status speculate(EventLoop eventLoop)
    {
        if (hasDataResponses())
            scheduleDigestRequest(eventLoop, extraReplica);
        else
            scheduleDataRequest(eventLoop, extraReplica);

        speculateTimer = null;

        return Status.PROCESSING;
    }

    private Status timeOut()
    {
        int blockFor = cl.blockFor(Keyspace.open(command.metadata().ksName));
        return fail(new ReadTimeoutException(cl, responses.size(), blockFor, hasDataResponses()));
    }

    private boolean hasDataResponses()
    {
        return Iterables.any(responses, r -> !r.isDigestResponse());
    }
}
