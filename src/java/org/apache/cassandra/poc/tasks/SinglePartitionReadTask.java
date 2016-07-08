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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
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
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel.Timer;

/**
 * 1. Initiate the initial requests
 * 2. Wait to speculate, if enabled
 * 3. As soon as blockFor reqs are received, compare digests
        (TODO: no need to wait for all blockFor reqs, can start the repair on first mismatch)
 * 4. In case of digest mismatch, initiate read repair (fire DATA reqs for all the replicas with DATA missing)
 * 5. Upon receiving blockFor of DATA reqs, merge and return them all
 * TODO:
 * 6. Keep background repair in progress
 * 7. Send end-result merged mutation to all contacted replicas nodes (or, rather, only those that have a different DIGEST)
 */
public class SinglePartitionReadTask extends Task<Message.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionReadTask.class);

    private final SinglePartitionReadCommand command;
    private final Function<PartitionIterator, Message.Response> resultProcessor;
    private final ConsistencyLevel cl;

    private int blockFor;
    private Map<InetAddress, ReadResponse> responses;
    private Map<InetAddress, Boolean> pendingRequests;
    private boolean handlingDigestMismatch;
    private InetAddress extraReplica;

    private Timer requestTimer;
    private Timer speculateTimer;

    public SinglePartitionReadTask(SinglePartitionReadCommand command, ConsistencyLevel cl, Function<PartitionIterator, Message.Response> resultProcessor)
    {
        this.command = command;
        this.resultProcessor = resultProcessor;
        this.cl = cl;

        blockFor = cl.blockFor(Keyspace.open(command.metadata().ksName));
    }

    /*
     * TODO: read_repair_chance/dc_local_read_repair_chance speculation
     * TODO: short read protection
     * TODO: background read repair
     */

    @Override
    public Status start(EventLoop eventLoop)
    {
        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        List<InetAddress> liveReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.partitionKey());
        List<InetAddress> targetReplicas = cl.filterForQuery(keyspace, liveReplicas, ReadRepairDecision.NONE);

        // throw UAE early if we don't have enough replicas.
        cl.assureSufficientLiveNodes(keyspace, targetReplicas);

        responses = Maps.newHashMapWithExpectedSize(targetReplicas.size() + 1);
        pendingRequests = Maps.newHashMapWithExpectedSize(targetReplicas.size() + 1);

        // decide if we are going to speculate
        SpeculativeRetryParam retry = command.metadata().params.speculativeRetry;
        if (liveReplicas.size() > targetReplicas.size())
        {
            InetAddress edgeReplica = liveReplicas.get(targetReplicas.size());
            switch (retry.kind())
            {
                case ALWAYS:
                    scheduleDataRequest(eventLoop, edgeReplica); // TODO: should send DATA to the 2nd replica, not the last (if QUORUM)
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

        // set the timer (shared for all reqs)
        requestTimer = eventLoop.scheduleTimer(this, DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);

        // schedule local and remote read requests
        scheduleRequests(eventLoop, targetReplicas);

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
        pendingRequests.put(replica, false);
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        if (replica.equals(localAddress))
        {
            if (command.metadata().ksName.contains("system"))
            {
                Adapters.readLocally(eventLoop, this, command);
            }
            else
            {
                try (ReadExecutionController executionController = command.executionController();
                     UnfilteredPartitionIterator iterator = command.executeLocallyAsync(executionController, this))
                {
                    ReadResponse response = command.createResponse(iterator);
                    boolean isCompleted = command.complete();
                    assert isCompleted;
                    handleResponse(eventLoop, localAddress, response);
                }
            }
        }
        else
        {
            Adapters.sendMessage(eventLoop, this, command.createMessage(MessagingService.instance().getVersion(replica)), replica);
        }
    }

    // always remote
    private void scheduleDigestRequest(EventLoop eventLoop, InetAddress replica)
    {
        Adapters.sendMessage(eventLoop, this, command.copy().setIsDigestQuery(true).createMessage(MessagingService.instance().getVersion(replica)), replica);
        pendingRequests.put(replica, true);
    }

    @Override
    public Status resume(EventLoop eventLoop)
    {
        throw new UnsupportedOperationException();
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
        // a DIGEST response for a replica we had already issued a DATA request for (on digest mismatch)
        if (response.isDigestResponse() != pendingRequests.get(from))
            return Status.PROCESSING;

        pendingRequests.remove(from);
        responses.put(from, response);

        return handlingDigestMismatch
             ? handleDigestMismatchResponse(eventLoop, from, response)
             : handleInitialResponse(eventLoop, from, response);
    }

    private Status handleInitialResponse(EventLoop eventLoop, InetAddress from, ReadResponse response)
    {
        if (responses.size() < blockFor || !hasDataResponses())
            return Status.PROCESSING;

        if (speculateTimer != null)
            speculateTimer.cancel();

        if (responses.size() == 1 || allDigestsMatch(responses.values()))
        {
            requestTimer.cancel();
            ReadResponse dataResponse = Iterables.tryFind(responses.values(), ReadResponse::isDataResponse).get();
            PartitionIterator iterator = UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
            return complete(resultProcessor.apply(iterator));
        }

        /*
         * more than 1 response, not all digests are matching;
         * issue full DATA requests to the replicas for which we only have digests, or still expecting digests
         */

        for (Map.Entry<InetAddress, ReadResponse> entry : responses.entrySet())
        {
            if (entry.getValue().isDigestResponse())
            {
                scheduleDataRequest(eventLoop, entry.getKey());
                responses.remove(entry.getKey());
            }
        }

        for (Map.Entry<InetAddress, Boolean> entry : pendingRequests.entrySet())
            if (entry.getValue())
                scheduleDataRequest(eventLoop, entry.getKey());

        handlingDigestMismatch = true;

        return Status.PROCESSING;
    }

    // Await for blockFor DATA responses, merge them and return the result
    private Status handleDigestMismatchResponse(EventLoop eventLoop, InetAddress from, ReadResponse response)
    {
        if (responses.size() < blockFor)
            return Status.PROCESSING;

        List<UnfilteredPartitionIterator> iterators = new ArrayList<>(responses.size());
        responses.values().forEach(r -> iterators.add(r.makeIterator(command)));

        // Even though every responses should honor the limit, we might have more than requested post reconciliation,
        // so ensure we're respecting the limit.
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);
        return complete(resultProcessor.apply(counter.applyTo(UnfilteredPartitionIterators.mergeAndFilter(iterators, command.nowInSec(), new VoidMergeListener()))));
    }

    private boolean allDigestsMatch(Collection<ReadResponse> responses)
    {
        ByteBuffer digest = null;
        for (ReadResponse response : responses)
        {
            ByteBuffer newDigest = response.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                return false;
        }

        return true;
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
        return fail(new ReadTimeoutException(cl, responses.size(), blockFor, hasDataResponses()));
    }

    private boolean hasDataResponses()
    {
        return Iterables.any(responses.values(), ReadResponse::isDataResponse);
    }

    private static class VoidMergeListener implements UnfilteredPartitionIterators.MergeListener
    {

        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new UnfilteredRowIterators.MergeListener()
            {
                public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                {
                }

                public void onMergedRows(Row merged, Row[] versions)
                {
                }

                public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
                {
                }

                public void close()
                {
                }
            };
        }

        public void close()
        {
        }
    }
}
