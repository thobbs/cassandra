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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.poc.WriteTask;
import org.apache.cassandra.poc.events.Event;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public abstract class AbstractWriteResponseHandler<T> implements IAsyncCallbackWithFailure<T>
{
    protected static final Logger logger = LoggerFactory.getLogger( AbstractWriteResponseHandler.class );

    private final SimpleCondition condition = new SimpleCondition();
    protected final Keyspace keyspace;
    protected final long start;
    protected final Collection<InetAddress> naturalEndpoints;
    public final ConsistencyLevel consistencyLevel;
    protected final Runnable callback;
    protected final Collection<InetAddress> pendingEndpoints;
    protected final WriteType writeType;
    protected final WriteTask.SubTask mutationTask;
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
        = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    private volatile int failures = 0;

    /**
     * @param callback A callback to be called when the write is successful.
     */
    protected AbstractWriteResponseHandler(Keyspace keyspace,
                                           Collection<InetAddress> naturalEndpoints,
                                           Collection<InetAddress> pendingEndpoints,
                                           ConsistencyLevel consistencyLevel,
                                           Runnable callback,
                                           WriteType writeType,
                                           WriteTask.SubTask mutationTask)
    {
        this.keyspace = keyspace;
        this.pendingEndpoints = pendingEndpoints;
        this.start = System.nanoTime();
        this.consistencyLevel = consistencyLevel;
        this.naturalEndpoints = naturalEndpoints;
        this.callback = callback;
        this.writeType = writeType;
        this.mutationTask = mutationTask;
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long requestTimeout = writeType == WriteType.COUNTER
                            ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                            : DatabaseDescriptor.getWriteRpcTimeout();

        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);

        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new WriteTimeoutException(writeType, consistencyLevel, acks, blockedFor);
        }

        if (totalBlockFor() + failures > totalEndpoints())
        {
            throw new WriteFailureException(consistencyLevel, ackCount(), failures, totalBlockFor(), writeType);
        }
    }

    /** 
     * @return the minimum number of endpoints that must reply. 
     */
    protected int totalBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return consistencyLevel.blockFor(keyspace) + pendingEndpoints.size();
    }

    /** 
     * @return the total number of endpoints the request has been sent to. 
     */
    protected int totalEndpoints()
    {
        return naturalEndpoints.size() + pendingEndpoints.size();
    }

    public Iterable<InetAddress> getTargetedEndpoints()
    {
        return Iterables.concat(naturalEndpoints, pendingEndpoints);
    }

    /**
     * @return true if the message counts towards the totalBlockFor() threshold
     */
    protected boolean waitingFor(InetAddress from)
    {
        return true;
    }

    /**
     * @return number of responses received
     */
    protected abstract int ackCount();

    /** null message means "response from local write" */
    public abstract void response(MessageIn<T> msg, int id);

    /** Called when a local write has completed */
    public abstract AckResponse localResponse();

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(keyspace, Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive));
    }

    protected void signal()
    {
        if (mutationTask != null)
        {
            // TPC write path
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            Event event;
            if (blockedFor + failures > totalEndpoints())
            {
                event = new WriteTask.WriteTimeoutEvent(mutationTask, writeType, consistencyLevel, acks, blockedFor);
            }
            else if (totalBlockFor() + failures > totalEndpoints())
            {
                event = new WriteTask.WriteFailureEvent(mutationTask, writeType, consistencyLevel, acks, blockedFor, failures);
            }
            else
            {
                event = new WriteTask.WriteSuccessEvent(mutationTask);
            }
            mutationTask.getTask().eventLoop().emitEvent(event);
        }
        else
        {
            // SEDA write path
            condition.signalAll();
            if (callback != null)
                callback.run();
        }
    }

    /**
     * Called when the final required ack is received from the local node.  Token-aware writes at CL.ONE are common
     * enough that this can be optimized for by avoiding emitting an event on the event loop.
     * @return null if the consistency level has been met, a WriteTimeoutException
     */
    protected WriteFailureException handleLocalFinalAck()
    {
        assert mutationTask != null;

        int blockedFor = totalBlockFor();
        int acks = ackCount();
        if (blockedFor + failures > totalEndpoints())
            return new WriteFailureException(consistencyLevel, acks, failures, blockedFor, writeType);
        else
            return null;
    }

    @Override
    public void onFailure(InetAddress from, int id)
    {
        logger.trace("Got failure from {}", from);

        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        if (totalBlockFor() + n > totalEndpoints())
            signal();
    }

    public static class AckResponse
    {
        public final boolean complete;
        public final WriteFailureException exc;

        AckResponse(boolean complete, WriteFailureException exc)
        {
            this.complete = complete;
            this.exc = exc;
        }
    }
}
