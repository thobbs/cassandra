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
package org.apache.cassandra.poc;

import org.apache.cassandra.poc.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel.Timer;

import java.util.ArrayList;
import java.util.function.Consumer;

public abstract class Task<T>
{
    private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

    public enum Status
    {
        NEW, // new task, not initialized yet
        PROCESSING, // waiting for events to process
        RESCHEDULED, // not waiting for new events, but in the middle of a long running task, giving others a chance to run
        COMPLETED, // all done
        FAILED; // exceptional exit

        public boolean isFinal()
        {
            return this == COMPLETED || this == FAILED;
        }
    }

    // these 3 fields here for testing only for now (and will probably go)
    private volatile boolean hasCompleted = false;
    private volatile T result; // can be Void null
    private volatile Throwable exception;
    private final ArrayList<Consumer<T>> callbacks = new ArrayList<>(1);

    private Status status = Status.NEW;

    protected EventLoop eventLoop;

    public abstract Status start(EventLoop eventLoop);

    public abstract Status resume(EventLoop eventLoop);

    public abstract Status handleEvent(EventLoop eventLoop, Event event);

    public abstract Status handleTimeout(EventLoop eventLoop, Timer timer);

    public void cleanup(EventLoop eventLoop)
    {
    }

    public void onComplete(T result)
    {
    }

    public void onFailure(Throwable t)
    {
    }

    public void addCallback(Consumer<T> callback)
    {
        callbacks.add(callback);
    }

    public EventLoop eventLoop()
    {
        return eventLoop;
    }

    final Status status()
    {
        return status;
    }

    protected final Status complete(T result)
    {
        if (hasCompleted || exception != null)
            throw new IllegalStateException();

        this.result = result;
        status = Status.COMPLETED;
        hasCompleted = true;
        onComplete(result);

        // TODO wrap in try/catch
        for (Consumer<T> callback : callbacks)
            callback.accept(result);

        return status;
    }

    public final boolean hasCompleted()
    {
        return hasCompleted;
    }

    public final T result()
    {
        if (hasCompleted)
            throw new IllegalStateException();

        return result;
    }

    protected final Status fail(Throwable t)
    {
        if (hasCompleted || exception != null)
            throw new IllegalStateException();

        this.exception = t;
        status = Status.FAILED;
        onFailure(t);
        return status;
    }

    final boolean hasFailed()
    {
        return exception != null;
    }

    final Throwable exception()
    {
        return exception;
    }

    final Status dispatchStart(EventLoop eventLoop)
    {
        try
        {
            status = start(eventLoop);
        }
        catch (Throwable t)
        {
            logger.warn("Unhandled exception while starting Task", t);
            fail(t);
        }

        maybeCleanup(eventLoop);
        return status;
    }

    final Status dispatchResume(EventLoop eventLoop)
    {
        if (status.isFinal())
            return status;

        try
        {
            status = resume(eventLoop);
        }
        catch (Throwable t)
        {
            fail(t);
        }

        maybeCleanup(eventLoop);
        return status;
    }

    final Status dispatchEvent(EventLoop eventLoop, Event event)
    {
        if (status.isFinal())
            return status;

        try
        {
            status = handleEvent(eventLoop, event);
        }
        catch (Throwable t)
        {
            logger.warn("Unhandled exception while handling event", t);
            fail(t);
        }

        maybeCleanup(eventLoop);
        return status;
    }

    final Status dispatchTimeout(EventLoop eventLoop, Timer timer)
    {
        if (status.isFinal())
            return status;

        try
        {
            status = handleTimeout(eventLoop, timer);
        }
        catch (Throwable t)
        {
            fail(t);
        }

        maybeCleanup(eventLoop);
        return status;
    }

    private void maybeCleanup(EventLoop eventLoop)
    {
        if (!status.isFinal())
            return;

        try
        {
            cleanup(eventLoop);
        }
        catch (Throwable t)
        {
            // TODO: log the error?
        }
    }
}
