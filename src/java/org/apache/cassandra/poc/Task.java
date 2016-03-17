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
import uk.co.real_logic.agrona.TimerWheel.Timer;

public class Task
{
    enum Status
    {
        NEW, // new task, not initialized yet
        REGULAR, // waiting for events to process
        RESCHEDULED, // not waiting for new events, but in the middle of a long running task, giving others a chance to run
        COMPLETED // all done
    }

    protected Status status = Status.NEW;

    public Status initialize(EventLoop eventLoop)
    {
        return Status.REGULAR;
    }

    public Status resume(EventLoop eventLoop)
    {
        throw new UnsupportedOperationException();
    }

    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        throw new UnsupportedOperationException();
    }

    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        throw new UnsupportedOperationException();
    }

    public void cleanup(EventLoop eventLoop)
    {
    }

    final Status dispatchResume(EventLoop eventLoop)
    {
        if (status != Status.COMPLETED)
        {
            status = resume(eventLoop);
            maybeCleanup(eventLoop);
        }

        return status;
    }

    final Status dispatchEvent(EventLoop eventLoop, Event event)
    {
        if (status != Status.COMPLETED)
        {
            status = handleEvent(eventLoop, event);
            maybeCleanup(eventLoop);
        }

        return status;
    }

    final Status dispatchTimeout(EventLoop eventLoop, Timer timer)
    {
        if (status != Status.COMPLETED)
        {
            status = handleTimeout(eventLoop, timer);
            maybeCleanup(eventLoop);
        }

        return status;
    }

    private void maybeCleanup(EventLoop eventLoop)
    {
        if (status == Status.COMPLETED)
            cleanup(eventLoop);
    }
}
