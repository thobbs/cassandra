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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.netty.channel.SingleThreadEventLoop;
import org.apache.cassandra.poc.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.TimerWheel.Timer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

public final class EventLoop implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EventLoop.class);

    // TODO: come up with a test-based number
    private static final long QUANTUM = TimeUnit.MILLISECONDS.toNanos(1);

    private volatile boolean isRunning = false;

    private final ManyToOneConcurrentArrayQueue<Task> newTasks;
    private final ManyToOneConcurrentArrayQueue<Event> events;
    private final TimerWheel timers;

    private ArrayList<Task> rescheduledTasks;
    private ArrayList<Task> nextRescheduledTasks;

    // cache instances of Consumer-s that are capturing lambdas otherwise
    private final Consumer<Task> taskStarter;
    private final Consumer<Task> taskRescheduler;
    private final Consumer<Event> eventHandler;

    private SingleThreadEventLoop nettyEventLoop;

    public EventLoop()
    {
        newTasks = new ManyToOneConcurrentArrayQueue<>(1024 * 1024);
        rescheduledTasks = new ArrayList<>(1024);
        nextRescheduledTasks = new ArrayList<>(1024);
        events = new ManyToOneConcurrentArrayQueue<>(1024 * 1024);
        timers = new TimerWheel(1, TimeUnit.MILLISECONDS, 256);

        taskStarter = this::startTask;
        taskRescheduler = this::rescheduleTask;
        eventHandler = this::handleEvent;
    }

    public void setNettyExecutor(SingleThreadEventLoop nettyEventLoop)
    {
        this.nettyEventLoop = nettyEventLoop;
    }

    public void run()
    {
        isRunning = true;
        // spin once, yield 100 times, start sleeping from 0.5 to 10 millis
        IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 100, TimeUnit.MICROSECONDS.toNanos(500), TimeUnit.MILLISECONDS.toNanos(10));
        while (isRunning)
            idleStrategy.idle(cycle());
    }

    /**
     * Run a single cycle of the event loop:
     *     1. start all newly enqueued newTasks
     *     2. resume all rescheduled tasks
     *     3. handle all new incoming events
     *     4. trigger all expired timers
     * @return number of new newTasks started, resumed, events handled, and timers expired during this cycle
     */
    public int cycle()
    {
        int returnValue = newTasks.drain(taskStarter) + rescheduleTasks() + events.drain(eventHandler) + timers.expireTimers();
        nettyEventLoop.execute(this::cycle);
        return returnValue;
    }

    void stop()
    {
        isRunning = false;
    }

    public void scheduleTask(Task task)
    {
        boolean scheduled;
        do
        {
            scheduled = newTasks.offer(task);
        }
        while (!scheduled);
    }

    public Timer scheduleTask(Task task, long delay, TimeUnit unit)
    {
        return timers.newTimeout(delay, unit, () -> scheduleTask(task));
    }

    public Timer scheduleTimer(Task task, long delay, TimeUnit unit)
    {
        Timer timer = timers.newBlankTimer();
        timers.rescheduleTimeout(delay, unit, timer, () ->  handleTimeout(task, timer));
        return timer;
    }

    public void emitEvent(Event event)
    {
        boolean emitted;
        do
        {
            emitted = events.offer(event);
        }
        while (!emitted);
    }

    private int rescheduleTasks()
    {
        ArrayList<Task> tasks = rescheduledTasks;

        rescheduledTasks = nextRescheduledTasks;
        nextRescheduledTasks = tasks;

        int size = tasks.size();
        tasks.forEach(taskRescheduler);
        tasks.clear();
        return size;
    }

    private void rescheduleTask(Task task)
    {
        Task.Status status;
        long startedAt = System.nanoTime();

        do
        {
            status = task.dispatchResume(this);
        }
        while (status == Task.Status.RESCHEDULED && (System.nanoTime() - startedAt) < QUANTUM);

        if (status == Task.Status.RESCHEDULED)
            reschedule(task);
    }

    private void startTask(Task task)
    {
        if (task.start(this) == Task.Status.RESCHEDULED)
            reschedule(task);
    }

    private void handleEvent(Event event)
    {
        try
        {
            if (event.task().dispatchEvent(this, event) == Task.Status.RESCHEDULED)
                reschedule(event.task());
        }
        catch (Throwable t)
        {
            logger.error("Unhandled exception in event loop: {}", t);

            // TODO for early testing purposes it's easier to exit on an unhandled error, but eventually we don't want to do this
            // JVMStabilityInspector.inspectThrowable(t);
            System.exit(1);
        }
    }

    private void handleTimeout(Task task, Timer timer)
    {
        if (task.dispatchTimeout(this, timer) == Task.Status.RESCHEDULED)
            reschedule(task);
    }

    private void reschedule(Task task)
    {
        rescheduledTasks.add(task);
    }
}
