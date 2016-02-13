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

import org.apache.cassandra.poc.events.Event;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.TimerWheel.Timer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

public final class EventLoop implements Runnable
{
    private volatile boolean isRunning = false;

    private final ManyToOneConcurrentArrayQueue<Task> newTasks;
    private final ManyToOneConcurrentArrayQueue<Event> events;
    private final TimerWheel timers;

    private ArrayList<Task> rescheduledTasks;
    private ArrayList<Task> nextRescheduledTasks;

    // cache instances of Consumer-s that are capturing lambdas otherwise
    private final Consumer<Task> taskInitializer;
    private final Consumer<Task> taskResumer;
    private final Consumer<Event> eventHandler;

    public EventLoop()
    {
        newTasks = new ManyToOneConcurrentArrayQueue<>(1024 * 1024);
        rescheduledTasks = new ArrayList<>(1024);
        nextRescheduledTasks = new ArrayList<>(1024);
        events = new ManyToOneConcurrentArrayQueue<>(1024 * 1024);
        timers = new TimerWheel(1, TimeUnit.MILLISECONDS, 256);

        taskInitializer = this::initializeTask;
        taskResumer = this::resumeTask;
        eventHandler = this::handleEvent;
    }

    public void run()
    {
        isRunning = true;
        IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        while (isRunning)
            idleStrategy.idle(cycle());
    }

    /**
     * Run a single cycle of the event loop:
     *     1. initialize all newly enqueued newTasks
     *     2. resume all rescheduled tasks
     *     3. handle all new incoming events
     *     4. trigger all expired timers
     * @return number of new newTasks initialized, resumed, events handled, and timers expired during this cycle
     */
    int cycle()
    {
        return newTasks.drain(taskInitializer) + rescheduleTasks() + events.drain(eventHandler) + timers.expireTimers();
    }

    void stop()
    {
        isRunning = false;
    }

    public boolean scheduleTask(Task task)
    {
        return newTasks.offer(task);
    }

    public Timer scheduleTimer(Task task, long delay, TimeUnit unit)
    {
        Timer timer = timers.newBlankTimer();
        timers.rescheduleTimeout(delay, unit, timer, () ->  handleTimeout(task, timer));
        return timer;
    }

    public boolean emitEvent(Event event)
    {
        return events.offer(event);
    }

    private int rescheduleTasks()
    {
        ArrayList<Task> tasks = rescheduledTasks;

        rescheduledTasks = nextRescheduledTasks;
        nextRescheduledTasks = tasks;

        int size = tasks.size();
        tasks.forEach(taskResumer);
        tasks.clear();
        return size;
    }

    private void initializeTask(Task task)
    {
        task.initialize(this);
    }

    private void resumeTask(Task task)
    {
        if (task.dispatchResume(this) == Task.Status.RESCHEDULED)
            reschedule(task);
    }

    private void handleEvent(Event event)
    {
        if (event.task().dispatchEvent(this, event) == Task.Status.RESCHEDULED)
            reschedule(event.task());
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
