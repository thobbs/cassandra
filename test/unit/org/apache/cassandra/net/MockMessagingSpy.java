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
package org.apache.cassandra.net;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.AssertionFailedError;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * Allows inspecting the behavior of mocked messaging by observing {@link MatcherResponse}.
 */
public class MockMessagingSpy
{
    private static final Logger logger = LoggerFactory.getLogger(MockMessagingSpy.class);

    private boolean capture;
    public List<MessageOut> capturedMessages = new LinkedList<>();

    public int messagesIntercepted = 0;
    public int mockedMessageResponses = 0;

    private volatile SimpleCondition blockReceivedCondition;
    private volatile SimpleCondition blockResponsesCondition;

    private int waitUntilNumberOfReceivedMessages = -1;
    private int waitUntilMockedResponses = -1;


    MockMessagingSpy(boolean capture)
    {
        this.capture = capture;
    }

    /**
     * Enables capturing of outgoing messages that have been intercepted
     */
    public MockMessagingSpy capture()
    {
        capture = true;
        return this;
    }

    /**
     * Blocks until one intercepted message has been received.
     */
    public MockMessagingSpy receive1() throws InterruptedException
    {
        return receiveN(1, 1, TimeUnit.MINUTES);
    }

    /**
     * Blocks until n intercepted message have been received.
     */
    public MockMessagingSpy receiveN(int numberOfReceivedMessages) throws InterruptedException
    {
        return receiveN(numberOfReceivedMessages, 1, TimeUnit.MINUTES);
    }

    /**
     * Blocks until n intercepted message have been received.
     */
    public MockMessagingSpy receiveN(int numberOfReceivedMessages, long time, TimeUnit unit) throws InterruptedException
    {
        waitUntilNumberOfReceivedMessages = messagesIntercepted + numberOfReceivedMessages;
        blockReceivedCondition = new SimpleCondition();
        if(!blockReceivedCondition.await(time, unit))
            throw new AssertionFailedError("Timeout while waiting for messages");
        return this;
    }

    /**
     * Blocks specified time interval and raises an error in case of any received messages.
     */
    public MockMessagingSpy receiveNoMsg(long time, TimeUnit unit) throws InterruptedException
    {
        try
        {
            receiveN(1, time, unit);
        }
        catch (AssertionFailedError e)
        {
            return this;
        }
        throw new AssertionFailedError("Received unexpected message");
    }

    /**
     * Blocks until one response has been send in return of an intercepted message.
     */
    public MockMessagingSpy respond1() throws InterruptedException
    {
        return respondN(1, 1, TimeUnit.MINUTES);
    }

    /**
     * Blocks until n responses have been send in return of intercepted messages.
     */
    public MockMessagingSpy respondN(int numberOfMockedResponses) throws InterruptedException
    {
        return respondN(numberOfMockedResponses, 1, TimeUnit.MINUTES);
    }

    /**
     * Blocks until n responses have been send in return of intercepted messages.
     */
    public MockMessagingSpy respondN(int numberOfMockedResponses, long time, TimeUnit unit) throws InterruptedException
    {
        waitUntilMockedResponses = mockedMessageResponses + numberOfMockedResponses;
        blockResponsesCondition = new SimpleCondition();
        if(!blockResponsesCondition.await(time, unit))
            throw new AssertionFailedError("Timeout while waiting for messages");
        return this;
    }

    void matchingMessage(MessageOut<?> message)
    {
        messagesIntercepted++;
        logger.trace("Received matching message: {}", message);
        if (capture) capturedMessages.add(message);
        if (waitUntilNumberOfReceivedMessages != -1 && messagesIntercepted >= waitUntilNumberOfReceivedMessages) blockReceivedCondition.signalAll();
    }

    void matchingResponse(MessageIn<?> response)
    {
        mockedMessageResponses++;
        logger.trace("Responding to intercepted message: {}", response);
        if (waitUntilMockedResponses != -1 && mockedMessageResponses >= waitUntilMockedResponses) blockResponsesCondition.signalAll();
    }
}
