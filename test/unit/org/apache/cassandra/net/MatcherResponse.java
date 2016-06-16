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

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sends a response for an incoming message with a matching {@link Matcher}.
 * The actual behavior by any instance of this class can be inspected by
 * interacting with the returned {@link MockMessagingSpy}.
 */
public class MatcherResponse
{
    private final Matcher<?> matcher;
    private final Set<Integer> sendResponses = new HashSet<>();
    private final MockMessagingSpy spy = new MockMessagingSpy(false);
    private final AtomicInteger limitCounter = new AtomicInteger(Integer.MAX_VALUE);

    MatcherResponse(Matcher<?> matcher)
    {
        this.matcher = matcher;
    }

    public MockMessagingSpy respond(MessageIn<?> message)
    {
        return respondN(message, Integer.MAX_VALUE);
    }

    public MockMessagingSpy respondN(final MessageIn<?> response, int limit)
    {
        limitCounter.set(limit);

        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                // prevent outgoing message from being send in case matcher indicates a match
                // and instead send the mocked response
                if (matcher.matches(message, to))
                {
                    spy.matchingMessage(message);

                    if (limitCounter.decrementAndGet() < 0)
                    {
                        return false;
                    }

                    synchronized (sendResponses)
                    {
                        // I'm not sure about retry semantics regarding message/ID relationships, but I assume
                        // sending a message mutliple times using the same ID shouldn't happen..
                        assert !sendResponses.contains(id) : "ID re-use for outgoing message";
                        sendResponses.add(id);
                    }
                    MessagingService.instance().getRegisteredCallback(id).callback.response(response);
                    spy.matchingResponse(response);
                    return false;
                }
                return true;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return true;
            }
        });

        return spy;
    }
}
