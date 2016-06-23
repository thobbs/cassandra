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
import java.util.function.BiFunction;

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
    private IMessageSink sink;

    MatcherResponse(Matcher<?> matcher)
    {
        this.matcher = matcher;
    }

    public MockMessagingSpy dontReply()
    {
        return respond((MessageIn<?>)null);
    }

    public MockMessagingSpy respond(MessageIn<?> message)
    {
        return respondN(message, Integer.MAX_VALUE);
    }

    public MockMessagingSpy respondN(final MessageIn<?> response, int limit)
    {
        return respondN((in, to) -> response, limit);
    }

    public <T, S> MockMessagingSpy respond(BiFunction<MessageOut<T>, InetAddress, MessageIn<S>> fnResponse)
    {
        return respondN(fnResponse, Integer.MAX_VALUE);
    }

    public <T, S> MockMessagingSpy respondN(BiFunction<MessageOut<T>, InetAddress, MessageIn<S>> fnResponse, int limit)
    {
        limitCounter.set(limit);

        assert sink == null: "destroy() must be called first to register new response";

        sink = new IMessageSink()
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
                    MessageIn<?> response = fnResponse.apply(message, to);
                    if (response != null)
                    {
                        CallbackInfo cb = MessagingService.instance().getRegisteredCallback(id);
                        if (cb != null)
                        {
                            cb.callback.response(response);
                        }
                        else
                        {
                            MessagingService.instance().receive(response, id);
                        }
                        spy.matchingResponse(response);
                    }
                    return false;
                }
                return true;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return true;
            }
        };
        MessagingService.instance().addMessageSink(sink);

        return spy;
    }

    /**
     * Stops currently registered response from being send.
     */
    public void destroy()
    {
        MessagingService.instance().removeMessageSink(sink);
    }
}
