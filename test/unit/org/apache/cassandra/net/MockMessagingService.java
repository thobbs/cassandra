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
import java.net.UnknownHostException;
import java.util.function.Predicate;

/**
 * Starting point for mocking {@link MessagingService} interactions. Outgoing messages can be
 * intercepted by first creating a {@link MatcherResponse} by calling {@link MockMessagingService#when(Matcher)}.
 * {@link Matcher}s can be created using predefined predicates provided by this class and may also be
 * nested using {@link MockMessagingService#all(Matcher[])} or {@link MockMessagingService#any(Matcher[])}.
 * After each test, {@link MockMessagingService#cleanup()} must be called for free listeners registered
 * in {@link MessagingService}.
 */
public class MockMessagingService
{

    private MockMessagingService()
    {
    }

    public static MatcherResponse when(Matcher matcher)
    {
        return new MatcherResponse(matcher);
    }

    public static void cleanup()
    {
        MessagingService.instance().clearMessageSinks();
    }

    public static Matcher<InetAddress> from(String saddr)
    {
        try
        {
            return from(InetAddress.getByName(saddr));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Matcher<InetAddress> from(InetAddress addr)
    {
        return (in, to) -> in.from == addr || in.from.equals(addr);
    }

    public static Matcher<InetAddress> to(String saddr)
    {
        try
        {
            return to(InetAddress.getByName(saddr));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Matcher<InetAddress> to(InetAddress addr)
    {
        return (in, to) -> to == addr || to.equals(addr);
    }

    public static Matcher<MessagingService.Verb> verb(MessagingService.Verb verb)
    {
        return (in, to) -> in.verb == verb;
    }

    public static <T> Matcher<T> message(Predicate<MessageOut<T>> fn)
    {
        return (msg, to) -> fn.test(msg);
    }

    public static <T> Matcher<T> payload(Predicate<T> fn)
    {
        return (msg, to) -> fn.test(msg.payload);
    }

    public static <T> Matcher<T> not(Matcher<T> matcher)
    {
        return (o, to) -> !matcher.matches(o, to);
    }

    public static <T> Matcher<?> all(Matcher<?>... matchers)
    {
        return (MessageOut<T> out, InetAddress to) -> {
            for (Matcher matcher : matchers)
            {
                if (!matcher.matches(out, to)) return false;
            }
            return true;
        };
    }

    public static <T> Matcher<?> any(Matcher<?>... matchers)
    {
        return (MessageOut<T> out, InetAddress to) -> {
            for (Matcher matcher : matchers)
            {
                if (matcher.matches(out, to)) return true;
            }
            return false;
        };
    }
}