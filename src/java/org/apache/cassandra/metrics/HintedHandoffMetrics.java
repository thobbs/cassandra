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
package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.Map.Entry;

import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

/**
 * Metrics for {@link HintedHandOffManager}.
 */
public class HintedHandoffMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandoffMetrics.class);

    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "HintedHandOffManager";

    /** Total number of hints which are not stored, This is not a cache. */
    private final LoadingCache<InetAddress, DifferencingCounter> notStored = CacheBuilder.newBuilder().build(new CacheLoader<InetAddress, DifferencingCounter>()
    {
        public DifferencingCounter load(InetAddress address)
        {
            return new DifferencingCounter(address);
        }
    });

    /** Total number of hints that have been created, This is not a cache. */
    private final LoadingCache<InetAddress, Counter> createdHintCounts = CacheBuilder.newBuilder().build(new CacheLoader<InetAddress, Counter>()
    {
        public Counter load(InetAddress address)
        {
            return Metrics.newCounter(new MetricName(GROUP_NAME, TYPE_NAME, "Hints_created-" + address.getHostAddress()));
        }
    });

    public void incrCreatedHints(InetAddress address)
    {
        createdHintCounts.getUnchecked(address).inc();
    }

    public void incrPastWindow(InetAddress address)
    {
        notStored.getUnchecked(address).mark();
    }

    public void log()
    {
        for (Entry<InetAddress, DifferencingCounter> entry : notStored.asMap().entrySet())
        {
            long diffrence = entry.getValue().diffrence();
            if (diffrence == 0)
                continue;
            logger.warn("{} has {} dropped hints, because node is down past configured hint window.", entry.getKey(), diffrence);
            SystemKeyspace.updateHintsDropped(entry.getKey(), UUIDGen.getTimeUUID(), (int) diffrence);
        }
    }

    public class DifferencingCounter
    {
        private final Counter meter;
        private long reported = 0;

        public DifferencingCounter(InetAddress address)
        {
            this.meter = Metrics.newCounter(new MetricName(GROUP_NAME, TYPE_NAME, "Hints_not_stored-" + address.getHostAddress()));
        }

        public long diffrence()
        {
            long current = meter.count();
            long diffrence = current - reported;
            this.reported = current;
            return diffrence;
        }

        public long count()
        {
            return meter.count();
        }

        public void mark()
        {
            meter.inc();
        }
    }
}
