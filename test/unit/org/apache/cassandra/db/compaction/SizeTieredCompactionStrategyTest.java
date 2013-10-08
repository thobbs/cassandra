/**
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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class SizeTieredCompactionStrategyTest
{

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "0.35");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, "1.5");
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, "10000");
        Map<String, String> unvalidated = SizeTieredCompactionStrategy.validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "-0.5");
            SizeTieredCompactionStrategy.validateOptions(options);
            fail("Negative coldness_threshold should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.COLDNESS_THRESHOLD_KEY, "0.25");
        }

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "1000.0");
            SizeTieredCompactionStrategy.validateOptions(options);
            fail("bucket_low greater than bucket_high should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        }

        options.put("bad_option", "1.0");
        unvalidated = SizeTieredCompactionStrategy.validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = new ArrayList<Pair<String, Long>>();
        String[] strings = { "a", "bbbb", "cccccccc", "cccccccc", "bbbb", "a" };
        for (String st : strings)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        List<List<String>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0).length(), bucket.get(1).length());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
        }

        pairs.clear();
        buckets.clear();

        String[] strings2 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings2)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(3, bucket.size());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
            assertEquals(bucket.get(1).charAt(0), bucket.get(2).charAt(0));
        }

        // Test the "min" functionality
        pairs.clear();
        buckets.clear();

        String[] strings3 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings3)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = SizeTieredCompactionStrategy.getBuckets(pairs, 1.5, 0.5, 10);
        assertEquals(1, buckets.size());
    }
}
