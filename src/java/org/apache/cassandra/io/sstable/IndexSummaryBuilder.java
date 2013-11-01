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
package org.apache.cassandra.io.sstable;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSummaryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryBuilder.class);

    private final ArrayList<Long> positions;
    private final ArrayList<byte[]> keys;
    private final int indexInterval;
    private final int samplingLevel;
    private final int[] startPoints;
    private long keysWritten = 0;
    private long indexIntervalMatches = 0;
    private long offheapSize = 0;

    private static final Map<Integer, List<Integer>> samplePatternCache = new HashMap<>();

    public IndexSummaryBuilder(long expectedKeys, int indexInterval, int samplingLevel)
    {
        this.indexInterval = indexInterval;
        this.samplingLevel = samplingLevel;
        this.startPoints = getStartPoints(IndexSummary.BASE_SAMPLING_LEVEL, samplingLevel);

        long expectedEntries = expectedKeys / indexInterval;
        if (expectedEntries > Integer.MAX_VALUE)
        {
            // that's a _lot_ of keys, and a very low interval
            int effectiveInterval = (int) Math.ceil((double) Integer.MAX_VALUE / expectedKeys);
            expectedEntries = expectedKeys / effectiveInterval;
            assert expectedEntries <= Integer.MAX_VALUE : expectedEntries;
            logger.warn("Index interval of {} is too low for {} expected keys; using interval of {} instead",
                        indexInterval, expectedKeys, effectiveInterval);
        }

        // adjust our estimates based on the sampling level
        expectedEntries *= (long) Math.ceil(samplingLevel / ((double) IndexSummary.BASE_SAMPLING_LEVEL));

        positions = new ArrayList<>((int)expectedEntries);
        keys = new ArrayList<>((int)expectedEntries);
    }

    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexPosition)
    {
        if (keysWritten % indexInterval == 0)
        {
            indexIntervalMatches++;

            // see if we should skip this key based on our sampling level
            boolean shouldSkip = false;
            for (int start : startPoints)
            {
                if ((indexIntervalMatches - start) % IndexSummary.BASE_SAMPLING_LEVEL == 0)
                {
                    shouldSkip = true;
                    break;
                }
            }

            if (!shouldSkip)
            {
                byte[] key = ByteBufferUtil.getArray(decoratedKey.key);
                keys.add(key);
                offheapSize += key.length;
                positions.add(indexPosition);
                offheapSize += TypeSizes.NATIVE.sizeof(indexPosition);
            }
        }
        keysWritten++;

        return this;
    }

    public IndexSummary build(IPartitioner partitioner)
    {
        assert keys.size() > 0;
        assert keys.size() == positions.size();

        // first we write out the position in the *summary* for each key in the summary,
        // then we write out (key, actual index position) pairs
        Memory memory = Memory.allocate(offheapSize + (keys.size() * 4));
        int idxPosition = 0;
        int keyPosition = keys.size() * 4;
        for (int i = 0; i < keys.size(); i++)
        {
            // write the position of the actual entry in the index summary (4 bytes)
            memory.setInt(idxPosition, keyPosition);
            idxPosition += TypeSizes.NATIVE.sizeof(keyPosition);

            // write the key
            byte[] keyBytes = keys.get(i);
            memory.setBytes(keyPosition, keyBytes, 0, keyBytes.length);
            keyPosition += keyBytes.length;

            // write the position in the actual index file
            long actualIndexPosition = positions.get(i);
            memory.setLong(keyPosition, actualIndexPosition);
            keyPosition += TypeSizes.NATIVE.sizeof(actualIndexPosition);
        }
        return new IndexSummary(partitioner, memory, keys.size(), indexInterval, samplingLevel);
    }

    @VisibleForTesting
    static List<Integer> getSamplingPattern(int baseSamplingLevel)
    {
        List<Integer> pattern = samplePatternCache.get(baseSamplingLevel);
        if (pattern != null)
            return pattern;

        if (baseSamplingLevel <= 1)
            return Arrays.asList(0);

        ArrayList<Integer> startIndices = new ArrayList<>(baseSamplingLevel);
        startIndices.add(0);

        int spread = baseSamplingLevel;
        while (spread >= 2)
        {
            ArrayList<Integer> roundIndices = new ArrayList<>(baseSamplingLevel / spread);
            for (int i = spread / 2; i < baseSamplingLevel; i += spread)
                roundIndices.add(i);

            // especially for latter rounds, it's important that we spread out the start points, so we'll
            // make a recursive call to get an ordering for this list of start points
            List<Integer> roundIndicesOrdering = getSamplingPattern(roundIndices.size());
            for (int i = 0; i < roundIndices.size(); ++i)
                startIndices.add(roundIndices.get(roundIndicesOrdering.get(i)));

            spread /= 2;
        }

        return startIndices;
    }

    public static int entriesAtSamplingLevel(IndexSummary summary, int samplingLevel)
    {
        return (samplingLevel * summary.getOriginalNumEntries()) / IndexSummary.BASE_SAMPLING_LEVEL;
    }

    public static int calculateSamplingLevel(IndexSummary existing, long targetNumEntries)
    {
        // Algebraic explanation for calculating the new sampling level (solve for newSamplingLevel):
        // originalNumEntries = (baseSamplingLevel / currentSamplingLevel) * currentNumEntries
        // targetNumEntries = (newSamplingLevel / baseSamplingLevel) * originalNumEntries
        // targetNumEntries = (newSamplingLevel / baseSamplingLevel) * (baseSamplingLevel / currentSamplingLevel) * currentNumEntries
        // targetNumEntries = (newSamplingLevel / currentSamplingLevel) * currentNumEntries
        // (targetNumEntries * currentSamplingLevel) / currentNumEntries = newSamplingLevel
        int newSamplingLevel = (int) (targetNumEntries * existing.getSamplingLevel()) / existing.size();
        return Math.min(IndexSummary.BASE_SAMPLING_LEVEL, Math.max(IndexSummary.MIN_SAMPLING_LEVEL, newSamplingLevel));
    }

    private static int[] getStartPoints(int currentSamplingLevel, int newSamplingLevel)
    {
        List<Integer> allStartPoints = getSamplingPattern(IndexSummary.BASE_SAMPLING_LEVEL);

        // calculate starting indexes for sampling rounds
        int initialRound = IndexSummary.BASE_SAMPLING_LEVEL - currentSamplingLevel;
        int numRounds = Math.abs(currentSamplingLevel - newSamplingLevel);
        int[] startPoints = new int[numRounds];
        for (int i = 0; i < numRounds; ++i)
        {
            int start = allStartPoints.get(initialRound + i);

            // our "ideal" start points will be affected by the removal of items in earlier rounds, so go through all
            // earlier rounds, and if we see an index that comes before our ideal start point, decrement the start point
            int adjustment = 0;
            for (int j = 0; j < initialRound; ++j)
            {
                if (allStartPoints.get(j) < start)
                    adjustment++;
            }
            startPoints[i] = start - adjustment;
        }
        return startPoints;
    }

    public static IndexSummary downsample(IndexSummary existing, int newSamplingLevel, IPartitioner partitioner)
    {
        // To downsample the old index summary, we'll go through (potentially) several rounds of downsampling.
        // Conceptually, each round starts at position X and then removes every Nth item.  The value of X follows
        // a particular pattern to evenly space out the items that we remove.  The value of N decreases by one each
        // round.

        int currentSamplingLevel = existing.getSamplingLevel();
        if (currentSamplingLevel <= newSamplingLevel)
            return existing;

        // calculate starting indexes for downsampling rounds
        int[] startPoints = getStartPoints(currentSamplingLevel, newSamplingLevel);

        // calculate new off-heap size
        int removedKeyCount = 0;
        long newOffHeapSize = existing.getOffHeapSize();
        for (int start : startPoints)
        {
            for (int j = start; j < existing.size(); j += currentSamplingLevel)
            {
                removedKeyCount++;
                newOffHeapSize -= existing.getEntry(j).length;
            }
        }

        int newKeyCount = existing.size() - removedKeyCount;

        // Subtract (removedKeyCount * 4) from the new size to account for fewer entries in the first section, which
        // stores the position of the actual entries in the summary.
        Memory memory = Memory.allocate(newOffHeapSize - (removedKeyCount * 4));

        // Copy old entries to our new Memory.
        int idxPosition = 0;
        int keyPosition = newKeyCount * 4;
        for (int oldSummaryIndex = 0; oldSummaryIndex < existing.size(); oldSummaryIndex++)
        {
            // to determine if we can skip this entry, go through the starting points for our downsampling rounds
            // and see if the entry's index is covered by that round
            boolean skip = false;
            for (int start : startPoints)
            {
                if ((oldSummaryIndex - start) % currentSamplingLevel == 0)
                {
                    skip = true;
                    break;
                }
            }

            if (!skip)
            {
                // write the position of the actual entry in the index summary (4 bytes)
                memory.setInt(idxPosition, keyPosition);
                idxPosition += TypeSizes.NATIVE.sizeof(keyPosition);

                // write the entry itself
                byte[] entry = existing.getEntry(oldSummaryIndex);
                memory.setBytes(keyPosition, entry, 0, entry.length);
                keyPosition += entry.length;
            }
        }
        return new IndexSummary(partitioner, memory, newKeyCount, existing.getIndexInterval(), newSamplingLevel);
    }
}
