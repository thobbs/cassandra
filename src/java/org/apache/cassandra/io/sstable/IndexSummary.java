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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;

/*
 * Layout of Memory for index summaries:
 *
 * There are two sections:
 *  * A lookup for the position of entries in the summary itself.  This will contain one four byte position
 *    for each entry in the summary. This allows us do simple math in getIndex() to find the position in the Memory
 *    to start reading the actual index summary entry.  (This is necessary because keys can have different lengths.)
 *  * A sequence of (DecoratedKey, position) pairs, where position is the offset into the actual index file.
 */
public class IndexSummary implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummary.class);

    // The base downsampling level determines the granularity at which we can down/upsample.  A higher number would
    // mean fewer items would be removed in each downsampling round.  This must be a power of two in order to have
    // good sampling patterns. This cannot be changed without rebuilding all index summaries at full sampling.
    public static final int BASE_SAMPLING_LEVEL = 16;

    // The lowest level we will downsample to.  (Arbitrary value, can be anywhere from 1 to the base value.)
    public static final int MIN_SAMPLING_LEVEL = 4;

    // A cache of starting points for downsampling rounds. The first index is the technically for the base sampling
    // level, but since the function recursively calls itself with lower values, it's effectively just the argument
    // to getSamplingPattern().
    private static final List<List<Integer>> samplePatternCache = new ArrayList<>(BASE_SAMPLING_LEVEL);

    // A cache of arrays to translate current summary indices to their original (non-downsampled) indices.  The first
    // index is the current sampling level.
    private static final List<List<Integer>> originalIndexLookup = new ArrayList<>(BASE_SAMPLING_LEVEL);

    static {
        // initialize the caches to avoid index errors
        for (int i = 0; i <= BASE_SAMPLING_LEVEL; i++)
        {
            samplePatternCache.add(null);
            originalIndexLookup.add(null);
        }
    }

    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();
    private final int indexInterval;
    private final IPartitioner partitioner;
    private final int summarySize;
    private final Memory bytes;

    /*
     * A value between MIN_SAMPLING_LEVEL and BASE_SAMPLING_LEVEL that represents how many of the original
     * index summary entries ((1 / indexInterval) * numKeys) have been retained.
     * This summary contains (samplingLevel / BASE_SAMPLING_LEVEL) * ((1 / indexInterval) * numKeys)) entries.
     * In other words, a lower samplingLevel means fewer entries have been retained.
     */
    private final int samplingLevel;

    public IndexSummary(IPartitioner partitioner, Memory memory, int summarySize, int indexInterval, int samplingLevel)
    {
        this.partitioner = partitioner;
        this.indexInterval = indexInterval;
        this.summarySize = summarySize;
        this.bytes = memory;
        this.samplingLevel = samplingLevel;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = summarySize, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            result = -DecoratedKey.compareTo(partitioner, ByteBuffer.wrap(getKey(mid)), key);
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return -mid - (result < 0 ? 1 : 2);
    }

    /**
     * Gets the position of the actual index summary entry in our Memory attribute, 'bytes'.
     * @param index The index of the entry or key to get the position for
     * @return an offset into our Memory attribute where the actual entry resides
     */
    public int getPositionInSummary(int index)
    {
        // The first section of bytes holds a four-byte position for each entry in the summary, so just multiply by 4.
        return bytes.getInt(index << 2);
    }

    public byte[] getKey(int index)
    {
        long start = getPositionInSummary(index);
        int keySize = (int) (calculateEnd(index) - start - 8L);
        byte[] key = new byte[keySize];
        bytes.getBytes(start, key, 0, keySize);
        return key;
    }

    public long getPosition(int index)
    {
        return bytes.getLong(calculateEnd(index) - 8);
    }

    public byte[] getEntry(int index)
    {
        long start = getPositionInSummary(index);
        long end = calculateEnd(index);
        byte[] entry = new byte[(int)(end - start)];
        bytes.getBytes(start, entry, 0, (int)(end - start));
        return entry;
    }

    private long calculateEnd(int index)
    {
        return index == (summarySize - 1) ? bytes.size() : getPositionInSummary(index + 1);
    }

    public int getIndexInterval()
    {
        return indexInterval;
    }

    public int size()
    {
        return summarySize;
    }

    public int getSamplingLevel()
    {
        return samplingLevel;
    }

    /**
     * Returns the number of entries this summary would have if it were at the full sampling level.
     * @return the most entries this summary could have
     */
    public int getMaxNumberOfEntries()
    {
        return (BASE_SAMPLING_LEVEL * summarySize) / samplingLevel;
    }

    /**
     * Returns the amount of off-heap memory used for this summary.
     * @return size in bytes
     */
    public long getOffHeapSize()
    {
        return bytes.size();
    }

    /**
     * Gets a list of starting indices for downsampling rounds.
     * @param samplingLevel the base sampling level
     * @return A list of `samplingLevel` unique indices between 0 and `samplingLevel`
     */
    @VisibleForTesting
    static List<Integer> getSamplingPattern(int samplingLevel)
    {
        List<Integer> pattern = samplePatternCache.get(samplingLevel);
        if (pattern != null)
            return pattern;

        if (samplingLevel <= 1)
            return Arrays.asList(0);

        ArrayList<Integer> startIndices = new ArrayList<>(samplingLevel);
        startIndices.add(0);

        int spread = samplingLevel;
        while (spread >= 2)
        {
            ArrayList<Integer> roundIndices = new ArrayList<>(samplingLevel / spread);
            for (int i = spread / 2; i < samplingLevel; i += spread)
                roundIndices.add(i);

            // especially for latter rounds, it's important that we spread out the start points, so we'll
            // make a recursive call to get an ordering for this list of start points
            List<Integer> roundIndicesOrdering = getSamplingPattern(roundIndices.size());
            for (int i = 0; i < roundIndices.size(); ++i)
                startIndices.add(roundIndices.get(roundIndicesOrdering.get(i)));

            spread /= 2;
        }

        samplePatternCache.set(samplingLevel, startIndices);
        return startIndices;
    }

    /**
     * Returns a list that can be used to translate current index summary indexes to their original index before
     * downsampling.  For example, if [7, 15] is returned, the current index summary entry at index 0 was originally
     * at index 7, and the current index 1 was originally at index 15.
     * @param samplingLevel the current sampling level for the index summary
     * @return a list of original indexes for current summary entries
     */
    @VisibleForTesting
    static List<Integer> getOriginalIndexes(int samplingLevel)
    {
        List<Integer> originalIndexes = originalIndexLookup.get(samplingLevel);
        if (originalIndexes != null)
            return originalIndexes;

        List<Integer> pattern = getSamplingPattern(BASE_SAMPLING_LEVEL).subList(0, BASE_SAMPLING_LEVEL - samplingLevel);
        originalIndexes = new ArrayList<>(samplingLevel);
        for (int j = 0; j < BASE_SAMPLING_LEVEL; j++)
        {
            if (!pattern.contains(j))
                originalIndexes.add(j);
        }
        originalIndexLookup.set(samplingLevel, originalIndexes);
        return originalIndexes;
    }

    /**
     * Returns the number of primary (on-disk) index entries between the index summary entry at `index` and the next
     * index summary entry (assuming there is one).  Without any downsampling, this will always be equivalent to
     * the index interval.
     * @param index the index of an index summary entry (between zero and the index entry size)
     * @return the number of partitions after `index` until the next partition with a summary entry
     */
    public int getNumberOfSkippedEntriesAfterIndex(int index)
    {
        return getNumberOfSkippedEntriesAfterIndex(index, samplingLevel, indexInterval);
    }

    @VisibleForTesting
    static int getNumberOfSkippedEntriesAfterIndex(int index, int samplingLevel, int indexInterval)
    {
        assert index >= -1;
        List<Integer> originalIndexes = getOriginalIndexes(samplingLevel);
        if (index == -1)
            return originalIndexes.get(0) * indexInterval;

        index %= samplingLevel;
        if (index == originalIndexes.size() - 1)
            return ((BASE_SAMPLING_LEVEL - originalIndexes.get(index)) + originalIndexes.get(0)) * indexInterval;
        else
            return (originalIndexes.get(index + 1) - originalIndexes.get(index)) * indexInterval;
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputStream out, boolean withSamplingLevel) throws IOException
        {
            out.writeInt(t.indexInterval);
            out.writeInt(t.summarySize);
            out.writeLong(t.bytes.size());
            if (withSamplingLevel)
                out.writeInt(t.samplingLevel);
            FBUtilities.copy(new MemoryInputStream(t.bytes), out, t.bytes.size());
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner, boolean haveSamplingLevel) throws IOException
        {
            int indexInterval = in.readInt();
            int summarySize = in.readInt();
            long offheapSize = in.readLong();
            int downsampleLevel = haveSamplingLevel ? in.readInt() : BASE_SAMPLING_LEVEL;
            Memory memory = Memory.allocate(offheapSize);
            FBUtilities.copy(in, new MemoryOutputStream(memory), offheapSize);
            return new IndexSummary(partitioner, memory, summarySize, indexInterval, downsampleLevel);
        }
    }

    @Override
    public void close() throws IOException
    {
        bytes.free();
    }
}