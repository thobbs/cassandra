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
    // The base downsampling level determines the granularity at which we can downsample.  A higher number would
    // mean fewer items would be removed in each downsampling round.  This must be a power of two in order to have
    // good downsampling patterns.
    public static final int BASE_DOWNSAMPLE_LEVEL = 16;

    // The lowest level we will downsample to.  (Arbitrary value, can be anywhere from 1 to the base value.)
    public static final int LOWEST_DOWNSAMPLE_LEVEL = 4;

    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();
    private final int indexInterval;
    private final IPartitioner partitioner;
    private final int summary_size;
    private final Memory bytes;

    /*
     * A value between LOWEST_DOWNSAMPLE_LEVEL and BASE_DOWNSAMPLE_LEVEL that represents how many of the original
     * index summary entries ((1 / indexInterval) * numKeys) have been retained.
     * This summary contains (downsampleLevel / BASE_DOWNSAMPLE_LEVEL) * ((1 / indexInterval) * numKeys)) entries.
     * In other words, a lower downsampleLevel means fewer entries have been retained.
     */
    private final int downsampleLevel;

    public IndexSummary(IPartitioner partitioner, Memory memory, int summary_size, int indexInterval, int downsampleLevel)
    {
        this.partitioner = partitioner;
        this.indexInterval = indexInterval;
        this.summary_size = summary_size;
        this.bytes = memory;
        this.downsampleLevel = downsampleLevel;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = summary_size, high = mid - 1, result = -1;
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
        return index == (summary_size - 1) ? bytes.size() : getPositionInSummary(index + 1);
    }

    public int getIndexInterval()
    {
        return indexInterval;
    }

    public int size()
    {
        return summary_size;
    }

    public int getDownsamplingLevel()
    {
        return downsampleLevel;
    }

    public long getOffHeapSize()
    {
        return bytes.size();
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputStream out, boolean withDownsampleLevel) throws IOException
        {
            out.writeInt(t.indexInterval);
            out.writeInt(t.summary_size);
            out.writeLong(t.bytes.size());
            if (withDownsampleLevel)
                out.writeInt(t.downsampleLevel);
            FBUtilities.copy(new MemoryInputStream(t.bytes), out, t.bytes.size());
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner, boolean haveDownsampleLevel) throws IOException
        {
            int indexInterval = in.readInt();
            int summarySize = in.readInt();
            long offheapSize = in.readLong();
            int downsampleLevel = haveDownsampleLevel ? in.readInt() : BASE_DOWNSAMPLE_LEVEL;
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