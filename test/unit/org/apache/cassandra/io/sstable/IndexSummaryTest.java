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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.downsample;
import static org.apache.cassandra.io.sstable.IndexSummaryBuilder.entriesAtSamplingLevel;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.redistributeSummaries;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.DOWNSAMPLE_THESHOLD;
import static org.apache.cassandra.io.sstable.IndexSummaryManager.UPSAMPLE_THRESHOLD;
import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.Downsampling.MIN_SAMPLING_LEVEL;

import static org.junit.Assert.*;

public class IndexSummaryTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryTest.class);

    @Test
    public void testGetKey()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(random.left.get(i).key, ByteBuffer.wrap(random.right.getKey(i)));
    }

    @Test
    public void testBinarySearch()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(i, random.right.binarySearch(random.left.get(i)));
    }

    @Test
    public void testGetPosition()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 2);
        for (int i = 0; i < 50; i++)
            assertEquals(i*2, random.right.getPosition(i));
    }

    @Test
    public void testSerialization() throws IOException
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(random.right, dos, false);
        // write junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        FileUtils.closeQuietly(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary is = IndexSummary.serializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), false);
        for (int i = 0; i < 100; i++)
            assertEquals(i, is.binarySearch(random.left.get(i)));
        // read the junk
        assertEquals(dis.readUTF(), "JUNK");
        assertEquals(dis.readUTF(), "JUNK");
        FileUtils.closeQuietly(dis);
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(1, 1, BASE_SAMPLING_LEVEL);
        builder.maybeAddEntry(p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), 0);
        IndexSummary summary = builder.build(p);
        assertEquals(1, summary.size());
        assertEquals(0, summary.getPosition(0));
        assertArrayEquals(new byte[0], summary.getKey(0));

        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(summary, dos, false);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary loaded = IndexSummary.serializer.deserialize(dis, p, false);

        assertEquals(1, loaded.size());
        assertEquals(summary.getPosition(0), loaded.getPosition(0));
        assertArrayEquals(summary.getKey(0), summary.getKey(0));
    }

    private Pair<List<DecoratedKey>, IndexSummary> generateRandomIndex(int size, int interval)
    {
        List<DecoratedKey> list = Lists.newArrayList();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(list.size(), interval, BASE_SAMPLING_LEVEL);
        for (int i = 0; i < size; i++)
        {
            UUID uuid = UUID.randomUUID();
            DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(uuid));
            list.add(key);
        }
        Collections.sort(list);
        for (int i = 0; i < size; i++)
            builder.maybeAddEntry(list.get(i), i);
        IndexSummary summary = builder.build(DatabaseDescriptor.getPartitioner());
        return Pair.create(list, summary);
    }

    @Test
    public void testDownsamplePatterns()
    {
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(0));
        assertEquals(Arrays.asList(0), Downsampling.getSamplingPattern(1));

        assertEquals(Arrays.asList(0, 1), Downsampling.getSamplingPattern(2));
        assertEquals(Arrays.asList(0, 2, 1, 3), Downsampling.getSamplingPattern(4));
        assertEquals(Arrays.asList(0, 4, 2, 6, 1, 5, 3, 7), Downsampling.getSamplingPattern(8));
        assertEquals(Arrays.asList(0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15), Downsampling.getSamplingPattern(16));
    }

    private static boolean shouldSkip(int index, List<Integer> startPoints)
    {
        for (int start : startPoints)
        {
            if ((index - start) % BASE_SAMPLING_LEVEL == 0)
                return true;
        }
        return false;
    }

    @Test
    public void testDownsample()
    {
        final int NUM_KEYS = 4096;
        final int INDEX_INTERVAL = 128;
        final int ORIGINAL_NUM_ENTRIES = NUM_KEYS / INDEX_INTERVAL;


        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(NUM_KEYS, INDEX_INTERVAL);
        List<DecoratedKey> keys = random.left;
        IndexSummary original = random.right;

        // sanity check on the original index summary
        for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(original.getKey(i)));

        List<Integer> samplePattern = Downsampling.getSamplingPattern(BASE_SAMPLING_LEVEL);

        // downsample by one level, then two levels, then three levels...
        int downsamplingRound = 1;
        for (int samplingLevel = BASE_SAMPLING_LEVEL - 1; samplingLevel >= MIN_SAMPLING_LEVEL; samplingLevel--)
        {
            IndexSummary downsampled = downsample(original, samplingLevel, DatabaseDescriptor.getPartitioner());
            assertEquals(entriesAtSamplingLevel(samplingLevel, original.getMaxNumberOfEntries()), downsampled.size());

            int sampledCount = 0;
            List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
            for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            {
                if (!shouldSkip(i, skipStartPoints))
                {
                    assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                    sampledCount++;
                }
            }
            downsamplingRound++;
        }

        // downsample one level each time
        IndexSummary previous = original;
        downsamplingRound = 1;
        for (int downsampleLevel = BASE_SAMPLING_LEVEL - 1; downsampleLevel >= MIN_SAMPLING_LEVEL; downsampleLevel--)
        {
            IndexSummary downsampled = downsample(previous, downsampleLevel, DatabaseDescriptor.getPartitioner());
            assertEquals(entriesAtSamplingLevel(downsampleLevel, original.getMaxNumberOfEntries()), downsampled.size());

            int sampledCount = 0;
            List<Integer> skipStartPoints = samplePattern.subList(0, downsamplingRound);
            for (int i = 0; i < ORIGINAL_NUM_ENTRIES; i++)
            {
                if (!shouldSkip(i, skipStartPoints))
                {
                    assertEquals(keys.get(i * INDEX_INTERVAL).key, ByteBuffer.wrap(downsampled.getKey(sampledCount)));
                    sampledCount++;
                }
            }

            previous = downsampled;
            downsamplingRound++;
        }
    }

    @Test
    public void testRebuildAtSamplingLevel() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numRows = 256;
        for (int row = 0; row < numRows; row++)
        {
            DecoratedKey key = Util.dk(String.valueOf(row));
            RowMutation rm = new RowMutation(ksname, key.key);
            rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableReader sstable = sstables.get(0);

        for (int samplingLevel = MIN_SAMPLING_LEVEL; samplingLevel < BASE_SAMPLING_LEVEL; samplingLevel++)
        {
            sstable = sstable.cloneWithNewSummarySamplingLevel(samplingLevel);
            assertEquals(samplingLevel, sstable.getIndexSummarySamplingLevel());
            int expectedSize = (numRows * samplingLevel) / (sstable.metadata.getIndexInterval() * BASE_SAMPLING_LEVEL);
            assertEquals(expectedSize, sstable.getIndexSummarySize(), 1);
        }
    }

    private static long totalOffHeapSize(List<SSTableReader> sstables)
    {
        long total = 0;
        for (SSTableReader sstable : sstables)
            total += sstable.getIndexSummaryOffHeapSize();

        return total;
    }

    private static List<SSTableReader> resetSummaries(List<SSTableReader> sstables, long originalOffHeapSize) throws IOException
    {
        for (SSTableReader sstable : sstables)
            sstable.readMeter = new RestorableMeter(100.0, 100.0);

        sstables = redistributeSummaries(sstables, originalOffHeapSize * sstables.size());
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());

        return sstables;
    }

    private void validateData(ColumnFamilyStore cfs, int numRows)
    {
        for (int i = 0; i < numRows; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            QueryFilter filter = QueryFilter.getIdentityFilter(key, cfs.getColumnFamilyName(), System.currentTimeMillis());
            ColumnFamily row = cfs.getColumnFamily(filter);
            assertNotNull(row);
            Column column = row.getColumn(ByteBufferUtil.bytes("column"));
            assertNotNull(column);
            assertEquals(100, column.value().array().length);
        }
    }

    private Comparator<SSTableReader> hotnessComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Double.compare(o1.readMeter.fifteenMinuteRate(), o2.readMeter.fifteenMinuteRate());
        }
    };

    @Test
    public void testRedistributeSummaries() throws IOException
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLowIndexInterval";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        int numSSTables = 4;
        int numRows = 256;
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                DecoratedKey key = Util.dk(String.valueOf(row));
                RowMutation rm = new RowMutation(ksname, key.key);
                rm.add(cfname, ByteBufferUtil.bytes("column"), value, 0);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        assertEquals(numSSTables, sstables.size());
        validateData(cfs, numRows);

        for (SSTableReader sstable : sstables)
            sstable.readMeter = new RestorableMeter(100.0, 100.0);

        long offHeapSize = sstables.get(0).getIndexSummaryOffHeapSize();

        // there should be enough space to not downsample anything
        sstables = redistributeSummaries(sstables, offHeapSize * numSSTables);
        for (SSTableReader sstable : sstables)
            assertEquals(sstable.getIndexSummarySamplingLevel(), BASE_SAMPLING_LEVEL);
        assertEquals(offHeapSize * numSSTables, totalOffHeapSize(sstables));
        validateData(cfs, numRows);

        // everything should get cut in half
        assert sstables.size() == 4;
        sstables = redistributeSummaries(sstables, (offHeapSize * numSSTables) / 2);
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // everything should get cut to a quarter
        sstables = redistributeSummaries(sstables, (offHeapSize * numSSTables) / 4);
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 4, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to half
        sstables = redistributeSummaries(sstables, (offHeapSize * numSSTables) / 2);
        assert sstables.size() == 4;
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL / 2, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // upsample back up to the original index summary
        sstables = redistributeSummaries(sstables, offHeapSize * numSSTables);
        for (SSTableReader sstable : sstables)
            assertEquals(BASE_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // make two of the four sstables cold, only leave enough space for three full index summaries,
        // so the two cold sstables should get downsampled to be half of their original size
        sstables.get(0).readMeter = new RestorableMeter(50.0, 50.0);
        sstables.get(1).readMeter = new RestorableMeter(50.0, 50.0);
        sstables = redistributeSummaries(sstables, offHeapSize * (numSSTables - 1));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // small increases or decreases in the read rate don't result in downsampling or upsampling
        double lowerRate = 50.0 * (DOWNSAMPLE_THESHOLD + (DOWNSAMPLE_THESHOLD * 0.10));
        double higherRate = 50.0 * (UPSAMPLE_THRESHOLD - (UPSAMPLE_THRESHOLD * 0.10));
        sstables.get(0).readMeter = new RestorableMeter(lowerRate, lowerRate);
        sstables.get(1).readMeter = new RestorableMeter(higherRate, higherRate);
        sstables = redistributeSummaries(sstables, offHeapSize * (numSSTables - 1));
        Collections.sort(sstables, hotnessComparator);
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL / 2, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // reset, and then this time, leave enough space for one of the cold sstables to not get downsampled
        // as far as the other
        sstables = resetSummaries(sstables, offHeapSize);
        sstables.get(0).readMeter = new RestorableMeter(50.0, 50.0);
        sstables.get(1).readMeter = new RestorableMeter(50.0, 50.0);

        int currentEntries = entriesAtSamplingLevel(BASE_SAMPLING_LEVEL / 2, sstables.get(0).getMaxIndexSummarySize());
        int expectedLevel = BASE_SAMPLING_LEVEL / 2 + 1;
        while (entriesAtSamplingLevel(expectedLevel, sstables.get(0).getMaxIndexSummarySize()) <= currentEntries)
            expectedLevel += 1;

        double avgSize = sstables.get(0).getIndexSummaryOffHeapSize() / (double) sstables.get(0).getIndexSummarySize();

        sstables = redistributeSummaries(sstables, offHeapSize * (numSSTables - 1) + (long) Math.ceil(avgSize));
        Collections.sort(sstables, hotnessComparator);

        int indexOfHigherLevel = sstables.get(0).getIndexSummarySamplingLevel() == expectedLevel ? 0 : 1;
        int higherLevel = sstables.get(indexOfHigherLevel).getIndexSummarySamplingLevel();
        int lowerLevel = sstables.get(indexOfHigherLevel == 0 ? 1 : 0).getIndexSummarySamplingLevel();

        assertEquals(expectedLevel, higherLevel);
        assertTrue(lowerLevel < higherLevel && lowerLevel >= BASE_SAMPLING_LEVEL / 2);
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);


        // Cause a mix of upsampling and downsampling. We'll leave enough space for two full index summaries. The two
        // coldest sstables will get downsampled to 8/128 of their size, leaving us with 1 and 112/128th index
        // summaries worth of space.  The hottest sstable should get a full index summary, and the one in the middle
        // should get the remaining 112/128th.
        int leftovers = BASE_SAMPLING_LEVEL - (MIN_SAMPLING_LEVEL * 2);
        sstables.get(0).readMeter = new RestorableMeter(0.0, 0.0);
        sstables.get(1).readMeter = new RestorableMeter(0.0, 0.0);
        sstables.get(2).readMeter = new RestorableMeter(leftovers, leftovers);
        sstables.get(3).readMeter = new RestorableMeter(128.0, 128.0);
        sstables = redistributeSummaries(sstables, offHeapSize * 2);
        Collections.sort(sstables, hotnessComparator);
        assertEquals(MIN_SAMPLING_LEVEL, sstables.get(0).getIndexSummarySamplingLevel());
        assertEquals(MIN_SAMPLING_LEVEL, sstables.get(1).getIndexSummarySamplingLevel());
        assertEquals(leftovers, sstables.get(2).getIndexSummarySamplingLevel());
        assertEquals(BASE_SAMPLING_LEVEL, sstables.get(3).getIndexSummarySamplingLevel());
        validateData(cfs, numRows);

        // Don't leave enough space for even the minimal index summaries
        sstables = redistributeSummaries(sstables, (long) ((offHeapSize * numSSTables) * (MIN_SAMPLING_LEVEL / (double) BASE_SAMPLING_LEVEL) - 1));
        for (SSTableReader sstable : sstables)
            assertEquals(MIN_SAMPLING_LEVEL, sstable.getIndexSummarySamplingLevel());
        validateData(cfs, numRows);
    }

    @Test
    public void testOriginalIndexLookup()
    {
        for (int i = BASE_SAMPLING_LEVEL; i >= MIN_SAMPLING_LEVEL; i--)
            assertEquals(i, Downsampling.getOriginalIndexes(i).size());

        ArrayList<Integer> full = new ArrayList<>();
        for (int i = 0; i < BASE_SAMPLING_LEVEL; i++)
            full.add(i);

        assertEquals(full, Downsampling.getOriginalIndexes(BASE_SAMPLING_LEVEL));
        // the entry at index 0 is the first to go
        assertEquals(full.subList(1, full.size()), Downsampling.getOriginalIndexes(BASE_SAMPLING_LEVEL - 1));

        // spot check a few values (these depend on BASE_SAMPLING_LEVEL being 128)
        assert BASE_SAMPLING_LEVEL == 128;
        assertEquals(Arrays.asList(31, 63, 95, 127), Downsampling.getOriginalIndexes(4));
        assertEquals(Arrays.asList(63, 127), Downsampling.getOriginalIndexes(2));
        assertEquals(Arrays.asList(), Downsampling.getOriginalIndexes(0));
    }

    @Test
    public void testGetNumberOfSkippedEntriesAfterIndex()
    {
        int indexInterval = 128;
        for (int i = 0; i < BASE_SAMPLING_LEVEL; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, BASE_SAMPLING_LEVEL, indexInterval));

        // with one round of downsampling, only the first summary has been removed, so only the last index will have
        // double the gap until the next sample
        for (int i = 0; i < BASE_SAMPLING_LEVEL - 2; i++)
            assertEquals(indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(i, BASE_SAMPLING_LEVEL - 1, indexInterval));
        assertEquals(indexInterval * 2, Downsampling.getEffectiveIndexIntervalAfterIndex(BASE_SAMPLING_LEVEL - 2, BASE_SAMPLING_LEVEL - 1, indexInterval));

        // at samplingLevel=2, the retained summary points are [63, 127] (assumes BASE_SAMPLING_LEVEL is 128)
        assert BASE_SAMPLING_LEVEL == 128;
        assertEquals(64 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(0, 2, indexInterval));
        assertEquals(64 * indexInterval, Downsampling.getEffectiveIndexIntervalAfterIndex(1, 2, indexInterval));
    }
}