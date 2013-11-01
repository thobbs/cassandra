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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

public class IndexSummarySizer
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummarySizer.class);

    private int intervalInMinutes = 0;
    private long memoryPoolSize;

    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.9;

    private DebuggableScheduledThreadPoolExecutor executor;
    private ScheduledFuture future;

    public IndexSummarySizer(int intervalInMinutes, int memoryPoolSizeInMB)
    {
        executor = new DebuggableScheduledThreadPoolExecutor(1, "IndexSummarySizer", 0);
        setMemoryPoolSizeInMB(memoryPoolSizeInMB);
        setIntervalInMinutes(intervalInMinutes);
    }

    public void setIntervalInMinutes(int newIntervalInMinutes)
    {
        int difference = newIntervalInMinutes - intervalInMinutes;
        intervalInMinutes = newIntervalInMinutes;

        long initialDelay;
        if (future != null)
        {
            long remaining = future.getDelay(TimeUnit.MINUTES);
            initialDelay = Math.max(0, remaining + difference);
            future.cancel(false);
        }
        else
        {
            initialDelay = intervalInMinutes;
        }

        if (intervalInMinutes < 0)
        {
            future = null;
            return;
        }

        future = executor.scheduleWithFixedDelay(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                redistributeSummaries();
            }
        }, initialDelay, newIntervalInMinutes, TimeUnit.MINUTES);
    }

    public void setMemoryPoolSizeInMB(int newMemorySizeInMB)
    {
        this.memoryPoolSize = newMemorySizeInMB * 1024L * 1024L;
    }

    public void redistributeSummaries()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
            {
                sstables.addAll(cfStore.getDataTracker().getUncompactingSSTables());
            }
        }
        redistributeSummaries(sstables, this.memoryPoolSize);
    }

    /**
     * Attempts to fairly distribute a fixed pool of memory for index summaries across a set of SSTables based on
     * their recent read rates.
     * @param sstables a list of sstables to share the memory pool across
     * @param memoryPoolSize a size (in bytes) that the total index summary space usage should stay close to or under,
     *                       if possible
     */
    public static void redistributeSummaries(List<SSTableReader> sstables, long memoryPoolSize)
    {
        logger.debug("Beginning redistribution of index summaries");

        double totalReadsPerSec = 0.0;
        for (SSTableReader sstr : sstables)
        {
            if (sstr.readMeter != null)
            {
                totalReadsPerSec += sstr.readMeter.fifteenMinuteRate();
            }
        }

        // copy and sort by read rates (ascending)
        sstables = new ArrayList<>(sstables);
        Collections.sort(sstables, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                if (o1.readMeter == null && o2.readMeter == null)
                    return 0;
                else if (o1.readMeter == null)
                    return -1;
                else if (o2.readMeter == null)
                    return 1;
                else
                    return Double.compare(o1.readMeter.fifteenMinuteRate(), o2.readMeter.fifteenMinuteRate());
            }
        });

        // list of (SSTR, (targetNumEntries, newSamplingLevel)) pairs
        List<Pair<SSTableReader, Pair<Long, Integer>>> toDownsample = new ArrayList<>(sstables.size() / 4);

        // list of (SSTR, newSamplingLevel) pairs
        List<Pair<SSTableReader, Integer>> toUpsample = new ArrayList<>(sstables.size() / 4);

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        long remainingSpace = memoryPoolSize;
        for (SSTableReader sstr : sstables)
        {
            IndexSummary summary = sstr.getIndexSummary();

            double readsPerSec = sstr.readMeter == null ? 0.0 : sstr.readMeter.fifteenMinuteRate();
            long idealSpace = Math.round(remainingSpace * (readsPerSec / totalReadsPerSec));

            // figure out how many entries our idealSpace would buy us, and pick a new sampling level based on that
            double avgEntrySize = summary.getOffHeapSize() / (double) summary.size();
            long targetNumEntries = Math.round(idealSpace / avgEntrySize);
            int newSamplingLevel = IndexSummaryBuilder.calculateSamplingLevel(summary, targetNumEntries);

            if (targetNumEntries >= summary.size() * UPSAMPLE_THRESHOLD)
            {
                toUpsample.add(Pair.create(sstr, newSamplingLevel));
                remainingSpace -= avgEntrySize * IndexSummaryBuilder.entriesAtSamplingLevel(summary, newSamplingLevel);
            }
            else if (targetNumEntries < summary.size() * DOWNSAMPLE_THESHOLD)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * IndexSummaryBuilder.entriesAtSamplingLevel(summary, newSamplingLevel));
                toDownsample.add(Pair.create(sstr, Pair.create(spaceUsed, newSamplingLevel)));
                remainingSpace -= spaceUsed;
            }
            else
            {
                // keep the same sampling level
                remainingSpace -= summary.getOffHeapSize();
            }
            totalReadsPerSec -= readsPerSec;
        }

        toDownsample = distributeRemainingSpace(toDownsample, remainingSpace);
        for (Pair<SSTableReader, Pair<Long, Integer>> entry : toDownsample)
        {
            SSTableReader sstr = entry.left;
            logger.debug("Downsampling index summary for {} to {}/{} of the original number of entries",
                         sstr, entry.right.right, IndexSummary.BASE_SAMPLING_LEVEL);
            IndexSummary newSummary = IndexSummaryBuilder.downsample(sstr.getIndexSummary(), entry.right.right, sstr.partitioner);
            sstr.setIndexSummary(newSummary);
        }

        for (Pair<SSTableReader, Integer> entry : toUpsample)
        {
            SSTableReader sstr = entry.left;
            logger.debug("Upsampling index summary for {} to {}/{} of the original number of entries",
                         sstr, entry.right, IndexSummary.BASE_SAMPLING_LEVEL);
            try
            {
                sstr.rebuildSummary(entry.right);
            }
            catch (IOException ioe)
            {
                logger.error("Failed to rebuild index summary: ", ioe);
            }
        }
    }

    @VisibleForTesting
    static List<Pair<SSTableReader, Pair<Long, Integer>>> distributeRemainingSpace(List<Pair<SSTableReader, Pair<Long, Integer>>> toDownsample, long remainingSpace)
    {
        // sort by read rate (descending)
        Collections.sort(toDownsample, new Comparator<Pair<SSTableReader, Pair<Long, Integer>>>()
        {
            public int compare(Pair<SSTableReader, Pair<Long, Integer>> o1, Pair<SSTableReader, Pair<Long, Integer>> o2)
            {
                RestorableMeter a = o1.left.readMeter;
                RestorableMeter b = o2.left.readMeter;
                if (a == null && b == null)
                    return 0;
                else if (a == null)
                    return 1;
                else if (b == null)
                    return -1;
                else
                    return -1 * Double.compare(a.fifteenMinuteRate(), b.fifteenMinuteRate());
            }
        });

        while (remainingSpace > 0 && !toDownsample.isEmpty())
        {
            boolean didAdjust = false;
            List<Pair<SSTableReader, Pair<Long, Integer>>> newToDownsample = new ArrayList<>(toDownsample.size());
            for (Pair<SSTableReader, Pair<Long, Integer>> entry : toDownsample)
            {
                SSTableReader sstr = entry.left;
                long plannedSpaceUsed = entry.right.left;
                int currentLevel = entry.right.right;

                IndexSummary summary = sstr.getIndexSummary();
                int entriesAtNextLevel = IndexSummaryBuilder.entriesAtSamplingLevel(sstr.getIndexSummary(), currentLevel + 1);
                double avgEntrySize = summary.getOffHeapSize() / (double) summary.size();
                long spaceAtNextLevel = (long) Math.ceil(avgEntrySize * entriesAtNextLevel);
                long extraSpaceRequired = (spaceAtNextLevel - plannedSpaceUsed);
                // see if we have enough leftover space to increase the sampling level
                if (extraSpaceRequired <= remainingSpace)
                {
                    didAdjust = true;
                    remainingSpace -= extraSpaceRequired;
                    // if increasing the level would put us back to the original level, just leave this sstable out
                    // of the set to downsample
                    if (currentLevel + 1 < summary.getSamplingLevel())
                        newToDownsample.add(Pair.create(sstr, Pair.create(spaceAtNextLevel, currentLevel + 1)));
                }
                else
                {
                    newToDownsample.add(entry);
                }
            }
            toDownsample = newToDownsample;
            if (!didAdjust)
                break;
        }
        return toDownsample;
    }
}