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
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Manages the fixed-size memory pool for index summaries, periodically resizing them
 * in order to give more memory to hot sstables and less memory to cold sstables.
 */
public class IndexSummaryManager implements IndexSummaryManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryManager.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=IndexSummaries";
    public static final IndexSummaryManager instance;

    private int resizeIntervalInMinutes = 0;
    private long memoryPoolBytes;

    // The target (or ideal) number of index summary entries must differ from the actual number of
    // entries by this ratio in order to trigger an upsample or downsample of the summary.  Because
    // upsampling requires reading the primary index in order to rebuild the summary, the threshold
    // for upsampling is is higher.
    static final double UPSAMPLE_THRESHOLD = 1.5;
    static final double DOWNSAMPLE_THESHOLD = 0.9;

    private final DebuggableScheduledThreadPoolExecutor executor;

    // our next scheduled resizing run
    private ScheduledFuture future;

    static
    {
        instance = new IndexSummaryManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private IndexSummaryManager()
    {
        executor = new DebuggableScheduledThreadPoolExecutor(1, "IndexSummaryManager", Thread.MIN_PRIORITY);

        long indexSummarySizeInMB = DatabaseDescriptor.getIndexSummaryCapacityInMB();
        int interval = DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes();
        logger.info("Initializing index summary manager with a memory pool size of {} MB and a resize interval of {} minutes",
                    indexSummarySizeInMB, interval);

        setMemoryPoolCapacityInMB(DatabaseDescriptor.getIndexSummaryCapacityInMB());
        setResizeIntervalInMinutes(DatabaseDescriptor.getIndexSummaryResizeIntervalInMinutes());
    }

    public int getResizeIntervalInMinutes()
    {
        return resizeIntervalInMinutes;
    }

    public void setResizeIntervalInMinutes(int resizeIntervalInMinutes)
    {
        int difference = resizeIntervalInMinutes - this.resizeIntervalInMinutes;
        this.resizeIntervalInMinutes = resizeIntervalInMinutes;

        long initialDelay;
        if (future != null)
        {
            long remaining = future.getDelay(TimeUnit.MINUTES);
            initialDelay = Math.max(0, remaining + difference);
            future.cancel(false);
        }
        else
        {
            initialDelay = this.resizeIntervalInMinutes;
        }

        if (this.resizeIntervalInMinutes < 0)
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
        }, initialDelay, resizeIntervalInMinutes, TimeUnit.MINUTES);
    }

    public long getMemoryPoolCapacityInMB()
    {
        return memoryPoolBytes / 1024L / 1024L;
    }

    public Map<String, Double> getSamplingRatios()
    {
        Pair<List<SSTableReader>, List<SSTableReader>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        Map<String, Double> ratios = new HashMap<>(compactingAndNonCompacting.left.size() + compactingAndNonCompacting.right.size());
        for (SSTableReader sstable : Iterables.concat(compactingAndNonCompacting.left, compactingAndNonCompacting.right))
            ratios.put(sstable.getFilename(), sstable.getIndexSummarySamplingLevel() / (double) Downsampling.BASE_SAMPLING_LEVEL);

        return ratios;
    }

    public double getAverageSamplingRatio()
    {
        Pair<List<SSTableReader>, List<SSTableReader>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        double total = 0.0;
        for (SSTableReader sstable : Iterables.concat(compactingAndNonCompacting.left, compactingAndNonCompacting.right))
            total += sstable.getIndexSummarySamplingLevel() / (double) Downsampling.BASE_SAMPLING_LEVEL;
        return total / (compactingAndNonCompacting.left.size() + compactingAndNonCompacting.right.size());
    }

    public void setMemoryPoolCapacityInMB(long memoryPoolCapacityInMB)
    {
        this.memoryPoolBytes = memoryPoolCapacityInMB * 1024L * 1024L;
    }

    /**
     * Returns the actual space consumed by index summaries for all sstables.
     * @return space currently used in MB
     */
    public double getMemoryPoolSizeInMB()
    {
        long total = 0;
        Pair<List<SSTableReader>, List<SSTableReader>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        for (SSTableReader sstable : Iterables.concat(compactingAndNonCompacting.left, compactingAndNonCompacting.right))
            total += sstable.getIndexSummaryOffHeapSize();
        return total / 1024.0 / 1024.0;
    }

    private Pair<List<SSTableReader>, List<SSTableReader>> getCompactingAndNonCompactingSSTables()
    {
        List<SSTableReader> allCompacting = new ArrayList<>();
        List<SSTableReader> allNonCompacting = new ArrayList<>();
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfStore: ks.getColumnFamilyStores())
            {
                Set<SSTableReader> allSSTables = cfStore.getDataTracker().getSSTables();
                Set<SSTableReader> nonCompacting = Sets.newHashSet(cfStore.getDataTracker().getUncompactingSSTables(allSSTables));
                allNonCompacting.addAll(nonCompacting);
                allCompacting.addAll(Sets.difference(allSSTables, nonCompacting));
            }
        }
        return Pair.create(allCompacting, allNonCompacting);
    }

    public void redistributeSummaries() throws IOException
    {
        Pair<List<SSTableReader>, List<SSTableReader>> compactingAndNonCompacting = getCompactingAndNonCompactingSSTables();
        redistributeSummaries(compactingAndNonCompacting.left, compactingAndNonCompacting.right, this.memoryPoolBytes);
    }

    /**
     * Attempts to fairly distribute a fixed pool of memory for index summaries across a set of SSTables based on
     * their recent read rates.
     * @param nonCompacting a list of sstables to share the memory pool across
     * @param memoryPoolBytes a size (in bytes) that the total index summary space usage should stay close to or
     *                        under, if possible
     * @return a list of new SSTableReader instances
     */
    @VisibleForTesting
    public static List<SSTableReader> redistributeSummaries(List<SSTableReader> compacting, List<SSTableReader> nonCompacting, long memoryPoolBytes) throws IOException
    {
        logger.debug("Beginning redistribution of index summaries for {} sstables with memory pool size {} MB",
                     nonCompacting.size(), memoryPoolBytes / 1024L / 1024L);

        double totalReadsPerSec = 0.0;
        for (SSTableReader sstable : nonCompacting)
        {
            if (sstable.readMeter != null)
            {
                totalReadsPerSec += sstable.readMeter.fifteenMinuteRate();
            }
        }
        logger.trace("Total reads/sec across all sstables in index summary resize process: {}", totalReadsPerSec);

        // copy and sort by read rates (ascending)
        List<SSTableReader> sstablesByHotness = new ArrayList<>(nonCompacting);
        Collections.sort(sstablesByHotness, new Comparator<SSTableReader>()
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

        long remainingBytes = memoryPoolBytes;
        for (SSTableReader sstable : compacting)
            remainingBytes -= sstable.getIndexSummaryOffHeapSize();

        // leave enough space for rebuilding the largest summary
        long largest = 0;
        for (SSTableReader sstable : nonCompacting)
            largest = Math.max(largest, sstable.getIndexSummaryOffHeapSize());
        remainingBytes -= largest;


        List<SSTableReader> newSSTables = adjustSamplingLevels(sstablesByHotness, totalReadsPerSec, remainingBytes);

        long total = 0;
        for (SSTableReader sstable : Iterables.concat(compacting, newSSTables))
            total += sstable.getIndexSummaryOffHeapSize();
        logger.debug("Completed resizing of index summaries; current approximate memory used: {} MB",
                     total / 1024.0 / 1024.0);

        return newSSTables;
    }

    private static List<SSTableReader> adjustSamplingLevels(List<SSTableReader> sstables,
                                                            double totalReadsPerSec, long memoryPoolCapacity) throws IOException
    {

        List<ResampleEntry> toDownsample = new ArrayList<>(sstables.size() / 4);
        List<ResampleEntry> toUpsample = new ArrayList<>(sstables.size() / 4);
        List<SSTableReader> newSSTables = new ArrayList<>(sstables.size());

        // Going from the coldest to the hottest sstables, try to give each sstable an amount of space proportional
        // to the number of total reads/sec it handles.
        long remainingSpace = memoryPoolCapacity;
        for (SSTableReader sstable : sstables)
        {
            double readsPerSec = sstable.readMeter == null ? 0.0 : sstable.readMeter.fifteenMinuteRate();
            long idealSpace = Math.round(remainingSpace * (readsPerSec / totalReadsPerSec));

            // figure out how many entries our idealSpace would buy us, and pick a new sampling level based on that
            int currentNumEntries = sstable.getIndexSummarySize();
            double avgEntrySize = sstable.getIndexSummaryOffHeapSize() / (double) currentNumEntries;
            long targetNumEntries = Math.max(1, Math.round(idealSpace / avgEntrySize));
            int currentSamplingLevel = sstable.getIndexSummarySamplingLevel();
            int newSamplingLevel = IndexSummaryBuilder.calculateSamplingLevel(currentSamplingLevel, currentNumEntries, targetNumEntries);
            int numEntriesAtNewSamplingLevel = IndexSummaryBuilder.entriesAtSamplingLevel(newSamplingLevel, sstable.getMaxIndexSummarySize());

            logger.trace("{} has {} reads/sec; ideal space for index summary: {} bytes ({} entries); considering moving from level " +
                         "{} ({} entries) to level {} ({} entries)",
                         sstable.getFilename(), readsPerSec, idealSpace, targetNumEntries, currentSamplingLevel, currentNumEntries, newSamplingLevel, numEntriesAtNewSamplingLevel);

            if (targetNumEntries >= currentNumEntries * UPSAMPLE_THRESHOLD && newSamplingLevel > currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toUpsample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= avgEntrySize * numEntriesAtNewSamplingLevel;
            }
            else if (targetNumEntries < currentNumEntries * DOWNSAMPLE_THESHOLD && newSamplingLevel < currentSamplingLevel)
            {
                long spaceUsed = (long) Math.ceil(avgEntrySize * numEntriesAtNewSamplingLevel);
                toDownsample.add(new ResampleEntry(sstable, spaceUsed, newSamplingLevel));
                remainingSpace -= spaceUsed;
            }
            else
            {
                // keep the same sampling level
                logger.trace("SSTable {} is within thresholds of ideal sampling", sstable);
                remainingSpace -= sstable.getIndexSummaryOffHeapSize();
                newSSTables.add(sstable);
            }
            totalReadsPerSec -= readsPerSec;
        }

        if (remainingSpace > 0)
        {
            Pair<List<SSTableReader>, List<ResampleEntry>> result = distributeRemainingSpace(toDownsample, remainingSpace);
            toDownsample = result.right;
            newSSTables.addAll(result.left);
        }

        // downsample first, then upsample
        toDownsample.addAll(toUpsample);
        Map<DataTracker, List<SSTableReader>> replacedByTracker = new HashMap<>();
        Map<DataTracker, List<SSTableReader>> replacementsByTracker = new HashMap<>();
        for (ResampleEntry entry : toDownsample)
        {
            SSTableReader sstable = entry.sstable;
            logger.debug("Re-sampling index summary for {} from {}/{} to {}/{} of the original number of entries",
                         sstable, sstable.getIndexSummarySamplingLevel(), Downsampling.BASE_SAMPLING_LEVEL,
                         entry.newSamplingLevel, Downsampling.BASE_SAMPLING_LEVEL);
            SSTableReader replacement = sstable.cloneWithNewSummarySamplingLevel(entry.newSamplingLevel);
            DataTracker tracker = Keyspace.open(sstable.getKeyspaceName()).getColumnFamilyStore(sstable.getColumnFamilyName()).getDataTracker();

            List<SSTableReader> replaced = replacedByTracker.get(tracker);
            if (replaced == null)
            {
                replaced = new ArrayList<>();
                replacedByTracker.put(tracker, replaced);
            }
            replaced.add(sstable);

            List<SSTableReader> replacements = replacementsByTracker.get(tracker);
            if (replacements == null)
            {
                replacements = new ArrayList<>();
                replacementsByTracker.put(tracker, replacements);
            }
            replacements.add(replacement);
        }

        for (Map.Entry<DataTracker, List<SSTableReader>> entry : replacedByTracker.entrySet())
        {
            DataTracker tracker = entry.getKey();
            tracker.replaceReaders(replacedByTracker.get(tracker), replacementsByTracker.get(tracker));
            newSSTables.addAll(replacementsByTracker.get(tracker));
        }

        return newSSTables;
    }

    @VisibleForTesting
    static Pair<List<SSTableReader>, List<ResampleEntry>> distributeRemainingSpace(List<ResampleEntry> toDownsample, long remainingSpace)
    {
        // sort by the amount of space regained by doing the downsample operation; we want to try to avoid operations
        // that will make little difference.
        Collections.sort(toDownsample, new Comparator<ResampleEntry>()
        {
            public int compare(ResampleEntry o1, ResampleEntry o2)
            {
                return Double.compare(o1.sstable.getIndexSummaryOffHeapSize() - o1.newSpaceUsed,
                                      o2.sstable.getIndexSummaryOffHeapSize() - o2.newSpaceUsed);
            }
        });

        int noDownsampleCutoff = 0;
        List<SSTableReader> willNotDownsample = new ArrayList<>();
        while (remainingSpace > 0 && noDownsampleCutoff < toDownsample.size())
        {
            ResampleEntry entry = toDownsample.get(noDownsampleCutoff);

            long extraSpaceRequired = entry.sstable.getIndexSummaryOffHeapSize() - entry.newSpaceUsed;
            // see if we have enough leftover space to keep the current sampling level
            if (extraSpaceRequired <= remainingSpace)
            {
                logger.trace("Using leftover space to keep {} at the current sampling level ({})",
                             entry.sstable, entry.sstable.getIndexSummarySamplingLevel());
                willNotDownsample.add(entry.sstable);
                remainingSpace -= extraSpaceRequired;
            }
            else
            {
                break;
            }

            noDownsampleCutoff++;
        }
        return Pair.create(willNotDownsample, toDownsample.subList(noDownsampleCutoff, toDownsample.size()));
    }

    private static class ResampleEntry
    {
        public final SSTableReader sstable;
        public final long newSpaceUsed;
        public final int newSamplingLevel;

        public ResampleEntry(SSTableReader sstable, long newSpaceUsed, int newSamplingLevel)
        {
            this.sstable = sstable;
            this.newSpaceUsed = newSpaceUsed;
            this.newSamplingLevel = newSamplingLevel;
        }
    }
}