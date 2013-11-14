package org.apache.cassandra.io.sstable;

import java.util.*;

public class Downsampling
{
    /**
     * The base (down)sampling level determines the granularity at which we can down/upsample.
     *
     * A higher number allows us to approximate more closely the ideal sampling.  (It could also mean we do a lot of
     * expensive almost-no-op resamplings from N to N-1, but the thresholds in IndexSummaryManager prevent that.)
     *
     * BSL must be a power of two in order to have good sampling patterns. This cannot be changed without rebuilding
     * all index summaries at full sampling; for now we treat it as a constant.
     */
    public static final int BASE_SAMPLING_LEVEL = 128;

    /**
     * The lowest level we will downsample to: the coarsest summary will have (MSL / BSL) entries left.
     *
     * This can be anywhere from 1 to the base sampling level.
     */
    public static final int MIN_SAMPLING_LEVEL = 8;

    private static final Map<Integer, List<Integer>> samplePatternCache = new HashMap<>();

    private static final Map<Integer, List<Integer>> originalIndexCache = new HashMap<>();

    /**
     * Gets a list L of starting indices for downsampling rounds: the first round should start with the offset
     * given by L[0], the second by the offset in L[1], etc.
     *
     * @param samplingLevel the base sampling level
     *
     * @return A list of `samplingLevel` unique indices between 0 and `samplingLevel`
     */
    public static List<Integer> getSamplingPattern(int samplingLevel)
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

        samplePatternCache.put(samplingLevel, startIndices);
        return startIndices;
    }

    /**
     * Returns a list that can be used to translate current index summary indexes to their original index before
     * downsampling.  (This repeats every `samplingLevel`, so that's how many entries we return.)
     *
     * For example, if [7, 15] is returned, the current index summary entry at index 0 was originally
     * at index 7, and the current index 1 was originally at index 15.
     *
     * @param samplingLevel the current sampling level for the index summary
     *
     * @return a list of original indexes for current summary entries
     */
    public static List<Integer> getOriginalIndexes(int samplingLevel)
    {
        List<Integer> originalIndexes = originalIndexCache.get(samplingLevel);
        if (originalIndexes != null)
            return originalIndexes;

        List<Integer> pattern = getSamplingPattern(BASE_SAMPLING_LEVEL).subList(0, BASE_SAMPLING_LEVEL - samplingLevel);
        originalIndexes = new ArrayList<>(samplingLevel);
        for (int j = 0; j < BASE_SAMPLING_LEVEL; j++)
        {
            if (!pattern.contains(j))
                originalIndexes.add(j);
        }

        originalIndexCache.put(samplingLevel, originalIndexes);
        return originalIndexes;
    }

    public static int getEffectiveIndexIntervalAfterIndex(int index, int samplingLevel, int indexInterval)
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

    public static int[] getStartPoints(int currentSamplingLevel, int newSamplingLevel)
    {
        List<Integer> allStartPoints = getSamplingPattern(BASE_SAMPLING_LEVEL);

        // calculate starting indexes for sampling rounds
        int initialRound = BASE_SAMPLING_LEVEL - currentSamplingLevel;
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
}
