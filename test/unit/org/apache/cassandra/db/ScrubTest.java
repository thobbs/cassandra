package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.atoms.AtomStats;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;


@RunWith(OrderedJUnit4ClassRunner.class)
public class ScrubTest extends CQLTester
{
    private static String compressionOpts;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        CQLTester.setUpClass();
        compressionOpts = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"))
                        ? " WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_kb': '4'}"
                        : " WITH compression = {}";
    }

    @Test
    public void testScrubOneRow() throws Throwable
    {
        CompactionManager.instance.disableAutoCompaction();
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compressionOpts);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        // insert data and verify we get it back w/ range query
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        cfs.forceBlockingFlush();
        assertRows(execute("SELECT * FROM %s"), row(0, 0));

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        assertRows(execute("SELECT * FROM %s"), row(0, 0));
    }

    /*
    // TODO this OOMS the test runner
    @Test
    public void testScrubCorruptedCounterRow() throws Throwable
    {
        // When compression is enabled, for testing corrupted chunks we need enough partitions to cover
        // at least 3 chunks of size COMPRESSION_CHUNK_LENGTH
        int numPartitions = 1000;

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b counter)" + compressionOpts);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        for (int i = 0; i < numPartitions; i++)
            execute("UPDATE %s SET b = b + ? WHERE a = ?", 100L, i);

        cfs.forceBlockingFlush();

        UntypedResultSet results = execute("SELECT * FROM %s");
        assertEquals(numPartitions, results.size());

        assertEquals(1, cfs.getSSTables().size());

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        //make sure to override at most 1 chunk when compression is enabled
        overrideWithGarbage(sstable, ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(1));

        // with skipCorrupted == false, the scrub is expected to fail
        try(Scrubber scrubber = new Scrubber(cfs, sstable, false, false))
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt rows will be skipped
        Scrubber.ScrubResult scrubResult;
        try(Scrubber scrubber = new Scrubber(cfs, sstable, true, false))
        {
            scrubResult = scrubber.scrubWithResult();
        }

        assertNotNull(scrubResult);

        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        if (compression)
        {
            assertEquals(0, scrubResult.emptyRows);
            assertEquals(numPartitions, scrubResult.badRows + scrubResult.goodRows);
            //because we only corrupted 1 chunk and we chose enough partitions to cover at least 3 chunks
            assertTrue(scrubResult.goodRows >= scrubResult.badRows * 2);
        }
        else
        {
            assertEquals(0, scrubResult.emptyRows);
            assertEquals(1, scrubResult.badRows);
            assertEquals(numPartitions-1, scrubResult.goodRows);
        }
        assertEquals(1, cfs.getSSTables().size());

        results = execute("SELECT * FROM %s");
        assertEquals(scrubResult.goodRows, results.size());
    }
    */

    @Test
    public void testScrubCorruptedRowInSmallFile() throws Throwable
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b counter)" + compressionOpts);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        for (int i = 0; i < 2; i++)
            execute("UPDATE %s SET b = b + ? WHERE a = ?", 100L, i);

        UntypedResultSet results = execute("SELECT * FROM %s");
        assertEquals(2, results.size());

        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();

        // overwrite one row with garbage
        overrideWithGarbage(sstable, ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(1));

        // with skipCorrupted == false, the scrub is expected to fail
        Scrubber scrubber = new Scrubber(cfs, sstable, false, false);
        try
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt row will be skipped
        scrubber = new Scrubber(cfs, sstable, true, false);

        // NOTE: this errors without CASSANDRA-9141
        scrubber.scrub();

        scrubber.close();
        assertEquals(1, cfs.getSSTables().size());

        // verify that we can read all of the rows, and there is now one less row
        results = execute("SELECT * FROM %s");
        assertEquals(1, results.size());
    }

    @Test
    public void testScrubOneRowWithCorruptedKey() throws Throwable
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compressionOpts);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        // insert data and verify we get it back w/ range query
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, 100);

        UntypedResultSet results = execute("SELECT * FROM %s");
        assertEquals(4, results.size());

        cfs.forceBlockingFlush();
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        overrideWithGarbage(sstable, 0, 2);

        CompactionManager.instance.performScrub(cfs, false);

        // NOTE: this fails without CASSANDRA-9140
        // check data is still there
        results = execute("SELECT * FROM %s");
        assertEquals(4, results.size());
    }

    /*
    // TODO this OOMs the test runner
    @Test
    public void testScrubCorruptedCounterRowNoEarlyOpen() throws Throwable
    {
        long oldOpenVal = SSTableRewriter.getOpenInterval();
        try
        {
            SSTableRewriter.overrideOpenInterval(Long.MAX_VALUE);
            testScrubCorruptedCounterRow();
        }
        finally
        {
            SSTableRewriter.overrideOpenInterval(oldOpenVal);
        }
    }
    */

    @Test
    public void testScrubDeletedRow() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compressionOpts + " AND gc_grace_seconds=0");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        execute("DELETE FROM %s WHERE a = ?", 0);
        cfs.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs, false);
        assert cfs.getSSTables().isEmpty();
    }

    @Test
    public void testScrubMultiRow() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compressionOpts + " AND gc_grace_seconds=0");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, 100);

        UntypedResultSet results = execute("SELECT * FROM %s");
        assertEquals(10, results.size());

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        results = execute("SELECT * FROM %s");
        assertEquals(10, results.size());
    }

    @Test
    public void testScrubOutOfOrder() throws Throwable
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        /*
         * Code used to generate an outOfOrder sstable. The test for out-of-order key in SSTableWriter must also be commented out.
         * The test also assumes an ordered partitioner.
         *
        long numKeys = 10;
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, 0);

        ArrayList<AtomIterator> iterators = new ArrayList<>();
        try
        {
            String root = System.getProperty("corrupt-sstable-root");
            assert root != null;
            File rootDir = new File(root);
            assert rootDir.isDirectory();

            Descriptor descriptor = new Descriptor(rootDir, KEYSPACE, tableName, 1, Descriptor.Type.FINAL);
            SSTableWriter writer = new BigTableWriter(
                    descriptor, numKeys, 0L, cfs.metadata, StorageService.getPartitioner(),
                    new MetadataCollector(cfs.metadata.comparator),
                    new SerializationHeader(cfs.metadata, cfs.metadata.partitionColumns(), AtomStats.NO_STATS, true));

            try (PartitionIterator partitionIterator = Util.getRangeSlice(cfs))
            {
                while (partitionIterator.hasNext())
                    iterators.add(partitionIterator.next());
            }

            // make the first key higher than the last to make validation fail when opening the sstable
            List<Integer> ordering = Arrays.asList(9, 0, 1, 2, 3, 4, 5, 8, 7, 6);
            for (int index : ordering)
                writer.append(iterators.get(index));

            writer.closeAndOpenReader();
        }
        finally
        {
            for (AtomIterator iterator : iterators)
                iterator.close();
        }
        */
        String root = System.getProperty("corrupt-sstable-root");
        assert root != null;
        File rootDir = new File(root);
        assert rootDir.isDirectory();

        Descriptor descriptor = new Descriptor(rootDir, KEYSPACE, tableName, 1, Descriptor.Type.FINAL);
        CFMetaData metadata = cfs.metadata;

        try
        {
            SSTableReader.open(descriptor, metadata);
            fail("SSTR validation should have caught the out-of-order rows");
        }
        catch (IllegalStateException ise) { }  // expected

        // open without validation for scrubbing
        Set<Component> components = new HashSet<>();
        components.add(Component.COMPRESSION_INFO);
        components.add(Component.DATA);
        components.add(Component.PRIMARY_INDEX);
        components.add(Component.FILTER);
        components.add(Component.STATS);
        components.add(Component.SUMMARY);
        components.add(Component.TOC);
        SSTableReader sstable = SSTableReader.openNoValidation(descriptor, components, cfs);

        try(Scrubber scrubber = new Scrubber(cfs, sstable, false, true))
        {
            scrubber.scrub();
        }
        cfs.loadNewSSTables();

        // TODO: this fails because scrub was unable to read the first key for some reason
        UntypedResultSet results = execute("SELECT * FROM %s");
        assertEquals(10, results.size());
        Iterator<UntypedResultSet.Row> iterator = results.iterator();
        for (int i = 0; i < 10; i++)
            assertEquals(i, iterator.next().getInt("a"));
    }

    private void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2) throws IOException
    {
        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        long startPosition, endPosition;

        if (compression)
        { // overwrite with garbage the compression chunks from key1 to key2
            CompressionMetadata compData = CompressionMetadata.create(sstable.getFilename());

            CompressionMetadata.Chunk chunk1 = compData.chunkFor(
                    sstable.getPosition(RowPosition.ForKey.get(key1, sstable.partitioner), SSTableReader.Operator.EQ).position);
            CompressionMetadata.Chunk chunk2 = compData.chunkFor(
                    sstable.getPosition(RowPosition.ForKey.get(key2, sstable.partitioner), SSTableReader.Operator.EQ).position);

            startPosition = Math.min(chunk1.offset, chunk2.offset);
            endPosition = Math.max(chunk1.offset + chunk1.length, chunk2.offset + chunk2.length);
        }
        else
        { // overwrite with garbage from key1 to key2
            long row0Start = sstable.getPosition(RowPosition.ForKey.get(key1, sstable.partitioner), SSTableReader.Operator.EQ).position;
            long row1Start = sstable.getPosition(RowPosition.ForKey.get(key2, sstable.partitioner), SSTableReader.Operator.EQ).position;
            startPosition = Math.min(row0Start, row1Start);
            endPosition = Math.max(row0Start, row1Start);
        }

        overrideWithGarbage(sstable, startPosition, endPosition);
    }

    private void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition) throws IOException
    {
        RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw");
        file.seek(startPosition);
        file.writeBytes(StringUtils.repeat('z', (int) (endPosition - startPosition)));
        file.close();
    }

    @Test
    public void testScrubColumnValidation() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))" + compressionOpts);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        execute("INSERT INTO %s (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false);
    }

    /**
     * For CASSANDRA-6892, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b text, c text, PRIMARY KEY (a, b)) " + compressionOpts + " AND COMPACT STORAGE");
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "a", "foo");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "b", "bar");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "c", "boo");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, true);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        assertRows(execute("SELECT * FROM %s"),
                row(0, "a", "foo"),
                row(0, "b", "bar"),
                row(0, "c", "boo"));
    }

    @Test // CASSANDRA-5174
    public void testScrubKeysIndex_preserveOrder() throws Throwable
    {
        //If the partitioner preserves the order then SecondaryIndex uses BytesType comparator,
        // otherwise it uses LocalByPartitionerType
        setKeyComparator(BytesType.instance);
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d bigint)");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, false, true);
    }

    @Test // CASSANDRA-5174
    public void testScrubCompositeIndex_preserveOrder() throws Throwable
    {
        setKeyComparator(BytesType.instance);
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d bigint, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, true, true);
    }

    @Test // CASSANDRA-5174
    public void testScrubKeysIndex() throws Throwable
    {
        setKeyComparator(new LocalByPartionerType(StorageService.getPartitioner()));
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d bigint)");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, false, true);
    }

    @Test // CASSANDRA-5174
    public void testScrubCompositeIndex() throws Throwable
    {
        setKeyComparator(new LocalByPartionerType(StorageService.getPartitioner()));
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d bigint, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, true, true);
    }

    @Test // CASSANDRA-5174
    public void testFailScrubKeysIndex() throws Throwable
    {
        // TODO: this is failing because the secondary index query after the rebuild isn't returning results
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d bigint)");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, false, false);
    }

    @Test // CASSANDRA-5174
    public void testFailScrubCompositeIndex() throws Throwable
    {
        // TODO: this is failing because the secondary index query after the rebuild isn't returning results
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d bigint, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, true, false);
    }

    @Test // CASSANDRA-5174
    public void testScrubTwice() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d bigint)");
        createIndex("CREATE INDEX ON %s (d)");
        testScrubIndex(tableName, false, true, true);
    }

    /** The SecondaryIndex class is used for custom indexes so to avoid
     * making a public final field into a private field with getters
     * and setters, we resort to this hack in order to test it properly
     * since it can have two values which influence the scrubbing behavior.
     * @param comparator - the key comparator we want to test
     */
    private void setKeyComparator(AbstractType<?> comparator)
    {
        try
        {
            Field keyComparator = SecondaryIndex.class.getDeclaredField("keyComparator");
            keyComparator.setAccessible(true);
            int modifiers = keyComparator.getModifiers();
            Field modifierField = keyComparator.getClass().getDeclaredField("modifiers");
            modifiers = modifiers & ~Modifier.FINAL;
            modifierField.setAccessible(true);
            modifierField.setInt(keyComparator, modifiers);

            keyComparator.set(null, comparator);
        }
        catch (Exception ex)
        {
            fail("Failed to change key comparator in secondary index : " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void testScrubIndex(String tableName, boolean composite, boolean ... scrubs)
            throws Throwable
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        int numRows = 1000;
        for (int i = 0; i < numRows; i++)
        {
            int partitionKey = composite ? (i - (i % 10)) : i;
            long indexedValue = (i % 2) == 0 ? 1L : 2L;
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", partitionKey, i, i, indexedValue);
        }

        // check index
        UntypedResultSet results = execute("SELECT * FROM %s WHERE d = ? ", 1L);
        assertEquals(numRows / 2, results.size());

        // scrub index
        Set<ColumnFamilyStore> indexCfss = cfs.indexManager.getIndexesBackedByCfs();
        assertTrue(indexCfss.size() == 1);
        ColumnFamilyStore indexCfs = indexCfss.iterator().next();
        indexCfs.forceBlockingFlush();

        for (boolean success : scrubs)
        {
            if (!success)
                overrideWithGarbage(indexCfs.getSSTables().iterator().next(), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(2L));

            CompactionManager.AllSSTableOpStatus result = indexCfs.scrub(false, false, true);
            CompactionManager.AllSSTableOpStatus expected = success
                                                          ? CompactionManager.AllSSTableOpStatus.SUCCESSFUL
                                                          : CompactionManager.AllSSTableOpStatus.ABORTED;
            assertEquals(expected, result);
        }

        // check index is still working (scrub failures should result in the index being rebuilt)
        results = execute("SELECT * FROM %s WHERE d = ?", 1L);
        assertEquals(numRows / 2, results.size());
    }
}
