/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Atom;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.partitions.ArrayBackedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.dk;
import static org.junit.Assert.*;


public class CompactionsPurgeTest
{
    private static final String KEYSPACE1 = "CompactionsPurgeTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String KEYSPACE2 = "CompactionsPurgeTest2";
    private static final String KEYSPACE_CACHED = "CompactionsPurgeTestCached";
    private static final String CF_CACHED = "CachedCF";
    private static final String KEYSPACE_CQL = "cql_keyspace";
    private static final String CF_CQL = "table1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
        SchemaLoader.createKeyspace(KEYSPACE2,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
        SchemaLoader.createKeyspace(KEYSPACE_CACHED,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE_CACHED, CF_CACHED).caching(CachingOptions.ALL));
        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                CFMetaData.compile("CREATE TABLE " + CF_CQL + " ("
                        + "k int PRIMARY KEY,"
                        + "v1 text,"
                        + "v2 int"
                        + ")", KEYSPACE_CQL));
    }

    @Test
    public void testMajorCompactionPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 0, key);
            builder.clustering(String.valueOf(i))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build().applyUnsafe();
        }

        cfs.forceBlockingFlush();

        // deletes
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata, 1, key, String.valueOf(i)).applyUnsafe();
        }
        cfs.forceBlockingFlush();

        // resurrect one column
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 2, key);
        builder.clustering(String.valueOf(5))
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build().applyUnsafe();

        cfs.forceBlockingFlush();

        // major compact and test that all columns but the resurrected one is completely gone
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));
        cfs.invalidateCachedPartition(dk(key));

        ArrayBackedPartition partition = Util.materializePartition(cfs, dk(key));
        assertEquals(1,partition.rowsWithNonExpiringCells());
        assertTrue(ByteBufferUtil.compareUnsigned(partition.lastRow().clustering().get(0), ByteBufferUtil.bytes("5")) == 0);
    }

    @Test
    public void testMinorCompactionPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        for (int k = 1; k <= 2; ++k) {
            String key = "key" + k;

            // inserts
            for (int i = 0; i < 10; i++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 0, key);
                builder.clustering(String.valueOf(i))
                        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .build().applyUnsafe();
            }
            cfs.forceBlockingFlush();

            // deletes
            for (int i = 0; i < 10; i++)
            {
                RowUpdateBuilder.deleteRow(cfs.metadata, 1, key, String.valueOf(i)).applyUnsafe();
            }

            cfs.forceBlockingFlush();
        }

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");

        // flush, remember the current sstable and then resurrect one column
        // for first key. Then submit minor compaction on remembered sstables.
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();

        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 2, "key1");
        builder.clustering(String.valueOf(5))
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build().applyUnsafe();

        cfs.forceBlockingFlush();
        cfs.getCompactionStrategy().getUserDefinedTask(sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        // verify that minor compaction does GC when key is provably not
        // present in a non-compacted sstable
        ArrayBackedPartition partition = Util.materializePartition(cfs, key2);
        assertEquals(0, partition.rowsWithNonExpiringCells());

        // verify that minor compaction still GC when key is present
        // in a non-compacted sstable but the timestamp ensures we won't miss anything
        partition = Util.materializePartition(cfs, key1);
        assertEquals(1,partition.rowsWithNonExpiringCells());
        assertTrue(ByteBufferUtil.compareUnsigned(partition.lastRow().clustering().get(0), ByteBufferUtil.bytes("5")) == 0);
    }

    /**
     * verify that we don't drop tombstones during a minor compaction that might still be relevant
     */
    @Test
    public void testMinTimestampPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        String key3 = "key3";

        // inserts
        new RowUpdateBuilder(cfs.metadata, 8, key3)
            .clustering("c1")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build().applyUnsafe();

        new RowUpdateBuilder(cfs.metadata, 8, key3)
        .clustering("c2")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build().applyUnsafe();

        cfs.forceBlockingFlush();
        // delete c1
        RowUpdateBuilder.deleteRow(cfs.metadata, 10, key3, "c1").applyUnsafe();

        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();

        // delete c2 so we have new delete in a diffrent SSTable
        RowUpdateBuilder.deleteRow(cfs.metadata, 9, key3, "c2").applyUnsafe();
        cfs.forceBlockingFlush();

        // compact the sstables with the c1/c2 data and the c1 tombstone
        cfs.getCompactionStrategy().getUserDefinedTask(sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        // We should have both the c1 and c2 tombstones still. Since the min timestamp in the c2 tombstone
        // sstable is older than the c1 tombstone, it is invalid to throw out the c1 tombstone.
        ArrayBackedPartition partition = Util.materializePartition(cfs, dk(key3));
        assertEquals(0,partition.rowsWithNonExpiringCells());
    }

    @Test
    public void testCompactionPurgeOneFile() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard2";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata, 1, key, String.valueOf(i)).applyUnsafe();
        }
        cfs.forceBlockingFlush();
        assertEquals(String.valueOf(cfs.getSSTables()), 1, cfs.getSSTables().size()); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertTrue(cfs.getSSTables().isEmpty());

        ArrayBackedPartition partition = Util.materializePartition(cfs, dk(key));
        assertTrue(partition.isEmpty());
    }


    @Test
    public void testCompactionPurgeCachedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = KEYSPACE_CACHED;
        String cfName = CF_CACHED;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key3";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes partition
        Mutation rm = new Mutation(KEYSPACE_CACHED, dk(key));
        rm.add(PartitionUpdate.fullPartitionDelete(cfs.metadata, dk(key), 1, FBUtilities.nowInSeconds()));
        rm.applyUnsafe();

        // Adds another unrelated partition so that the sstable is not considered fully expired. We do not
        // invalidate the row cache in that latter case.
        new RowUpdateBuilder(cfs.metadata, 0, "key4").clustering("c").add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER).build().applyUnsafe();

        // move the key up in row cache (it should not be empty since we have the partition deletion info)
        assertFalse(Util.materializePartition(cfs, dk(key)).isEmpty());

        // flush and major compact
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();

        // Since we've force purging (by passing MAX_VALUE for gc_before), the row should have been invalidated and we should have no deletion info anymore
        assertTrue(Util.materializePartition(cfs, dk(key)).isEmpty());
    }

    @Test
    public void testCompactionPurgeTombstonedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = KEYSPACE1;
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        String key = "key3";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, i, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes partition with timestamp such that not all columns are deleted
        Mutation rm = new Mutation(KEYSPACE1, dk(key));
        rm.add(PartitionUpdate.fullPartitionDelete(cfs.metadata, dk(key), 4, 0));
        rm.applyUnsafe();

        ArrayBackedPartition partition = Util.materializePartition(cfs, dk(key));
        assertTrue(!partition.partitionLevelDeletion().isLive());

        // flush and major compact (with tombstone purging)
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertFalse(Util.materializePartition(cfs, dk(key)).isEmpty());

        // re-inserts with timestamp lower than delete
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, i, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // Check that the second insert went in
        partition = Util.materializePartition(cfs, dk(key));
        assertEquals(10, partition.rowsWithNonExpiringCells());
    }


    @Test
    public void testRowTombstoneObservedBeforePurging()
    {
        String keyspace = "cql_keyspace";
        String table = "table1";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // write a row out to one sstable
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                                     keyspace, table, 1, "foo", 1));
        cfs.forceBlockingFlush();

        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a second sstable
        QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        cfs.forceBlockingFlush();

        // basic check that the row is considered deleted
        assertEquals(2, cfs.getSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // compact the two sstables with a gcBefore that does *not* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) - 10000, false));

        // the data should be gone, but the tombstone should still exist
        assertEquals(1, cfs.getSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // write a row out to one sstable
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                                     keyspace, table, 1, "foo", 1));
        cfs.forceBlockingFlush();
        assertEquals(2, cfs.getSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a different sstable
        QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        cfs.forceBlockingFlush();

        // compact the two sstables with a gcBefore that *does* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) + 10000, false));

        // both the data and the tombstone should be gone this time
        assertEquals(0, cfs.getSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());
    }
}
