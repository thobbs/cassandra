package org.apache.cassandra.poc;
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


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.KeyspaceAttributes;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import java.util.*;


public class WriteTaskTest extends CQLTester
{
    @Test
    public void testBatches()
    {
        String keyspaceName = "write_task_test";
        KeyspaceAttributes keyspaceAttributes = new KeyspaceAttributes();
        Map<String, String> replicationOptions = new HashMap<>();
        replicationOptions.put(ReplicationParams.CLASS, "LocalStrategy");
        keyspaceAttributes.addProperty(KeyspaceParams.Option.REPLICATION.toString(), replicationOptions);
        CreateKeyspaceStatement statement = new CreateKeyspaceStatement(keyspaceName, keyspaceAttributes, true);
        statement.announceMigration(true);  // announce locally

        String tableName = createTableName();
        schemaChange("CREATE TABLE " + keyspaceName + "." + tableName + " (k int PRIMARY KEY, v int)");
        EventLoop eventLoop = new EventLoop();

        CFMetaData cfm = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName).metadata;
        CBuilder builder = CBuilder.create(cfm.comparator);
        Clustering clustering = builder.build();

        LivenessInfo info =  LivenessInfo.create(FBUtilities.timestampMicros(), 0, FBUtilities.nowInSeconds());
        Row row = BTreeRow.noCellLiveRow(clustering, info);

        ArrayList<Mutation> mutations = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++)
        {
            DecoratedKey key = cfm.decorateKey(Int32Type.instance.getSerializer().serialize(i));
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, key, row);
            mutations.add(new Mutation(update));
        }

        WriteTask task = new WriteTask(mutations, ConsistencyLevel.ONE);
        eventLoop.scheduleTask(task);
        long startTime = System.currentTimeMillis();
        while (task.status() != Task.Status.COMPLETED)
            eventLoop.cycle();
        logger.info("WriteTasks took {}ms to execute", System.currentTimeMillis() - startTime);
        assert task.status() != Task.Status.NEW;
    }

    @Test
    public void testPaxos()
    {
        MessagingService.instance().listen();

        String keyspaceName = "paxos_write_task_test";
        KeyspaceAttributes keyspaceAttributes = new KeyspaceAttributes();
        Map<String, String> replicationOptions = new HashMap<>();
        replicationOptions.put(ReplicationParams.CLASS, "LocalStrategy");
        keyspaceAttributes.addProperty(KeyspaceParams.Option.REPLICATION.toString(), replicationOptions);
        CreateKeyspaceStatement createKeyspaceStatement = new CreateKeyspaceStatement(keyspaceName, keyspaceAttributes, true);
        createKeyspaceStatement.announceMigration(true);  // announce locally

        String tableName = createTableName();
        schemaChange("CREATE TABLE " + keyspaceName + "." + tableName + " (k int PRIMARY KEY, v int)");
        EventLoop eventLoop = new EventLoop();

        CFMetaData cfm = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName).metadata;

        String query = "INSERT INTO " + keyspaceName + "." + tableName + "(k, v) VALUES (0, 0) IF NOT EXISTS";
        ModificationStatement statement = (ModificationStatement) QueryProcessor.prepareInternal(query).statement;
        CQL3CasRequest casRequest = statement.makeCasRequest(QueryState.forInternalCalls(), QueryOptions.DEFAULT);

        DecoratedKey key = cfm.decorateKey(Int32Type.instance.getSerializer().serialize(0));
        PaxosWriteTask task = new PaxosWriteTask(keyspaceName, tableName, key, casRequest, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, ClientState.forInternalCalls());

        // WriteTask task = new WriteTask(mutations, ConsistencyLevel.ONE);
        eventLoop.scheduleTask(task);
        long startTime = System.currentTimeMillis();
        while (task.status() != Task.Status.COMPLETED)
            eventLoop.cycle();
        logger.info("PaxosWriteTask took {}ms to execute", System.currentTimeMillis() - startTime);
        assert task.status() != Task.Status.NEW;
    }
}
