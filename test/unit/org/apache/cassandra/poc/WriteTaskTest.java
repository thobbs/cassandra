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
    public void testBatches() throws Throwable
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

        int numRows = 100000;
        ArrayList<Mutation> mutations = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++)
        {
            DecoratedKey key = cfm.decorateKey(Int32Type.instance.getSerializer().serialize(i));
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, key, row);
            mutations.add(new Mutation(update));
        }

        WriteTask task = new WriteTask(mutations, ConsistencyLevel.ONE);
        long startTime = System.currentTimeMillis();
        runTask(eventLoop, task);
        logger.info("WriteTask with {} mutations took {}ms to execute", numRows, System.currentTimeMillis() - startTime);

        assertRows(execute("SELECT count(*) from " + keyspaceName + "." + tableName), row((long) numRows));
    }

    @Test
    public void testPaxos() throws Throwable
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

        runTask(eventLoop, task);
        assertRows(execute("SELECT * FROM " + keyspaceName + "." + tableName), row(0, 0));

        // update row with CAS
        query = "UPDATE " + keyspaceName + "." + tableName + " SET v = 1 WHERE k = 0 IF v = 0";
        statement = (ModificationStatement) QueryProcessor.prepareInternal(query).statement;
        casRequest = statement.makeCasRequest(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
        task = new PaxosWriteTask(keyspaceName, tableName, key, casRequest, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, ClientState.forInternalCalls());

        runTask(eventLoop, task);
        assertRows(execute("SELECT * FROM " + keyspaceName + "." + tableName), row(0, 1));

        // update row with CAS that won't apply
        query = "UPDATE " + keyspaceName + "." + tableName + " SET v = 1 WHERE k = 0 IF v = 0";
        statement = (ModificationStatement) QueryProcessor.prepareInternal(query).statement;
        casRequest = statement.makeCasRequest(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
        task = new PaxosWriteTask(keyspaceName, tableName, key, casRequest, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, ClientState.forInternalCalls());

        runTask(eventLoop, task);
        assertRows(execute("SELECT * FROM " + keyspaceName + "." + tableName), row(0, 1));
    }

    private void runTask(EventLoop eventLoop, Task task) throws Throwable
    {
        eventLoop.scheduleTask(task);
        long startTime = System.currentTimeMillis();
        while (!task.status().isFinal() && System.currentTimeMillis() < (startTime + 10000))
            eventLoop.cycle();

        assert task.status().isFinal() : "Task took more than 10s";

        if (task.hasFailed())
            throw task.exception();
    }
}
