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
package org.apache.cassandra.poc;

import java.net.InetAddress;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.db.monitoring.ConstructionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.poc.events.LocalReadResponse;
import org.apache.cassandra.poc.events.RemoteFailure;
import org.apache.cassandra.poc.events.RemoteResponse;

public final class Adapters
{
    public static int sendMessage(EventLoop eventLoop, Task task, MessageOut message, InetAddress endpoint)
    {
        return MessagingService.instance().sendRR(message, endpoint, new MessagingServiceCallback(eventLoop, task));
    }

    /**
     *  The local read is always a full DATA read, never a digest.
     */
    public static void readLocally(EventLoop eventLoop, Task task, ReadCommand command)
    {
        StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(eventLoop, task, command));
    }

    private static final class MessagingServiceCallback implements IAsyncCallbackWithFailure
    {
        private final EventLoop loop;
        private final Task task;

        private MessagingServiceCallback(EventLoop loop, Task task)
        {
            this.loop = loop;
            this.task = task;
        }

        public void onFailure(InetAddress address, int id)
        {
            loop.emitEvent(new RemoteFailure(task, id, address));
        }

        public void response(MessageIn message, int id)
        {
            loop.emitEvent(new RemoteResponse(task, id, message));
        }

        public boolean isLatencyForSnitch()
        {
            return true;
        }
    }

    private static final class LocalReadRunnable implements Runnable
    {
        private final EventLoop loop;
        private final Task task;
        private final ReadCommand command;

        private final long enqueuedAt;

        LocalReadRunnable(EventLoop loop, Task task, ReadCommand command)
        {
            this.loop = loop;
            this.task = task;
            this.command = command;

            enqueuedAt = ApproximateTime.currentTimeMillis();
        }

        public void run()
        {
            long timeWaited = timeWaited();

            if (timeWaited < DatabaseDescriptor.getReadRpcTimeout())
                read();
            else
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.READ, timeWaited);
        }

        private void read()
        {
            command.setMonitoringTime(new ConstructionTime(enqueuedAt), DatabaseDescriptor.getReadRpcTimeout());

            try (ReadExecutionController executionController = command.executionController();
                 UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
            {
                ReadResponse response = command.createResponse(iterator);

                if (command.complete())
                    loop.emitEvent(new LocalReadResponse(task, response));
                else
                    MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.READ, timeWaited());
            }
            catch (TombstoneOverwhelmingException e)
            {
                // TODO
            }
        }

        private long timeWaited()
        {
            return ApproximateTime.currentTimeMillis() - enqueuedAt;
        }
    }
}
