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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;

/**
 * Manages {@link View}'s for a single {@link ColumnFamilyStore}. All of the views for that table are created when this
 * manager is initialized.
 *
 * The main purposes of the manager are to provide a single location for updates to be vetted to see whether they update
 * any views {@link ViewManager#updatesAffectView(Collection, boolean)}, provide locks to prevent multiple
 * updates from creating incoherent updates in the view {@link ViewManager#acquireLockFor(ByteBuffer)}, and
 * to affect change on the view.
 */
public class ViewManager
{
    public class ForStore
    {
        private final ConcurrentNavigableMap<String, View> viewsByName;

        public ForStore()
        {
            this.viewsByName = new ConcurrentSkipListMap<>();
        }

        public Iterable<View> allViews()
        {
            return viewsByName.values();
        }

        public Iterable<ColumnFamilyStore> allViewsCfs()
        {
            List<ColumnFamilyStore> viewColumnFamilies = new ArrayList<>();
            for (View view : allViews())
                viewColumnFamilies.add(keyspace.getColumnFamilyStore(view.getDefinition().viewName));
            return viewColumnFamilies;
        }

        public void forceBlockingFlush()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.forceBlockingFlush();
        }

        public void dumpMemtables()
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
                viewCfs.dumpMemtable();
        }

        public void truncateBlocking(long truncatedAt)
        {
            for (ColumnFamilyStore viewCfs : allViewsCfs())
            {
                ReplayPosition replayAfter = viewCfs.discardSSTables(truncatedAt);
                SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter);
            }
        }

        public void addView(View view)
        {
            viewsByName.put(view.name, view);
        }

        public void removeView(String name)
        {
            viewsByName.remove(name);
        }
    }

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentViewWriters() * 1024);
    private static final boolean disableCoordinatorBatchlog = Boolean.getBoolean("cassandra.mv_disable_coordinator_batchlog");

    private final ConcurrentNavigableMap<UUID, ForStore> viewManagersByStore;
    private final ConcurrentNavigableMap<String, View> viewsByName;
    private final Keyspace keyspace;

    public ViewManager(Keyspace keyspace)
    {
        this.viewManagersByStore = new ConcurrentSkipListMap<>();
        this.viewsByName = new ConcurrentSkipListMap<>();
        this.keyspace = keyspace;
    }

    /**
     * Calculates and pushes updates to the views replicas. The replicas are determined by
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     */
    public void pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog)
    {
        List<Mutation> mutations = null;
        TemporalRow.Set temporalRows = null;
        for (Map.Entry<String, View> view : viewsByName.entrySet())
        {
            temporalRows = view.getValue().getTemporalRowSet(update, temporalRows, false);

            Collection<Mutation> viewMutations = view.getValue().createMutations(update, temporalRows, false);
            if (viewMutations != null && !viewMutations.isEmpty())
            {
                if (mutations == null)
                    mutations = Lists.newLinkedList();
                mutations.addAll(viewMutations);
            }
        }

        if (mutations != null)
            StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog);
    }

    public boolean updatesAffectView(Collection<? extends IMutation> mutations, boolean coordinatorBatchlog)
    {
        if (coordinatorBatchlog && disableCoordinatorBatchlog)
            return false;

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate cf : mutation.getPartitionUpdates())
            {
                assert keyspace.getName().equals(cf.metadata().ksName);

                if (coordinatorBatchlog && keyspace.getReplicationStrategy().getReplicationFactor() == 1)
                    continue;

                for (View view : allViews())
                {
                    if (!cf.metadata().cfId.equals(view.getDefinition().baseTableId))
                        continue;

                    if (view.updateAffectsView(cf))
                        return true;
                }
            }
        }

        return false;
    }

    public Iterable<View> allViews()
    {
        return viewsByName.values();
    }

    public void reload()
    {
        Map<String, ViewDefinition> newViewsByName = new HashMap<>();
        for (ViewDefinition definition : keyspace.getMetadata().views)
        {
            newViewsByName.put(definition.viewName, definition);
        }

        for (String viewName : viewsByName.keySet())
        {
            if (!newViewsByName.containsKey(viewName))
                removeView(viewName);
        }

        for (Map.Entry<String, ViewDefinition> entry : newViewsByName.entrySet())
        {
            if (!viewsByName.containsKey(entry.getKey()))
                addView(entry.getValue());
        }

        for (View view : allViews())
        {
            view.build();
            // We provide the new definition from the base metadata
            view.updateDefinition(newViewsByName.get(view.name));
        }
    }

    public void addView(ViewDefinition definition)
    {
        View view = new View(definition, keyspace.getColumnFamilyStore(definition.baseTableId));
        forTable(view.getDefinition().baseTableId).addView(view);
        viewsByName.put(definition.viewName, view);
    }

    public void removeView(String name)
    {
        View view = viewsByName.remove(name);

        if (view == null)
            return;

        forTable(view.getDefinition().baseTableId).removeView(name);
        SystemKeyspace.setViewRemoved(keyspace.getName(), view.name);
    }

    public void buildAllViews()
    {
        for (View view : allViews())
            view.build();
    }

    public ForStore forTable(UUID baseId)
    {
        ForStore forStore = viewManagersByStore.get(baseId);
        if (forStore == null)
        {
            forStore = new ForStore();
            ForStore previous = viewManagersByStore.put(baseId, forStore);
            if (previous != null)
                forStore = previous;
        }
        return forStore;
    }

    public static Lock acquireLockFor(ByteBuffer key)
    {
        Lock lock = LOCKS.get(key);

        if (lock.tryLock())
            return lock;

        return null;
    }
}
