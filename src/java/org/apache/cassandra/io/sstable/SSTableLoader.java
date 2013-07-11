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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.*;

/**
 * Cassandra SSTable bulk loader.
 * Load an externally created sstable into a cluster.
 */
public class SSTableLoader
{
    private final File directory;
    private final String keyspace;
    private final Client client;
    private final OutputHandler outputHandler;

    private final List<SSTableReader> sstables = new ArrayList<SSTableReader>();
    private final Map<InetAddress, EndpointStreamingDetails> endpointToStreamingDetails = new HashMap<InetAddress, EndpointStreamingDetails>();

    static
    {
        Config.setLoadYaml(false);
    }

    public SSTableLoader(File directory, Client client, OutputHandler outputHandler)
    {
        this.directory = directory;
        this.keyspace = directory.getParentFile().getName();
        this.client = client;
        this.outputHandler = outputHandler;
    }

    protected void openSSTables(final Map<InetAddress, Collection<Range<Token>>> endpointToRanges)
    {
        outputHandler.output("Opening sstables and calculating sections to stream");

        directory.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (new File(dir, name).isDirectory())
                    return false;
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (p == null || !p.right.equals(Component.DATA) || desc.temporary)
                    return false;

                if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists())
                {
                    outputHandler.output(String.format("Skipping file %s because index is missing", name));
                    return false;
                }

                if (!client.validateColumnFamily(keyspace, desc.cfname))
                {
                    outputHandler.output(String.format("Skipping file %s: column family %s.%s doesn't exist", name, keyspace, desc.cfname));
                    return false;
                }

                Set<Component> components = new HashSet<Component>();
                components.add(Component.DATA);
                components.add(Component.PRIMARY_INDEX);
                if (new File(desc.filenameFor(Component.SUMMARY)).exists())
                    components.add(Component.SUMMARY);
                if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists())
                    components.add(Component.COMPRESSION_INFO);
                if (new File(desc.filenameFor(Component.STATS)).exists())
                    components.add(Component.STATS);

                try
                {
                    // To conserve heap space, open SSTableReaders without bloom filters and discard
                    // the index summary after calculating the file sections to stream and the estimated
                    // number of keys for each endpoint.  See CASSANDRA-5555 for details.
                    SSTableReader sstable = SSTableReader.openForBatch(desc, components, client.getPartitioner());
                    sstables.add(sstable);

                    // calculate the sstable sections to stream as well as the estimated number of
                    // keys per host
                    for (Map.Entry<InetAddress, Collection<Range<Token>>> entry: endpointToRanges.entrySet())
                    {
                        InetAddress endpoint = entry.getKey();
                        Collection<Range<Token>> tokenRanges = entry.getValue();

                        List<Pair<Long, Long>> sstableSections = sstable.getPositionsForRanges(tokenRanges);
                        Long estimatedKeys = sstable.estimatedKeysForRanges(tokenRanges);

                        EndpointStreamingDetails details = endpointToStreamingDetails.get(endpoint);
                        if (details == null)
                        {
                            details = new EndpointStreamingDetails();
                            endpointToStreamingDetails.put(endpoint, details);
                        }

                        details.addSSTableDetails(estimatedKeys, sstableSections);
                    }

                    sstable.releaseSummary();
                }
                catch (IOException e)
                {
                    outputHandler.output(String.format("Skipping file %s, error opening it: %s", name, e.getMessage()));
                }
                return false;
            }
        });
    }

    public LoaderFuture stream() throws IOException
    {
        return stream(Collections.<InetAddress>emptySet());
    }

    public LoaderFuture stream(Set<InetAddress> toIgnore) throws IOException
    {
        client.init(keyspace);
        outputHandler.output("Established Thrift connection to initial host");

        Map<InetAddress, Collection<Range<Token>>> filteredEndpointToRanges = client.getEndpointToRangesMap();
        Map<InetAddress, Collection<Range<Token>>> endpointToRanges = client.getEndpointToRangesMap();
        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry: endpointToRanges.entrySet())
        {
            InetAddress remote = entry.getKey();
            if (!toIgnore.contains(remote))
                filteredEndpointToRanges.put(entry.getKey(), entry.getValue());
        }

        openSSTables(filteredEndpointToRanges);
        if (sstables.isEmpty())
        {
            outputHandler.output("No sstables to stream");
            return new LoaderFuture(0);
        }

        outputHandler.output(String.format("Streaming relevant part of %s to %s", names(sstables), endpointToRanges.keySet()));

        // There will be one streaming session by endpoint
        LoaderFuture future = new LoaderFuture(endpointToRanges.size());
        for (InetAddress remote : endpointToRanges.keySet())
        {
            if (toIgnore.contains(remote))
            {
                future.latch.countDown();
            }
            else
            {
                StreamOutSession session = StreamOutSession.create(keyspace, remote, new CountDownCallback(future, remote));
                // transferSSTables assumes references have been acquired
                EndpointStreamingDetails details = endpointToStreamingDetails.get(remote);
                SSTableReader.acquireReferences(sstables);
                StreamOut.transferSSTables(session, sstables, details.perSSTableSections, details.estimatedKeysPerSSTable, OperationType.BULK_LOAD);
                future.setPendings(remote, session.getFiles());
            }
        }
        return future;
    }

    private class EndpointStreamingDetails
    {
        public List<Long> estimatedKeysPerSSTable = new ArrayList<Long>();
        public List<List<Pair<Long, Long>>> perSSTableSections = new ArrayList<List<Pair<Long, Long>>>();

        public void addSSTableDetails (Long estimatedKeys, List<Pair<Long, Long>> sstableSections)
        {
            estimatedKeysPerSSTable.add(estimatedKeys);
            perSSTableSections.add(sstableSections);
        }
    }

    public static class LoaderFuture implements Future<Void>
    {
        final CountDownLatch latch;
        final Map<InetAddress, Collection<PendingFile>> pendingFiles;
        private List<InetAddress> failedHosts = new ArrayList<InetAddress>();

        private LoaderFuture(int request)
        {
            latch = new CountDownLatch(request);
            pendingFiles = new HashMap<InetAddress, Collection<PendingFile>>();
        }

        private void setPendings(InetAddress remote, Collection<PendingFile> files)
        {
            pendingFiles.put(remote, new ArrayList(files));
        }

        private void setFailed(InetAddress addr)
        {
            failedHosts.add(addr);
        }

        public List<InetAddress> getFailedHosts()
        {
            return failedHosts;
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException("Cancellation is not yet supported");
        }

        public Void get() throws InterruptedException
        {
            latch.await();
            return null;
        }

        public Void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
        {
            if (latch.await(timeout, unit))
                return null;
            else
                throw new TimeoutException();
        }

        public boolean isCancelled()
        {
            // For now, cancellation is not supported, maybe one day...
            return false;
        }

        public boolean isDone()
        {
            return latch.getCount() == 0;
        }

        public boolean hadFailures()
        {
            return failedHosts.size() > 0;
        }

        public Map<InetAddress, Collection<PendingFile>> getPendingFiles()
        {
            return pendingFiles;
        }
    }

    private String names(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
            builder.append(sstable.descriptor.filenameFor(Component.DATA)).append(" ");
        return builder.toString();
    }

    private class CountDownCallback implements IStreamCallback
    {
        private final InetAddress endpoint;
        private final LoaderFuture future;

        CountDownCallback(LoaderFuture future, InetAddress endpoint)
        {
            this.future = future;
            this.endpoint = endpoint;
        }

        public void onSuccess()
        {
            future.latch.countDown();
            outputHandler.debug(String.format("Streaming session to %s completed (waiting on %d outstanding sessions)", endpoint, future.latch.getCount()));

            // There could be race with stop being called twice but it should be ok
            if (future.latch.getCount() == 0)
                client.stop();
        }

        public void onFailure()
        {
            outputHandler.output(String.format("Streaming session to %s failed", endpoint));
            future.setFailed(endpoint);
            future.latch.countDown();
            client.stop();
        }
    }

    public static abstract class Client
    {
        private final Map<InetAddress, Collection<Range<Token>>> endpointToRanges = new HashMap<InetAddress, Collection<Range<Token>>>();
        private IPartitioner partitioner;

        /**
         * Initialize the client.
         * Perform any step necessary so that after the call to the this
         * method:
         *   * partitioner is initialized
         *   * getEndpointToRangesMap() returns a correct map
         * This method is guaranteed to be called before any other method of a
         * client.
         */
        public abstract void init(String keyspace);

        /**
         * Stop the client.
         */
        public void stop() {}

        /**
         * Validate that {@code keyspace} is an existing keyspace and {@code
         * cfName} one of its existing column family.
         */
        public abstract boolean validateColumnFamily(String keyspace, String cfName);

        public Map<InetAddress, Collection<Range<Token>>> getEndpointToRangesMap()
        {
            return endpointToRanges;
        }

        protected void setPartitioner(String partclass) throws ConfigurationException
        {
            setPartitioner(FBUtilities.newPartitioner(partclass));
        }

        protected void setPartitioner(IPartitioner partitioner) throws ConfigurationException
        {
            this.partitioner = partitioner;
            DatabaseDescriptor.setPartitioner(partitioner);
        }

        public IPartitioner getPartitioner()
        {
            return partitioner;
        }

        protected void addRangeForEndpoint(Range<Token> range, InetAddress endpoint)
        {
            Collection<Range<Token>> ranges = endpointToRanges.get(endpoint);
            if (ranges == null)
            {
                ranges = new HashSet<Range<Token>>();
                endpointToRanges.put(endpoint, ranges);
            }
            ranges.add(range);
        }
    }
}
