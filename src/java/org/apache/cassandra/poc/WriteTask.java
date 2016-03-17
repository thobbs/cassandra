package org.apache.cassandra.poc;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.poc.events.Event;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel.Timer;

import java.net.InetAddress;
import java.util.*;

public class WriteTask extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

    private static final int CHUNK_SIZE = 64;  // number of mutations to process before yielding to the event loop

    private EventLoop eventLoop;

    private long startTime;
    private Exception error;

    // TODO where do these get set?
    public Collection<? extends IMutation> mutations = Collections.EMPTY_LIST;
    private Iterator<? extends IMutation> mutationIterator;
    public ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private ArrayList<MutationTask> mutationTasks;
    private int remaining;

    // TODO maybe not the best way to expose this
    public EventLoop eventLoop()
    {
        return this.eventLoop;
    }

    public Status initialize(EventLoop eventLoop)
    {
        startTime = System.nanoTime();

        this.eventLoop = eventLoop;
        mutationTasks = new ArrayList<>(mutations.size());
        mutationIterator = mutations.iterator();
        remaining = mutations.size();
        status = processMutations();
        return status;
    }

    private Status processMutations()
    {
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        int chunkCount = 0;
        while (mutationIterator.hasNext() && chunkCount < CHUNK_SIZE)
        {
            Mutation mutation = (Mutation) mutationIterator.next();
            chunkCount++;

            // TODO breakup multi-partition batch into chunks, yield and resume
            WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

            // start StorageProxy.performWrite() behavior
            String keyspaceName = mutation.getKeyspaceName();
            AbstractReplicationStrategy replicationStrategy = Keyspace.open(keyspaceName).getReplicationStrategy();

            Token tk = mutation.key().getToken();
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

            MutationTask mutTask = new MutationTask(this, mutation);
            mutationTasks.add(mutTask);
            AbstractWriteResponseHandler<IMutation> responseHandler = replicationStrategy.getWriteResponseHandler(
                    naturalEndpoints, pendingEndpoints, consistencyLevel, null, wt, mutTask);
            mutTask.responseHandler = responseHandler;

            // exit early if we can't fulfill the CL at this time
            if (checkForUnavailable(responseHandler))
                return Status.COMPLETED;

            Iterable<InetAddress> targets = Iterables.concat(naturalEndpoints, pendingEndpoints);

            // start StorageProxy.sendToHintedEndpoints() behavior

            // extra-datacenter replicas, grouped by dc
            Map<String, Collection<InetAddress>> dcGroups = null;
            // only need to create a Message for non-local writes
            MessageOut<Mutation> message = null;

            boolean insertLocal = false;
            ArrayList<InetAddress> endpointsToHint = null;

            for (InetAddress destination : targets)
            {
                if (checkForHintOverload(destination))
                    return Status.COMPLETED;

                if (FailureDetector.instance.isAlive(destination))
                {
                    if (StorageProxy.canDoLocalRequest(destination))
                    {
                        insertLocal = true;
                    }
                    else
                    {
                        // belongs on a different server
                        if (message == null)
                            message = mutation.createMessage();
                        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                        // direct writes to local DC or old Cassandra versions
                        // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
                        if (localDataCenter.equals(dc))
                        {
                            MessagingService.instance().sendRR(message, destination, responseHandler, true);
                        }
                        else
                        {
                            Collection<InetAddress> messages = (dcGroups != null) ? dcGroups.get(dc) : null;
                            if (messages == null)
                            {
                                messages = new ArrayList<>(3); // most DCs will have <= 3 replicas
                                if (dcGroups == null)
                                    dcGroups = new HashMap<>();
                                dcGroups.put(dc, messages);
                            }
                            messages.add(destination);
                        }
                    }
                }
                else
                {
                    if (StorageProxy.shouldHint(destination))
                    {
                        if (endpointsToHint == null)
                            endpointsToHint = new ArrayList<>(Iterables.size(targets));
                        endpointsToHint.add(destination);
                    }
                }
            }

            // TODO submitting hints
            // if (endpointsToHint != null)
            //     submitHint(mutation, endpointsToHint, responseHandler);

            // write to remote DCs
            if (dcGroups != null)
            {
                // for each datacenter, send the message to one node to relay the write to other replicas
                if (message == null)
                    message = mutation.createMessage();

                for (Collection<InetAddress> dcTargets : dcGroups.values())
                    StorageProxy.sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
            }

            // write locally
            if (insertLocal)
            {
                // begin StorageProxy.performLocally() behavior
                mutTask.nowInSec = FBUtilities.nowInSeconds();
                mutTask.serializedSize = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

                Pair<ReplayPosition, OpOrder.Group> pair = mutation.writeCommitlogAsync(mutTask);
                mutTask.replayPosition = pair.left;
                mutTask.opGroup = pair.right;

                if (mutTask.replayPosition == null)
                {
                    // we are either waiting on commitlog allocation or syncing
                    continue;
                }

                mutTask.applyToMemtable();
                if (remaining == 0)
                    status = Status.COMPLETED;
            }
        }
        if (mutationIterator.hasNext())
            status = Status.RESCHEDULED;

        return status;
    }

    private boolean checkForUnavailable(AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        try
        {
            responseHandler.assureSufficientLiveNodes();
        }
        catch (UnavailableException exc)
        {
            error = exc;
            status = Status.COMPLETED;
            return true;
        }

        return false;
    }

    private boolean checkForHintOverload(InetAddress destination)
    {
        if (StorageProxy.hintsAreOverloaded(destination))
        {
            error = new OverloadedException(
                    "Too many in flight hints: " + StorageMetrics.totalHintsInProgress.getCount() +
                            " destination: " + destination +
                            " destination hints: " + StorageProxy.getHintsInProgressFor(destination).get());
            status = Status.COMPLETED;
            return true;
        }

        return false;
    }

    public Status resume(EventLoop eventLoop)
    {
        // logger.warn("#### resuming");
        return processMutations();
    }

    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        // TODO better organization of state transitions

        if (event instanceof WriteSuccessEvent)
        {
            // we've gotten acks on all writes
            StorageProxy.writeMetrics.addNano(System.nanoTime() - startTime);
            remaining--;
            return remaining == 0 ? Status.COMPLETED : Status.REGULAR;
        }
        else if (event instanceof WriteTimeoutEvent)
        {
            WriteTimeoutEvent wte = (WriteTimeoutEvent) event;
            WriteTimeoutException exc = new WriteTimeoutException(wte.writeType, wte.consistencyLevel, wte.acks, wte.blockedFor);
            wte.task.error = exc;
            error = exc;
            StorageProxy.writeMetrics.timeouts.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof WriteFailureEvent)
        {
            WriteFailureEvent wfe = (WriteFailureEvent) event;
            WriteFailureException exc = new WriteFailureException(wfe.consistencyLevel, wfe.acks, wfe.failures, wfe.blockedFor, wfe.writeType);
            wfe.task.error = exc;
            error = exc;
            StorageProxy.writeMetrics.failures.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof CommitlogSegmentAvailableEvent)
        {
            MutationTask mutTask = ((CommitlogSegmentAvailableEvent) event).task;
            int totalSize = mutTask.serializedSize + CommitLogSegment.ENTRY_OVERHEAD_SIZE;
            CommitLogSegment.Allocation allocation = CommitLog.instance.allocator.allocateAsync(
                    mutTask.mutation, totalSize, ((CommitlogSegmentAvailableEvent) event).segment, mutTask);
            if (allocation == null)
                return Status.REGULAR;  // allocation failed, wait for another CommitlogSegmentAvailableEvent

            mutTask.replayPosition = CommitLog.instance.writeToAllocationAndSync(mutTask.mutation, allocation, mutTask);
            if (mutTask.replayPosition == null)
                return Status.REGULAR;  // commitlog sync would have blocked, wait for CommitlogSyncCompleteEvent

            mutTask.applyToMemtable();
            return remaining == 0 ? Status.COMPLETED : Status.REGULAR;
        }
        else if (event instanceof CommitlogSyncCompleteEvent)
        {
            // logger.warn("#### commitlog sync complete event");
            MutationTask mutTask = ((CommitlogSyncCompleteEvent) event).task;
            CommitLog.instance.executor.pending.decrementAndGet();
            CommitLogSegment.Allocation allocation = ((CommitlogSyncCompleteEvent) event).allocation;
            mutTask.replayPosition = allocation.getReplayPosition();
            mutTask.applyToMemtable();
            return remaining == 0 ? Status.COMPLETED : Status.REGULAR;
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        throw new UnsupportedOperationException();
    }

    public void cleanup(EventLoop eventLoop)
    {
        logger.warn("#### doing cleanup");
        for (MutationTask mutationTask : mutationTasks)
        {
            if (mutationTask.opGroup != null)
                mutationTask.opGroup.close();
        }
    }

    public static abstract class WriteTaskEvent implements Event
    {
        protected final MutationTask task;

        WriteTaskEvent(MutationTask task)
        {
            this.task = task;
        }

        public WriteTask task()
        {
            return task.writeTask;
        }
    }

    public static class WriteTimeoutEvent extends WriteTaskEvent
    {
        public final WriteType writeType;
        public final ConsistencyLevel consistencyLevel;
        public final int acks;
        public final int blockedFor;

        public WriteTimeoutEvent(MutationTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor)
        {
            super(task);
            this.writeType = writeType;
            this.consistencyLevel = consistencyLevel;
            this.acks = acks;
            this.blockedFor = blockedFor;
        }
    }

    public static class WriteFailureEvent extends WriteTaskEvent
    {
        public final WriteType writeType;
        public final ConsistencyLevel consistencyLevel;
        public final int acks;
        public final int blockedFor;
        public final int failures;

        public WriteFailureEvent(MutationTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor, int failures)
        {
            super(task);
            this.writeType = writeType;
            this.consistencyLevel = consistencyLevel;
            this.acks = acks;
            this.blockedFor = blockedFor;
            this.failures = failures;
        }
    }

    public static class WriteSuccessEvent extends WriteTaskEvent
    {
        public WriteSuccessEvent(MutationTask task)
        {
            super(task);
        }
    }

    public static class CommitlogSegmentAvailableEvent extends WriteTaskEvent
    {
        final CommitLogSegment segment;
        public CommitlogSegmentAvailableEvent(MutationTask task, CommitLogSegment segment)
        {
            super(task);
            this.segment = segment;
        }

        public static void createAndEmit(MutationTask task, CommitLogSegment segment)
        {
            task.writeTask.eventLoop.emitEvent(new CommitlogSegmentAvailableEvent(task, segment));
        }
    }

    public static class CommitlogSyncCompleteEvent extends WriteTaskEvent
    {
        CommitLogSegment.Allocation allocation;

        public CommitlogSyncCompleteEvent(MutationTask task, CommitLogSegment.Allocation allocation)
        {
            super(task);
            this.allocation = allocation;
        }

        public static void createAndEmit(MutationTask task, CommitLogSegment.Allocation allocation)
        {
            task.writeTask.eventLoop.emitEvent(new CommitlogSyncCompleteEvent(task, allocation));
        }
    }

    public class MutationTask
    {
        private final WriteTask writeTask;
        private final Mutation mutation;
        private AbstractWriteResponseHandler<IMutation> responseHandler;
        private int nowInSec;
        private int serializedSize;
        private OpOrder.Group opGroup;
        private ReplayPosition replayPosition;

        private Exception error;

        MutationTask(WriteTask writeTask, Mutation mutation)
        {
            this.writeTask = writeTask;
            this.mutation = mutation;
        }

        public WriteTask writeTask()
        {
            return writeTask;
        }

        public int getSerializedSize()
        {
            return serializedSize;
        }

        void applyToMemtable()
        {
            mutation.applyToMemtable(opGroup, replayPosition, nowInSec);
            AbstractWriteResponseHandler.AckResponse ackResponse = responseHandler.localResponse();
            if (ackResponse.complete)
            {
                if (ackResponse.exc != null)
                    error = ackResponse.exc;

                remaining--;
            }
        }
    }
}
