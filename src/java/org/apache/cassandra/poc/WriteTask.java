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

    private final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

    public final Collection<? extends IMutation> mutations;
    public final ConsistencyLevel consistencyLevel;
    private final Iterator<? extends IMutation> mutationIterator;
    private final ArrayList<MutationTask> mutationTasks;
    private final WriteType writeType;
    private final boolean localOnly;
    private final Task parentTask;

    private EventLoop eventLoop;
    private Exception error;
    private int remaining;
    private long startTime;

    public WriteTask(Collection<? extends IMutation> mutations, ConsistencyLevel consistencyLevel)
    {
        this.mutations = mutations;
        this.consistencyLevel = consistencyLevel;

        this.mutationIterator = mutations.iterator();
        mutationTasks = new ArrayList<>(mutations.size());
        remaining = mutations.size();
        writeType = remaining <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        localOnly = false;
        parentTask = null;
    }

    /**
     * Used only for local mutations
     */
    public WriteTask(Collection<? extends IMutation> mutations, Task parentTask)
    {
        this.mutations = mutations;
        this.consistencyLevel = null;

        this.mutationIterator = mutations.iterator();
        mutationTasks = new ArrayList<>(mutations.size());
        remaining = mutations.size();
        writeType = remaining <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        localOnly = true;
        this.parentTask = parentTask;
    }

    public EventLoop eventLoop()
    {
        return this.eventLoop;
    }

    public Status initialize(EventLoop eventLoop)
    {
        startTime = System.nanoTime();

        this.eventLoop = eventLoop;
        status = processMutations();
        return status;
    }

    private Status processMutations()
    {
        int chunkCount = 0;
        while (mutationIterator.hasNext() && chunkCount < CHUNK_SIZE)
        {
            Mutation mutation = (Mutation) mutationIterator.next();
            chunkCount++;

            // start StorageProxy.performWrite() behavior
            MutationTask mutationTask = new MutationTask(this, mutation);
            mutationTasks.add(mutationTask);

            boolean insertLocal = localOnly;

            if (!localOnly)
            {
                AbstractWriteResponseHandler<IMutation> responseHandler = makeResponseHandler(mutation, mutationTask);
                mutationTask.responseHandler = responseHandler;

                // exit early if we can't fulfill the CL at this time
                if (checkForUnavailable(responseHandler))
                    return Status.COMPLETED;

                Iterable<InetAddress> targets = responseHandler.getTargetedEndpoints();

                // extra-datacenter replicas, grouped by dc
                Map<String, Collection<InetAddress>> dcGroups = new HashMap<>();
                // only need to create a Message for non-local writes
                MessageOut<Mutation> message = null;

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
                            if (message == null)
                                message = mutation.createMessage();

                            handleRemoteReplica(destination, message, responseHandler, dcGroups);
                        }
                    }
                    else if (StorageProxy.shouldHint(destination))
                    {
                        if (endpointsToHint == null)
                            endpointsToHint = new ArrayList<>(Iterables.size(targets));
                        endpointsToHint.add(destination);
                    }
                }

                // TODO hint submission is still performed in a separate stage
                if (endpointsToHint != null)
                    StorageProxy.submitHint(mutation, endpointsToHint, responseHandler);

                // write to remote DCs
                if (!dcGroups.isEmpty())
                {
                    // for each datacenter, send the message to one node to relay the write to other replicas
                    if (message == null)
                        message = mutation.createMessage();

                    for (Collection<InetAddress> dcTargets : dcGroups.values())
                        StorageProxy.sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
                }
            }

            if (insertLocal)
                performLocalWrite(mutationTask, mutation);
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

    private void handleRemoteReplica(InetAddress destination, MessageOut<Mutation> message,
                                     AbstractWriteResponseHandler<IMutation> responseHandler,
                                     Map<String, Collection<InetAddress>> dcGroups)
    {
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
                dcGroups.put(dc, messages);
            }
            messages.add(destination);
        }
    }

    private void performLocalWrite(MutationTask mutationTask, Mutation mutation)
    {
        mutationTask.nowInSec = FBUtilities.nowInSeconds();
        mutationTask.serializedSize = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

        Pair<ReplayPosition, OpOrder.Group> pair = mutation.writeCommitlogAsync(mutationTask);
        mutationTask.replayPosition = pair.left;
        mutationTask.opGroup = pair.right;

        if (mutationTask.replayPosition == null)
            return; // we are either waiting on commitlog allocation or syncing

        mutationTask.applyToMemtable();
        if (remaining == 0)
        {
            status = Status.COMPLETED;
            maybeNotifyParentTask();
        }
    }

    private void maybeNotifyParentTask()
    {
        if (parentTask != null)
            parentTask.dispatchEvent(this.eventLoop, new LocalWriteCompleteEvent(this, parentTask, error));
    }

    private AbstractWriteResponseHandler<IMutation> makeResponseHandler(Mutation mutation, MutationTask mutationTask)
    {
        String keyspaceName = mutation.getKeyspaceName();
        AbstractReplicationStrategy replicationStrategy = Keyspace.open(keyspaceName).getReplicationStrategy();

        Token tk = mutation.key().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        return replicationStrategy.getWriteResponseHandler(
                naturalEndpoints, pendingEndpoints, consistencyLevel, null, writeType, mutationTask);
    }

    public Status resume(EventLoop eventLoop)
    {
        return processMutations();
    }

    public Status handleEvent(EventLoop eventLoop, Event event)
    {
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
            error = exc;
            StorageProxy.writeMetrics.timeouts.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof WriteFailureEvent)
        {
            WriteFailureEvent wfe = (WriteFailureEvent) event;
            WriteFailureException exc = new WriteFailureException(wfe.consistencyLevel, wfe.acks, wfe.failures, wfe.blockedFor, wfe.writeType);
            error = exc;
            StorageProxy.writeMetrics.failures.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof CommitlogSegmentAvailableEvent)
        {
            MutationTask mutTask = (MutationTask) ((CommitlogSegmentAvailableEvent) event).task;
            int totalSize = mutTask.serializedSize + CommitLogSegment.ENTRY_OVERHEAD_SIZE;
            CommitLogSegment.Allocation allocation = CommitLog.instance.allocator.allocateAsync(
                    mutTask.mutation, totalSize, ((CommitlogSegmentAvailableEvent) event).segment, mutTask);
            if (allocation == null)
                return Status.REGULAR;  // allocation failed, wait for another CommitlogSegmentAvailableEvent

            mutTask.replayPosition = CommitLog.instance.writeToAllocationAndSync(mutTask.mutation, allocation, mutTask);
            if (mutTask.replayPosition == null)
                return Status.REGULAR;  // commitlog sync would have blocked, wait for CommitlogSyncCompleteEvent

            mutTask.applyToMemtable();
            if (remaining == 0)
                maybeNotifyParentTask();
            return remaining == 0 ? Status.COMPLETED : Status.REGULAR;
        }
        else if (event instanceof CommitlogSyncCompleteEvent)
        {
            MutationTask mutTask = (MutationTask) ((CommitlogSyncCompleteEvent) event).task;
            CommitLog.instance.executor.pending.decrementAndGet();
            CommitLogSegment.Allocation allocation = ((CommitlogSyncCompleteEvent) event).allocation;
            mutTask.replayPosition = allocation.getReplayPosition();
            mutTask.applyToMemtable();
            if (remaining == 0)
                maybeNotifyParentTask();
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
        for (MutationTask mutationTask : mutationTasks)
        {
            if (mutationTask.opGroup != null)
                mutationTask.opGroup.close();
        }
    }

    public static abstract class WriteTaskEvent implements Event
    {
        protected final SubTask task;

        WriteTaskEvent(SubTask task)
        {
            this.task = task;
        }

        public Task task()
        {
            return task.getTask();
        }
    }

    public static class WriteTimeoutEvent extends WriteTaskEvent
    {
        public final WriteType writeType;
        public final ConsistencyLevel consistencyLevel;
        public final int acks;
        public final int blockedFor;

        public WriteTimeoutEvent(SubTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor)
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

        public WriteFailureEvent(SubTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor, int failures)
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
        public WriteSuccessEvent(SubTask task)
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

    public static class LocalWriteCompleteEvent implements Event
    {
        public final WriteTask writeTask;
        public final Task parentTask;
        public final Exception error;

        public LocalWriteCompleteEvent(WriteTask writeTask, Task parentTask, Exception error)
        {
            this.writeTask = writeTask;
            this.parentTask = parentTask;
            this.error = error;
        }

        public Task task()
        {
            return parentTask;
        }

        public boolean didFail()
        {
            return error != null;
        }
    }

    public interface SubTask
    {
        Task getTask();
    }

    public class MutationTask implements SubTask
    {
        private final WriteTask writeTask;
        private final Mutation mutation;
        private AbstractWriteResponseHandler<IMutation> responseHandler;
        private int nowInSec;
        private int serializedSize;
        private OpOrder.Group opGroup;
        private ReplayPosition replayPosition;

        MutationTask(WriteTask writeTask, Mutation mutation)
        {
            this.writeTask = writeTask;
            this.mutation = mutation;
        }

        public WriteTask getTask()
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
