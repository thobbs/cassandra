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
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel.Timer;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A task for executing one or more mutations.
 *
 * The state machine for the local write path looks like this:
 *
 * [start] --> [segment allocated] --> [segment synced, memtable written]
 *
 * Both of the transitions may require yielding to the event loop and waiting for an event (CommitlogSegmentAvailableEvent
 * and CommitlogSyncCompleteEvent, respectively).  However, it is normal for both transitions to be made immediately
 * without yielding to the event loop (when a commitlog segment is available and periodic sync mode is in use).
 *
 * A normal WriteResponseHandler is used for managing responses from individual replicas (including the coordinator, if
 * it's a replica).  Once the WriteResponseHandler has enough successful responses to meet the consistency level,
 * a WriteSuccessEvent is emitted to complete the write.  Note that this step is also optimized for CL.(LOCAL_)ONE
 * requests, allowing most token-aware writes at ONE to complete in a single cycle of the event loop with no events
 * emitted.
 *
 * When handling multi-mutation batches, this task voluntarily defers to the event loop after processing a chunk of 64
 * events. This allows other tasks (and individual mutations within the batch) to make progress while a large batch
 * is handled.
 */
public class WriteTask extends Task<Message.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

    // for batch mutations, this is the number of mutations to process in one chunk before yielding to the event loop
    private static final int CHUNK_SIZE = 64;

    private final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

    public final Collection<? extends IMutation> mutations;
    public final ConsistencyLevel consistencyLevel;
    private final Iterator<? extends IMutation> mutationIterator;
    private final ArrayList<MutationTask> mutationTasks;
    private final WriteType writeType;
    private final boolean localOnly;
    private final String keyspace;

    // the number of mutations left to process
    private int remaining;

    // the time in nanoseconds when the write operation was started
    private long startTime;
    private Timer requestTimer;

    public WriteTask(Collection<? extends IMutation> mutations, ConsistencyLevel consistencyLevel)
    {
        this.mutations = mutations;
        this.consistencyLevel = consistencyLevel;

        mutationIterator = mutations.iterator();
        mutationTasks = new ArrayList<>(mutations.size());
        remaining = mutations.size();
        keyspace = mutations.iterator().next().getKeyspaceName();
        writeType = remaining <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        localOnly = false;
    }

    /**
     * Used only for local mutations executed as part of the work of a parent task (e.g. PaxosWriteTask)
     */
    WriteTask(Collection<? extends IMutation> mutations)
    {
        this.mutations = mutations;
        this.consistencyLevel = null;

        this.mutationIterator = mutations.iterator();
        mutationTasks = new ArrayList<>(mutations.size());
        remaining = mutations.size();
        keyspace = mutations.iterator().next().getKeyspaceName();
        writeType = remaining <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        localOnly = true;
    }

    @Override
    public Status start(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;

        startTime = System.nanoTime();
        requestTimer = eventLoop.scheduleTimer(this, DatabaseDescriptor.getWriteRpcTimeout() , TimeUnit.MILLISECONDS);

        return processMutations();
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
                    return Status.FAILED;

                Iterable<InetAddress> targets = responseHandler.getTargetedEndpoints();

                // extra-datacenter replicas, grouped by dc
                Map<String, Collection<InetAddress>> dcGroups = new HashMap<>();
                // only need to create a Message for non-local writes
                MessageOut<Mutation> message = null;

                ArrayList<InetAddress> endpointsToHint = null;

                for (InetAddress destination : targets)
                {
                    if (checkForHintOverload(destination))
                        return Status.FAILED;

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
            {
                boolean didError = performLocalWrite(mutationTask, mutation);
                if (didError)
                    return Status.FAILED;
            }
        }

        if (mutationIterator.hasNext())
            return Status.RESCHEDULED;

        return Status.PROCESSING;
    }

    private boolean checkForUnavailable(AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        try
        {
            responseHandler.assureSufficientLiveNodes();
        }
        catch (UnavailableException exc)
        {
            fail(exc);
            return true;
        }

        return false;
    }

    private boolean checkForHintOverload(InetAddress destination)
    {
        if (StorageProxy.hintsAreOverloaded(destination))
        {
            fail(new OverloadedException(
                    "Too many in flight hints: " + StorageMetrics.totalHintsInProgress.getCount() +
                            " destination: " + destination +
                            " destination hints: " + StorageProxy.getHintsInProgressFor(destination).get()));
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
            Collection<InetAddress> messages = dcGroups.get(dc);
            if (messages == null)
            {
                messages = new ArrayList<>(3); // most DCs will have <= 3 replicas
                dcGroups.put(dc, messages);
            }
            messages.add(destination);
        }
    }

    /** Returns true if there was an error performing the local write, false otherwise. */
    private boolean performLocalWrite(MutationTask mutationTask, Mutation mutation)
    {
        mutationTask.nowInSec = FBUtilities.nowInSeconds();
        mutationTask.serializedSize = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        boolean durableWrites = keyspace.getMetadata().params.durableWrites;

        try
        {
            Pair<ReplayPosition, OpOrder.Group> pair = keyspace.writeCommitlogAsync(mutation, durableWrites, true, mutationTask);
            mutationTask.replayPosition = pair.left;
            mutationTask.opGroup = pair.right;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Error writing mutation to commit log: ", t);
            fail(t);
            return true;
        }

        if (mutationTask.replayPosition == null && durableWrites)
            return false; // we are either waiting on commitlog allocation or syncing

        mutationTask.applyToMemtable();
        if (hasFailed())
            return true;

        if (remaining == 0)
            doComplete();

        return false;
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

    private Status doComplete()
    {
        requestTimer.cancel();
        return complete(new ResultMessage.Void());
    }

    @Override
    public Status resume(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        return processMutations();
    }

    @Override
    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        if (event instanceof WriteSuccessEvent)
        {
            // we've gotten acks on all writes
            StorageProxy.writeMetrics.addNano(System.nanoTime() - startTime);
            remaining--;

            return remaining == 0 ? doComplete() : Status.PROCESSING;
        }
        else if (event instanceof WriteTimeoutEvent)
        {
            WriteTimeoutEvent wte = (WriteTimeoutEvent) event;
            StorageProxy.writeMetrics.timeouts.mark();
            return fail(new WriteTimeoutException(wte.writeType, wte.consistencyLevel, wte.acks, wte.blockedFor));
        }
        else if (event instanceof WriteFailureEvent)
        {
            WriteFailureEvent wfe = (WriteFailureEvent) event;
            StorageProxy.writeMetrics.failures.mark();
            return fail(new WriteFailureException(wfe.consistencyLevel, wfe.acks, wfe.failures, wfe.blockedFor, wfe.writeType));
        }
        else if (event instanceof CommitlogSegmentAvailableEvent)
        {
            return handleCommitlogSegmentAvailable((CommitlogSegmentAvailableEvent) event);
        }
        else if (event instanceof CommitlogSyncCompleteEvent)
        {
            return handleCommitlogSyncComplete((CommitlogSyncCompleteEvent) event);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    private Status handleCommitlogSegmentAvailable(CommitlogSegmentAvailableEvent event)
    {
        MutationTask mutTask = (MutationTask) event.task;
        int totalSize = mutTask.serializedSize + CommitLogSegment.ENTRY_OVERHEAD_SIZE;
        CommitLogSegment.Allocation allocation = CommitLog.instance.allocator.allocateAsync(
                mutTask.mutation, totalSize, event.segment, mutTask);
        if (allocation == null)
            return Status.PROCESSING;  // allocation failed, wait for another CommitlogSegmentAvailableEvent

        try
        {
            mutTask.replayPosition = CommitLog.instance.writeToAllocationAndSync(mutTask.mutation, allocation, mutTask);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Error writing mutation to commitlog allocation: ", t);
            return fail(t);
        }

        if (mutTask.replayPosition == null)
            return Status.PROCESSING;  // commitlog sync would have blocked, wait for CommitlogSyncCompleteEvent

        mutTask.applyToMemtable();
        if (hasFailed())
            return status();

        if (remaining == 0)
            return doComplete();

        return Status.PROCESSING;
    }

    private Status handleCommitlogSyncComplete(CommitlogSyncCompleteEvent event)
    {
        MutationTask mutTask = (MutationTask) event.task;
        CommitLogSegment.Allocation allocation = event.allocation;
        mutTask.replayPosition = allocation.getReplayPosition();

        mutTask.applyToMemtable();
        if (hasFailed())
            return status();

        if (remaining == 0)
            return doComplete();

        return Status.PROCESSING;
    }

    @Override
    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        // there's not really a reasonable way to determine the number of "responses" that we have for a batch
        // mutation, so we use zero in that case
        int responses = (mutations.size() == 1) ? mutationTasks.get(0).responseHandler.ackCount() : 0;
        int blockFor = consistencyLevel.blockFor(Keyspace.open(this.keyspace));
        return fail(new WriteTimeoutException(writeType, consistencyLevel, responses, blockFor));
    }

    public void cleanup(EventLoop eventLoop)
    {
        mutationTasks.stream()
                .filter(mt -> mt.opGroup != null)
                .forEach(mt -> mt.opGroup.close());
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
            // for now, we always assume "updateIndexes" is true
            mutation.applyToMemtable(opGroup, replayPosition, nowInSec, true);
            if (localOnly)
            {
                remaining--;
            }
            else
            {
                AbstractWriteResponseHandler.AckResponse ackResponse = responseHandler.localResponse();
                if (ackResponse.complete)
                {
                    if (ackResponse.exc != null)
                        fail(ackResponse.exc);

                    remaining--;
                }
            }
        }
    }
}
