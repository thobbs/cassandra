package org.apache.cassandra.poc;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
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

    enum LocalWriteState {
        START,
        WAITING_FOR_COMMITLOG,
        WAITING_FOR_MEMTABLE,
        FAILED,
        COMPLETED
    }

    enum RemoteWriteState {
        START,
        WAITING_FOR_RESPONSES,
        FAILED,
        COMPLETED
    }

    private EventLoop eventLoop;
    private LocalWriteState localWriteState;
    private RemoteWriteState remoteWriteState;
    private long startTime;
    private Iterator<PartitionUpdate> updateIterator;
    private int nowInSec;
    private OpOrder.Group opGroup;
    private ReplayPosition replayPosition;
    private Mutation mutation;
    private int serializedSize;
    private CommitLogSegment.Allocation allocation;

    // in-flight partition update
    private PartitionUpdate currentUpdate;
    private MemtableApplyState memtableApplyState;

    // TODO where do these get set?
    public Collection<? extends IMutation> mutations = Collections.EMPTY_LIST;
    public ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    // TODO maybe not the best way to expose this
    public EventLoop eventLoop()
    {
        return this.eventLoop;
    }

    public void initialize(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        this.localWriteState = LocalWriteState.START;
        this.remoteWriteState = RemoteWriteState.START;


        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        startTime = System.nanoTime();

        assert mutations.size() == 1;  // TODO batches
        mutation = (Mutation) mutations.iterator().next();

        // TODO breakup multi-partition batch into chunks, yield and resume
        WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        // start StorageProxy.performWrite() behavior
        String keyspaceName = mutation.getKeyspaceName();
        AbstractReplicationStrategy replicationStrategy = Keyspace.open(keyspaceName).getReplicationStrategy();

        Token tk = mutation.key().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        AbstractWriteResponseHandler<IMutation> responseHandler = replicationStrategy.getWriteResponseHandler(
                naturalEndpoints, pendingEndpoints, consistencyLevel, null, wt, this);

        // TODO better err handling here
        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        Iterable<InetAddress> targets = Iterables.concat(naturalEndpoints, pendingEndpoints);

        // start StorageProxy.sendToHintedEndpoints() behavior

        // extra-datacenter replicas, grouped by dc
        Map<String, Collection<InetAddress>> dcGroups = null;
        // only need to create a Message for non-local writes
        MessageOut<Mutation> message = null;

        boolean insertLocal = false;
        boolean insertRemote = false;
        ArrayList<InetAddress> endpointsToHint = null;

        for (InetAddress destination : targets)
        {
            logger.warn("#### processing target {}", destination);
            // TODO check for hint overload
            // checkHintOverload(destination);

            if (FailureDetector.instance.isAlive(destination))
            {
                if (StorageProxy.canDoLocalRequest(destination))
                {
                    insertLocal = true;
                }
                else
                {
                    // belongs on a different server
                    insertRemote = true;

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

        if (insertRemote)
            remoteWriteState = RemoteWriteState.WAITING_FOR_RESPONSES;
        else
            remoteWriteState = RemoteWriteState.COMPLETED;

        // TODO submitting hints
        // if (endpointsToHint != null)
        //     submitHint(mutation, endpointsToHint, responseHandler);

        if (insertLocal)
        {
            logger.warn("#### starting local insert");
            try
            {
                // begin StorageProxy.performLocally() behavior
                nowInSec = FBUtilities.nowInSeconds();
                serializedSize = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);
                logger.warn("#### writing to commitlog");
                Pair<ReplayPosition, OpOrder.Group> pair = mutation.writeCommitlogAsync(this, serializedSize);
                logger.warn("#### done writing to commitlog");
                replayPosition = pair.left;
                opGroup = pair.right;

                if (replayPosition == null)
                {
                    // we are either waiting on commitlog allocation or syncing
                    localWriteState = LocalWriteState.WAITING_FOR_COMMITLOG;
                    logger.warn("#### waiting for commitlog");
                    return;
                }
            }
            catch (Exception exc)
            {
                logger.error("#### ", exc);
                throw exc;
            }

            logger.warn("#### going to apply to memtable");
            applyToMemtable();
            logger.warn("#### done applying to memtable");
        }
        else
        {
            localWriteState = LocalWriteState.COMPLETED;
        }

        // TODO non-local DC writes
        // if (dcGroups != null)
        // {
        //     // for each datacenter, send the message to one node to relay the write to other replicas
        //     if (message == null)
        //         message = mutation.createMessage();

        //     for (Collection<InetAddress> dcTargets : dcGroups.values())
        //         sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
        // }

        status = currentStatus();
    }

    private void applyToMemtable()
    {
        if (updateIterator == null)
            updateIterator = mutation.getPartitionUpdates().iterator();

        // TODO can potentially make progress on each of these individually if one is blocked
        while (updateIterator.hasNext())
        {
            currentUpdate = updateIterator.next();
            Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
            memtableApplyState = keyspace.applyPartitionUpdateToMemtableAsync(currentUpdate, opGroup, replayPosition, nowInSec, this);
            if (memtableApplyState != null)
            {
                // need to defer to event loop while we acquire lock on memtable partition
                logger.warn("#### waiting for memtable");
                localWriteState = LocalWriteState.WAITING_FOR_MEMTABLE;
                return;
            }
        }
        logger.warn("#### done applying to memtable");
        localWriteState = LocalWriteState.COMPLETED;
    }

    private Status currentStatus()
    {
        return (localWriteState == LocalWriteState.COMPLETED && remoteWriteState == RemoteWriteState.COMPLETED)
               ? Status.COMPLETED
               : Status.REGULAR;
    }

    public Status resume(EventLoop eventLoop)
    {
        throw new UnsupportedOperationException();
    }

    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        // TODO better organization for state transitions
        if (event instanceof WriteTimeoutEvent)
        {
            // TODO save error status for reporting
            StorageProxy.writeMetrics.timeouts.mark();
            remoteWriteState = RemoteWriteState.FAILED;
            return Status.COMPLETED;
        }
        else if (event instanceof WriteFailureEvent)
        {
            // TODO save error status for reporting
            StorageProxy.writeMetrics.failures.mark();
            remoteWriteState = RemoteWriteState.FAILED;
            return Status.COMPLETED;
        }
        else if (event instanceof WriteSuccessEvent)
        {
            // we've gotten acks on all writes
            StorageProxy.writeMetrics.addNano(System.nanoTime() - startTime);
            remoteWriteState = RemoteWriteState.COMPLETED;
            return currentStatus();
        }
        else if (event instanceof CommitlogSegmentAvailableEvent)
        {
            logger.warn("#### commitlog segment available event");
            allocation = CommitLog.instance.allocator.allocateAsync(
                    mutation, serializedSize, ((CommitlogSegmentAvailableEvent) event).segment, this);
            if (allocation == null)
                return Status.REGULAR;  // allocation failed, wait for another CommitlogSegmentAvailableEvent

            replayPosition = CommitLog.instance.writeToAllocationAndSync(mutation, allocation, serializedSize, this);
            if (replayPosition == null)
                return Status.REGULAR;  // commitlog sync would have blocked, wait for CommitlogSyncCompleteEvent

            applyToMemtable();
            return currentStatus();
        }
        else if (event instanceof CommitlogSyncCompleteEvent)
        {
            logger.warn("#### commitlog sync complete event");
            CommitLog.instance.executor.pending.decrementAndGet();
            logger.warn("#### getting replay position");
            allocation = ((CommitlogSyncCompleteEvent) event).allocation;
            replayPosition = allocation.getReplayPosition();
            logger.warn("#### going to apply to memtable");
            applyToMemtable();
            logger.warn("#### done applying to memtable");
            return currentStatus();
        }
        else if (event instanceof MemtablePartitionLockAcquiredEvent)
        {
            // TODO real non-blocking lock acquisition

            logger.warn("#### memtable partition lock acq event");

            Keyspace.open(mutation.getKeyspaceName()).resumeMemtableApply(
                    currentUpdate, memtableApplyState.startTime, memtableApplyState.memtable, memtableApplyState.partition,
                    opGroup, memtableApplyState.indexTransaction, this);
            applyToMemtable();  // continue with next partition update
            return currentStatus();
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
        if (opGroup != null)
            opGroup.close();
    }

    public static abstract class WriteTaskEvent implements Event
    {
        protected final WriteTask task;

        WriteTaskEvent(WriteTask task)
        {
            this.task = task;
        }

        public WriteTask task()
        {
            return task;
        }
    }

    public static class WriteTimeoutEvent extends WriteTaskEvent
    {
        public final WriteType writeType;
        public final ConsistencyLevel consistencyLevel;
        public final int acks;
        public final int blockedFor;

        public WriteTimeoutEvent(WriteTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor)
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

        public WriteFailureEvent(WriteTask task, WriteType writeType, ConsistencyLevel consistencyLevel, int acks, int blockedFor, int failures)
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
        public WriteSuccessEvent(WriteTask task)
        {
            super(task);
        }
    }

    public static class CommitlogSegmentAvailableEvent extends WriteTaskEvent
    {
        final CommitLogSegment segment;
        public CommitlogSegmentAvailableEvent(WriteTask task, CommitLogSegment segment)
        {
            super(task);
            this.segment = segment;
        }

        public static void createAndEmit(WriteTask task, CommitLogSegment segment)
        {
            task.eventLoop.emitEvent(new CommitlogSegmentAvailableEvent(task, segment));
        }
    }

    public static class CommitlogSyncCompleteEvent extends WriteTaskEvent
    {
        CommitLogSegment.Allocation allocation;

        public CommitlogSyncCompleteEvent(WriteTask task, CommitLogSegment.Allocation allocation)
        {
            super(task);
            this.allocation = allocation;
        }

        public static void createAndEmit(WriteTask task, CommitLogSegment.Allocation allocation)
        {
            task.eventLoop.emitEvent(new CommitlogSyncCompleteEvent(task, allocation));
        }
    }

    public static class MemtablePartitionLockAcquiredEvent extends WriteTaskEvent
    {
        final AtomicBTreePartition partition;

        public MemtablePartitionLockAcquiredEvent(WriteTask task, AtomicBTreePartition partition)
        {
            super(task);
            this.partition = partition;
        }

        public static void createAndEmit(WriteTask task, AtomicBTreePartition partition)
        {
            task.eventLoop.emitEvent(new MemtablePartitionLockAcquiredEvent(task, partition));
        }
    }

    public static class MemtableApplyState
    {
        final Memtable memtable;
        final AtomicBTreePartition partition;
        final long startTime;
        final long initialSize;

        UpdateTransaction indexTransaction;

        public MemtableApplyState(Memtable memtable, AtomicBTreePartition partition, long startTime, long initialSize)
        {
            this.memtable = memtable;
            this.partition = partition;
            this.startTime = startTime;
            this.initialSize = initialSize;
        }

        public void setIndexTransaction(UpdateTransaction indexTransaction)
        {
            this.indexTransaction = indexTransaction;
        }
    }
}
