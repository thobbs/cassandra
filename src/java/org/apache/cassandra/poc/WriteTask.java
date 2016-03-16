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
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.index.transactions.UpdateTransaction;
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

    private EventLoop eventLoop;

    private long startTime;
    private int nowInSec;
    private AbstractWriteResponseHandler<IMutation> responseHandler;
    private OpOrder.Group opGroup;
    private ReplayPosition replayPosition;
    private Mutation mutation;
    private int serializedSize;

    private Exception error;

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

        responseHandler = replicationStrategy.getWriteResponseHandler(
                naturalEndpoints, pendingEndpoints, consistencyLevel, null, wt, this);

        // exit early if we can't fulfill the CL at this time
        if (checkForUnavailable())
            return;

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
            logger.warn("#### processing target {}", destination);
            if (checkForHintOverload(destination))
                return;

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

        Status mutStatus = Status.REGULAR;

        // write locally
        if (insertLocal)
        {
            // begin StorageProxy.performLocally() behavior
            nowInSec = FBUtilities.nowInSeconds();
            serializedSize = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);
            Pair<ReplayPosition, OpOrder.Group> pair = mutation.writeCommitlogAsync(this, serializedSize);
            replayPosition = pair.left;
            opGroup = pair.right;

            if (replayPosition == null)
            {
                // we are either waiting on commitlog allocation or syncing
                return;
            }

            mutStatus = applyToMemtable();
        }

        // write to remote DCs
        if (dcGroups != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            if (message == null)
                message = mutation.createMessage();

            for (Collection<InetAddress> dcTargets : dcGroups.values())
                StorageProxy.sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
        }

        status = mutStatus;
    }

    private boolean checkForUnavailable()
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

    private Status applyToMemtable()
    {
        mutation.applyToMemtable(opGroup, replayPosition, nowInSec);
        AbstractWriteResponseHandler.AckResponse ackResponse = responseHandler.localResponse();
        if (ackResponse.complete)
        {
            if (ackResponse.exc != null)
                error = ackResponse.exc;
            return Status.COMPLETED;
        }

        return Status.REGULAR;
    }

    public Status resume(EventLoop eventLoop)
    {
        throw new UnsupportedOperationException();
    }

    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        // TODO better organization of state transitions

        if (event instanceof WriteSuccessEvent)
        {
            // we've gotten acks on all writes
            StorageProxy.writeMetrics.addNano(System.nanoTime() - startTime);
            return Status.COMPLETED;
        }
        else if (event instanceof WriteTimeoutEvent)
        {
            WriteTimeoutEvent wte = (WriteTimeoutEvent) event;
            error = new WriteTimeoutException(wte.writeType, wte.consistencyLevel, wte.acks, wte.blockedFor);
            StorageProxy.writeMetrics.timeouts.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof WriteFailureEvent)
        {
            WriteFailureEvent wfe = (WriteFailureEvent) event;
            error = new WriteFailureException(wfe.consistencyLevel, wfe.acks, wfe.failures, wfe.blockedFor, wfe.writeType);
            StorageProxy.writeMetrics.failures.mark();
            return Status.COMPLETED;
        }
        else if (event instanceof CommitlogSegmentAvailableEvent)
        {
            logger.warn("#### commitlog segment available event");
            CommitLogSegment.Allocation allocation = CommitLog.instance.allocator.allocateAsync(
                    mutation, serializedSize, ((CommitlogSegmentAvailableEvent) event).segment, this);
            if (allocation == null)
                return Status.REGULAR;  // allocation failed, wait for another CommitlogSegmentAvailableEvent

            replayPosition = CommitLog.instance.writeToAllocationAndSync(mutation, allocation, serializedSize, this);
            if (replayPosition == null)
                return Status.REGULAR;  // commitlog sync would have blocked, wait for CommitlogSyncCompleteEvent

            return applyToMemtable();
        }
        else if (event instanceof CommitlogSyncCompleteEvent)
        {
            logger.warn("#### commitlog sync complete event");
            CommitLog.instance.executor.pending.decrementAndGet();
            CommitLogSegment.Allocation allocation = ((CommitlogSyncCompleteEvent) event).allocation;
            replayPosition = allocation.getReplayPosition();
            return applyToMemtable();
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
}
