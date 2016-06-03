package org.apache.cassandra.poc;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.poc.events.Event;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.TimerWheel.Timer;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A task for executing a CAS operation using Paxos.
 *
 * The basic state machine for Paxos looks like this:
 *
 * [start] --> [prepared] --> [proposed] --> [committed]
 *
 * However, there are several complications that may require returning to an earlier state:
 *  * Contention (return to [start])
 *  * Incomplete rounds (return to [prepared] for the incomplete round, then return to [start] for this commit)
 *  * Replicas missing the most recent commit (return to [start])
 *
 * Additionally, we also need to handle:
 *  * A local read to check the precondition
 *  * A local write of the commit itself
 *  * A local write to save the commit to a system table
 *
 * These are accomplished with new WriteTasks and ReadTasks which have their own state machine.
 */
public class PaxosWriteTask extends Task<Message.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

    private final String keyspaceName;
    private final String tableName;
    private final DecoratedKey key;
    private final CASRequest request;
    private final ConsistencyLevel consistencyForPaxos;
    private final ConsistencyLevel consistencyForCommit;
    private final ClientState state;
    private final CFMetaData cfm;

    private long startTime;
    private int contentions = 0;
    private Timer casContentionTimer;
    private Timer commitWriteTimer;

    private List<InetAddress> liveEndpoints;
    private int requiredParticipants;
    private PrepareResponseHandler prepareResponseHandler = null;

    private UUID ballot;

    private long localCommitStartNanos;
    private Commit localProposal;
    AbstractWriteResponseHandler<Commit> commitResponseHandler = null;
    boolean wasPreempted = false;

    public PaxosWriteTask(String keyspaceName, String tableName, DecoratedKey key, CASRequest request,
                          ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit, ClientState state)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.key = key;
        this.request = request;
        this.consistencyForPaxos = consistencyForPaxos;
        this.consistencyForCommit = consistencyForCommit;
        this.state = state;

        cfm = Schema.instance.getCFMetaData(keyspaceName, tableName);
    }

    @Override
    public Status start(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;

        startTime = System.nanoTime();
        casContentionTimer = eventLoop.scheduleTimer(this, DatabaseDescriptor.getCasContentionTimeout(), TimeUnit.MILLISECONDS);

        contentions = 0;
        consistencyForPaxos.validateForCas();
        consistencyForCommit.validateForCasCommit(keyspaceName);

        return tryProcessMutations();
    }

    /**
     * Starts a Paxos round. This is rerun if we are preempted by a higher ballot
     **/
    private Status tryProcessMutations()
    {
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, tableName);
        try
        {
            Pair<List<InetAddress>, Integer> p = StorageProxy.getPaxosParticipants(metadata, key, consistencyForPaxos);
            liveEndpoints = p.left;
            requiredParticipants = p.right;
        }
        catch (UnavailableException uae)
        {
            return fail(uae);
        }

        beginAndRepairPaxos();
        return Status.PROCESSING;
    }

    /**
     *  This is rerun if our prepare is rejected due to higher ballots (i.e. contention).  It is also rerun after
     *  finishing a commit of a previously unfinished round.
     */
    private Status beginAndRepairPaxos()
    {
        // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
        // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
        // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
        // out-of-order (#7801).
        long minTimestampMicrosToUse = prepareResponseHandler == null
                                     ? Long.MIN_VALUE
                                     : 1 + UUIDGen.microsTimestamp(prepareResponseHandler.mostRecentInProgressCommit.ballot);
        long ballotMicros = state.getTimestamp(minTimestampMicrosToUse);
        UUID ballot = UUIDGen.getTimeUUIDFromMicros(ballotMicros);

        // prepare
        Tracing.trace("Preparing {}", ballot);
        Commit toPrepare = Commit.newPrepare(key, cfm, ballot);

        preparePaxos(toPrepare, ballot);
        return Status.PROCESSING;
    }

    private Status handlePrepareResponse(PrepareResponseHandler responseHandler)
    {
        prepareResponseHandler = responseHandler;
        if (!responseHandler.promised)
        {
            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
            contentions++;

            // sleep a random amount of time to give the other proposer a chance to finish
            eventLoop.scheduleTask(this, ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
            return Status.PROCESSING;
        }

        Commit inProgress = responseHandler.mostRecentInProgressCommitWithUpdate;
        Commit mostRecent = responseHandler.mostRecentCommit;

        // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
        // needs to be completed, so do it.
        if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
        {
            Tracing.trace("Finishing incomplete paxos round {}", inProgress);
            StorageProxy.casWriteMetrics.unfinishedCommit.inc();
            Commit refreshedInProgress = Commit.newProposal(responseHandler.ballot, inProgress.update);
            proposePaxos(refreshedInProgress, false, true);
            return Status.PROCESSING;
        }
        else
        {
            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            List<InetAddress> missingMRC = responseHandler.replicasMissingMostRecentCommit();
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                StorageProxy.sendCommit(mostRecent, missingMRC);  // this does not wait for replicas
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                return beginAndRepairPaxos();
            }

            ballot = responseHandler.ballot;
            return readExistingValuesAndApplyUpdates();
        }
    }

    private Status handleProposeResponse(ProposeResponseHandler responseHandler)
    {
        if (responseHandler.isSuccessful())
            return commitPaxos(responseHandler.proposal, consistencyForCommit, false, responseHandler.isForRepair());

        // unsuccessful
        if (!responseHandler.failFast && !responseHandler.isFullyRefused())
        {
            return fail(new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, responseHandler.getAcceptCount(), requiredParticipants));
        }
        else if (responseHandler.proposal != null)
        {
            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
            contentions++;

            // sleep a random amount of time to give the other proposer a chance to finish
            eventLoop.scheduleTask(this, ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
            return Status.RESCHEDULED;
        }
        else
        {
            Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
            contentions++;

            // restart from the beginning, deal with timeouts
            wasPreempted = true;
            eventLoop.scheduleTask(this, ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
            return Status.RESCHEDULED;
        }
    }

    /** Called after we get a prepare response */
    private Status readExistingValuesAndApplyUpdates()
    {
        // read the current values and check they validate the conditions
        Tracing.trace("Reading existing values for CAS precondition");
        SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
        ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;

        FilteredPartition current;
        // TODO this still uses a blocking read, normally we would have another async break here
        try (RowIterator rowIter = StorageProxy.readOne(readCommand, readConsistency))
        {
            current = FilteredPartition.create(rowIter);
        }

        if (!request.appliesTo(current))
        {
            Tracing.trace("CAS precondition does not match current values {}", current);
            StorageProxy.casWriteMetrics.conditionNotMet.inc();
            return doComplete(current.rowIterator());
        }

        // finish the paxos round w/ the desired updates
        PartitionUpdate updates = request.makeUpdates(current);

        // Apply triggers to cas updates. A consideration here is that
        // triggers emit Mutations, and so a given trigger implementation
        // may generate mutations for partitions other than the one this
        // paxos round is scoped for. In this case, TriggerExecutor will
        // validate that the generated mutations are targetted at the same
        // partition as the initial updates and reject (via an
        // InvalidRequestException) any which aren't.
        updates = TriggerExecutor.instance.execute(updates);

        Commit proposal = Commit.newProposal(ballot, updates);
        Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);

        proposePaxos(proposal, true, false);
        return Status.PROCESSING;
    }

    private void preparePaxos(Commit toPrepare, UUID ballot)
            throws WriteTimeoutException
    {
        PrepareResponseHandler callback = new PrepareResponseHandler(ballot, key, cfm);
        MessageOut<Commit> message = new MessageOut<>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (InetAddress target : liveEndpoints)
            MessagingService.instance().sendRR(message, target, callback);
    }

    private void proposePaxos(Commit proposal, boolean timeoutIfPartial, boolean forRepair)
    throws WriteTimeoutException
    {
        ProposeResponseHandler callback = new ProposeResponseHandler(liveEndpoints.size(), requiredParticipants, !timeoutIfPartial,
                consistencyForPaxos, proposal, forRepair);

        MessageOut<Commit> message = new MessageOut<>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (InetAddress target : liveEndpoints)
            MessagingService.instance().sendRR(message, target, callback);
    }

    private Status commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint, boolean forRepair) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

        Token tk = proposal.update.partitionKey().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            CommitSubTask subTask = new CommitSubTask(this, forRepair);
            commitResponseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE, subTask);
            commitWriteTimer = eventLoop.scheduleTimer(this, DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS);
        }

        MessageOut<Commit> message = new MessageOut<>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (checkForHintOverload(destination))
                return Status.COMPLETED;  // finalException will already be set

            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                {
                    if (StorageProxy.canDoLocalRequest(destination))
                        commitPaxosLocal(proposal, forRepair);
                    else
                        MessagingService.instance().sendRR(message, destination, commitResponseHandler, shouldHint);
                }
                else
                {
                    MessagingService.instance().sendOneWay(message, destination);
                }
            }
            else if (shouldHint)
            {
                StorageProxy.submitHint(proposal.makeMutation(), destination, null);
            }
        }

        return (forRepair || shouldBlock) ? Status.PROCESSING : doComplete(null);
    }

    /**
     * Commits a proposal locally.
     * @param proposal the proposal to commit
     * @param forRepair true if we are repairing an unfinished round (and thus have more work to do), false otherwise
     */
    private void commitPaxosLocal(Commit proposal, boolean forRepair)
    {
        localCommitStartNanos = System.nanoTime();
        // There is no guarantee we will see commits in the right order, because messages
        // can get delayed, so a proposal can be older than our current most recent ballot/commit.
        // Committing it is however always safe due to column timestamps, so always do it. However,
        // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
        // erase the in-progress update.
        // The table may have been truncated since the proposal was initiated. In that case, we
        // don't want to perform the mutation and potentially resurrect truncated data
        if (UUIDGen.unixTimestamp(proposal.ballot) >= SystemKeyspace.getTruncatedAt(proposal.update.metadata().cfId))
        {
            Tracing.trace("Committing proposal {}", proposal);
            Mutation mutation = proposal.makeMutation();
            WriteTask localWriteTask = new CommitWriteTask(Collections.singleton(mutation), forRepair);
            localProposal = proposal;
            this.eventLoop.scheduleTask(localWriteTask);
        }
        else
        {
            Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", proposal);
        }
    }

    /** Called when our local commit of a proposal completes */
    private Status handleLocalPaxosCommitCompleted()
    {
        // Update the system.paxos table
        // We don't need to lock, we're just blindly updating
        Collection<? extends IMutation> mutations = SystemKeyspace.getSavePaxosCommitMutations(localProposal);
        WriteTask saveCommitTask = new SavePaxosCommitWriteTask(mutations);
        this.eventLoop.scheduleTask(saveCommitTask);
        return Status.PROCESSING;
    }

    private void updateCasCommitStats()
    {
        ColumnFamilyStore cfs = Keyspace.open(localProposal.update.metadata().ksName)
                .getColumnFamilyStore(localProposal.update.metadata().cfId);
        cfs.metric.casCommit.addNano(System.nanoTime() - localCommitStartNanos);
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

    private void recordFinalMetrics()
    {
        if(contentions > 0)
            StorageProxy.casWriteMetrics.contention.update(contentions);

        StorageProxy.casWriteMetrics.addNano(System.nanoTime() - startTime);
    }

    /** Handler for responses for a PROPOSE step */
    private class ProposeResponseHandler extends AbstractPaxosCallback<Boolean>
    {
        private final AtomicInteger accepts = new AtomicInteger(0);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final AtomicBoolean haveEmittedEvent = new AtomicBoolean(false);
        private final Commit proposal;
        private final int targets;
        private final int requiredAccepts;
        private final boolean failFast;
        private final boolean forRepair;

        public ProposeResponseHandler(int totalTargets, int requiredTargets, boolean failFast, ConsistencyLevel consistency,
                                      Commit refreshedInProgress, boolean forRepair)
        {
            super(totalTargets, consistency);
            this.targets = totalTargets;
            this.requiredAccepts = requiredTargets;
            this.failFast = failFast;
            this.proposal = refreshedInProgress;
            this.forRepair = forRepair;
        }

        public boolean isForRepair()
        {
            return forRepair;
        }

        /** Called by MessagingService when we have a response */
        public void response(MessageIn<Boolean> msg, int id)
        {
            logger.debug("Propose response {} from {}", msg.payload, msg.from);

            if (msg.payload)
                accepts.incrementAndGet();
            else
                failures.incrementAndGet();

            if (isSuccessful() && !haveEmittedEvent.getAndSet(true))
                eventLoop.emitEvent(new ProposeResponseEvent(PaxosWriteTask.this, this));
            else if (failFast && (targets - failures.get()) < requiredAccepts && !haveEmittedEvent.getAndSet(true))
                eventLoop.emitEvent(new ProposeResponseEvent(PaxosWriteTask.this, this));
        }

        public int getAcceptCount()
        {
            return accepts.get();
        }

        public boolean isSuccessful()
        {
            return accepts.get() >= requiredAccepts;
        }

        // Note: this is only reliable if !failFast
        public boolean isFullyRefused()
        {
            return failures.get() == targets;
        }
    }

    /** Handler for responses for a PREPARE step */
    private class PrepareResponseHandler implements IAsyncCallback<PrepareResponse>
    {
        protected final AtomicInteger awaiting;

        public boolean promised = true;
        public Commit mostRecentCommit;
        public Commit mostRecentInProgressCommit;
        public Commit mostRecentInProgressCommitWithUpdate;
        public UUID ballot;

        private final Map<InetAddress, Commit> commitsByReplica = new ConcurrentHashMap<>();

        public PrepareResponseHandler(UUID ballot, DecoratedKey key, CFMetaData metadata)
        {
            this.ballot = ballot;
            awaiting = new AtomicInteger(requiredParticipants);

            mostRecentCommit = Commit.emptyCommit(key, metadata);
            mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
            mostRecentInProgressCommitWithUpdate = Commit.emptyCommit(key, metadata);
        }

        public synchronized void response(MessageIn<PrepareResponse> message, int id)
        {
            PrepareResponse response = message.payload;
            logger.debug("Prepare response {} from {}", response, message.from);

            // In case of clock skew, another node could be proposing with ballot that are quite a bit
            // older than our own. In that case, we record the more recent commit we've received to make
            // sure we re-prepare on an older ballot.
            if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
                mostRecentInProgressCommit = response.inProgressCommit;

            if (!response.promised)
            {
                promised = false;
                eventLoop.emitEvent(new PrepareResponseEvent(PaxosWriteTask.this, this));
                return;
            }

            commitsByReplica.put(message.from, response.mostRecentCommit);
            if (response.mostRecentCommit.isAfter(mostRecentCommit))
                mostRecentCommit = response.mostRecentCommit;

            // If some response has an update, then we should replay the update with the highest ballot. So find
            // the the highest commit that actually have an update
            if (response.inProgressCommit.isAfter(mostRecentInProgressCommitWithUpdate) && !response.inProgressCommit.update.isEmpty())
                mostRecentInProgressCommitWithUpdate = response.inProgressCommit;

            if (awaiting.decrementAndGet() == 0)
                eventLoop.emitEvent(new PrepareResponseEvent(PaxosWriteTask.this, this));
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public List<InetAddress> replicasMissingMostRecentCommit()
        {
            return commitsByReplica.keySet().stream()
                    .filter(addr -> !commitsByReplica.get(addr).ballot.equals(mostRecentCommit.ballot))
                    .collect(Collectors.toList());
        }
    }

    private Status doComplete(RowIterator result)
    {
        casContentionTimer.cancel();
        // TODO convert to ResultSet
        // return complete(result);
        return complete(null);
    }

    @Override
    public Status resume(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        if (wasPreempted)
        {
            wasPreempted = false;
            return tryProcessMutations();
        }

        return beginAndRepairPaxos();
    }

    @Override
    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        if (event instanceof PrepareResponseEvent)
        {
            return handlePrepareResponse(((PrepareResponseEvent) event).responseHandler);
        }
        else if (event instanceof ProposeResponseEvent)
        {
            return handleProposeResponse(((ProposeResponseEvent) event).responseHandler);
        }
        if (event instanceof WriteTask.WriteSuccessEvent)
        {
            // we've gotten acks on all writes
            commitWriteTimer.cancel();
            StorageProxy.casWriteMetrics.addNano(System.nanoTime() - startTime);

            CommitSubTask subTask = (CommitSubTask) ((WriteTask.WriteSuccessEvent) event).task;
            if (subTask.forRepair)
                return beginAndRepairPaxos();  // we finished a repair, check again for new proposals to repair

            recordFinalMetrics();
            return doComplete(null);
        }
        else if (event instanceof WriteTask.WriteTimeoutEvent)
        {
            cancelTimersForFailedWrite();
            if (event.task() instanceof SavePaxosCommitWriteTask)
                updateCasCommitStats();

            // If we're still doing preparation for the paxos rounds, use the CAS write type (see CASSANDRA-8672)
            WriteTask.WriteTimeoutEvent wte = (WriteTask.WriteTimeoutEvent) event;
            WriteType writeType = ((CommitWriteTask) event.task()).forRepair
                                ? WriteType.CAS
                                : wte.writeType;

            StorageProxy.casWriteMetrics.timeouts.mark();
            recordFinalMetrics();
            return fail(new WriteTimeoutException(writeType, wte.consistencyLevel, wte.acks, wte.blockedFor));
        }
        else if (event instanceof WriteTask.WriteFailureEvent)
        {
            cancelTimersForFailedWrite();
            if (event.task() instanceof SavePaxosCommitWriteTask)
                updateCasCommitStats();

            // If we're still doing preparation for the paxos rounds, use the CAS write type (see CASSANDRA-8672)
            WriteTask.WriteFailureEvent wfe = (WriteTask.WriteFailureEvent) event;
            WriteType writeType = ((CommitWriteTask) event.task()).forRepair
                                  ? WriteType.CAS
                                  : wfe.writeType;

            StorageProxy.casWriteMetrics.failures.mark();
            recordFinalMetrics();
            return fail(new WriteFailureException(wfe.consistencyLevel, wfe.acks, wfe.failures, wfe.blockedFor, writeType));
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    private void cancelTimersForFailedWrite()
    {
        if (commitWriteTimer != null)
            commitWriteTimer.cancel();

        casContentionTimer.cancel();
    }

    @Override
    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        Keyspace ks = Keyspace.open(keyspaceName);
        if (timer == casContentionTimer)
            return fail(new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(ks)));

        assert timer == commitWriteTimer;
        return fail(new WriteTimeoutException(WriteType.SIMPLE, consistencyForCommit, commitResponseHandler.ackCount(), consistencyForCommit.blockFor(ks)));
    }

    public static abstract class PaxosWriteEvent implements Event
    {
        protected final PaxosWriteTask task;

        protected PaxosWriteEvent(PaxosWriteTask task)
        {
            this.task = task;
        }

        public PaxosWriteTask task()
        {
            return this.task;
        }
    }

    public static class PrepareResponseEvent extends PaxosWriteEvent
    {
        final PrepareResponseHandler responseHandler;

        PrepareResponseEvent(PaxosWriteTask task, PrepareResponseHandler responseHandler)
        {
            super(task);
            this.responseHandler = responseHandler;
        }
    }

    public static class ProposeResponseEvent extends PaxosWriteEvent
    {
        final ProposeResponseHandler responseHandler;

        ProposeResponseEvent(PaxosWriteTask task, ProposeResponseHandler responseHandler)
        {
            super(task);
            this.responseHandler = responseHandler;
        }
    }

    public class CommitSubTask implements WriteTask.SubTask
    {
        PaxosWriteTask task;
        boolean forRepair;

        public CommitSubTask(PaxosWriteTask task, boolean forRepair)
        {
            this.task = task;
            this.forRepair = forRepair;
        }

        public Task getTask()
        {
            return task;
        }
    }

    /** Used for writing a commit locally */
    private class CommitWriteTask extends WriteTask
    {
        final boolean forRepair;

        private CommitWriteTask(Collection<? extends IMutation> mutations, boolean forRepair)
        {
            super(mutations);
            this.forRepair = forRepair;
        }

        @Override
        public void onFailure(Throwable t)
        {
            if (!(t instanceof WriteTimeoutException))
                logger.error("Failed to apply paxos commit locally{}: ", forRepair ? " (for repair)" : "", t);

            commitResponseHandler.onFailure(FBUtilities.getBroadcastAddress(), -1);
            updateCasCommitStats();
            fail(t);
        }

        @Override
        public void onComplete(Message.Response result)
        {
            handleLocalPaxosCommitCompleted();
        }
    }

    /** Used for saving paxos commits to the local system table */
    private class SavePaxosCommitWriteTask extends WriteTask
    {
        private SavePaxosCommitWriteTask(Collection<? extends IMutation> mutations)
        {
            super(mutations);
        }

        @Override
        public void onFailure(Throwable t)
        {
            logger.error("Error saving paxos commit to local system table: ", t);
            commitResponseHandler.onFailure(FBUtilities.getBroadcastAddress(), -1);
            updateCasCommitStats();
            fail(t);
        }

        @Override
        public void onComplete(Message.Response result)
        {
            commitResponseHandler.response(null, -1);
            updateCasCommitStats();
        }
    }
}
