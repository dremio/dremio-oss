/*
 * Copyright (C) 2017 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.work.foreman;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlanCaptureAttemptObserver;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.work.EndpointListener;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.NodeStatusListener;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;

/**
 * Each AttemptManager holds its own QueryManager.
 *
 * This manages the events associated with execution of a particular query across all fragments.
 */
class QueryManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);

  private final QueryId queryId;
  private final Pointer<QueryId> prepareId;
  private final QueryContext context;
  private final CompletionListener completionListener;
  private final PlanCaptureAttemptObserver capturer;

  private ImmutableMap<NodeEndpoint, NodeTracker> nodeMap = ImmutableMap.of();
  private ImmutableMap<FragmentHandle, FragmentData> fragmentDataMap = ImmutableMap.of();
  private List<MajorFragmentReporter> reporters = ImmutableList.of();

  // the following mutable variables are used to capture ongoing query status
  private long startPlanningTime;
  private long endPlanningTime;
  private long startTime;
  private long endTime;

  // How many nodes have finished their execution.  Query is complete when all nodes are complete.
  private final AtomicInteger finishedNodes = new AtomicInteger(0);

  // How many fragments have finished their execution.
  private final AtomicInteger finishedFragments = new AtomicInteger(0);

  public QueryManager(
      final QueryId queryId,
      final QueryContext context,
      final CompletionListener completionListener,
      final Pointer<QueryId> prepareId,
      final AttemptObservers observers,
      final boolean verboseProfiles) {
    this.queryId =  queryId;
    this.completionListener = completionListener;
    this.context = context;
    this.prepareId = prepareId;

    capturer = new PlanCaptureAttemptObserver(verboseProfiles);
    observers.add(capturer);
    observers.add(new TimeMarker());
  }

  private static boolean isTerminal(final FragmentState state) {
    return state == FragmentState.FAILED
        || state == FragmentState.FINISHED
        || state == FragmentState.CANCELLED;
  }

  private class TimeMarker extends AbstractAttemptObserver {

    @Override
    public void queryStarted(UserRequest query, String user) {
      markStartTime();
    }

    @Override
    public void planStart(String rawPlan) {
      markStartPlanningTime();
    }

    @Override
    public void planCompleted(ExecutionPlan plan) {
      markEndPlanningTime();
      if(plan != null){
        populate(plan.getFragments());
      }
    }

  }

  private boolean updateFragmentStatus(final FragmentStatus fragmentStatus) {
    final FragmentHandle fragmentHandle = fragmentStatus.getHandle();
    final FragmentData data = fragmentDataMap.get(fragmentHandle);

    final FragmentState oldState = data.getState();
    final boolean inTerminalState = isTerminal(oldState);
    final FragmentState currentState = fragmentStatus.getProfile().getState();

    if (inTerminalState || (oldState == FragmentState.CANCELLATION_REQUESTED && !isTerminal(currentState))) {
      // Already in a terminal state, or invalid state transition from CANCELLATION_REQUESTED. This shouldn't happen.
      logger.warn(String.format("Received status message for fragment %s after fragment was in state %s. New state was %s",
        QueryIdHelper.getQueryIdentifier(fragmentHandle), oldState, currentState));
      return false;
    }

    data.setStatus(fragmentStatus);
    return oldState != currentState;
  }

  private void fragmentDone(final FragmentStatus status) {
    final boolean stateChanged = updateFragmentStatus(status);

    if (stateChanged) {
      // since we're in the fragment done clause and this was a change from previous
      final NodeTracker node = nodeMap.get(status.getProfile().getEndpoint());
      node.fragmentComplete();
      finishedFragments.incrementAndGet();
    }
  }

  private void populate(List<PlanFragment> fragments){
    Map<NodeEndpoint, NodeTracker> trackers = new HashMap<>();
    Map<FragmentHandle, FragmentData> dataCollectors = new HashMap<>();
    ArrayListMultimap<Integer, FragmentData> majors = ArrayListMultimap.create();

    for(PlanFragment fragment : fragments) {
      final NodeEndpoint assignment = fragment.getAssignment();

      NodeTracker tracker = trackers.get(assignment);
      if (tracker == null) {
        tracker = new NodeTracker(assignment);
        trackers.put(assignment, tracker);
      }

      tracker.addFragment();
      FragmentData data = new FragmentData(fragment.getHandle(), assignment);
      dataCollectors.put(fragment.getHandle(), data);
      majors.put(fragment.getHandle().getMajorFragmentId(), data);
    }

    final ImmutableList.Builder<MajorFragmentReporter> reportersBuilder = ImmutableList.builder();
    for(Map.Entry<Integer, Collection<FragmentData>> e : majors.asMap().entrySet()){
      reportersBuilder.add(new MajorFragmentReporter(e.getKey(), e.getValue()));
    }

    this.reporters = reportersBuilder.build();
    this.nodeMap = ImmutableMap.copyOf(trackers);
    this.fragmentDataMap = ImmutableMap.copyOf(dataCollectors);
  }

  /**
   * Stop all fragments with currently *known* active status (active as in SENDING, AWAITING_ALLOCATION, RUNNING).
   *
   * For the actual cancel calls for intermediate and leaf fragments, see
   * {@link com.dremio.sabot.rpc.CoordToExecHandler#cancelFragment}
   * (1) Root fragment: pending or running, send the cancel signal through a tunnel.
   * (2) Intermediate fragment: pending or running, send the cancel signal through a tunnel (for local and remote
   *    fragments). The actual cancel is done by delegating the cancel to the work bus.
   * (3) Leaf fragment: running, send the cancel signal through a tunnel. The cancel is done directly.
   */
  void cancelExecutingFragments(final CoordToExecTunnelCreator creator) {
    for(final FragmentData data : fragmentDataMap.values()) {
      switch(data.getState()) {
      case SENDING:
      case AWAITING_ALLOCATION:
      case RUNNING:
        final FragmentHandle handle = data.getHandle();
        final NodeEndpoint endpoint = data.getEndpoint();
        // TODO is the CancelListener redundant? Does the FragmentStatusListener get notified of the same?
        creator.getTunnel(endpoint).cancelFragment(new SignalListener(endpoint, handle,
            SignalListener.Signal.CANCEL), handle);
        break;

      case FINISHED:
      case CANCELLATION_REQUESTED:
      case CANCELLED:
      case FAILED:
        // nothing to do
        break;
      }
    }
  }

  /*
     * This assumes that the FragmentStatusListener implementation takes action when it hears
     * that the target fragment has acknowledged the signal. As a result, this listener doesn't do anything
     * but log messages.
     */
  private static class SignalListener extends EndpointListener<Ack, FragmentHandle> {
    /**
     * An enum of possible signals that {@link SignalListener} listens to.
     */
    public enum Signal { CANCEL, UNPAUSE }

    private final Signal signal;

    public SignalListener(final NodeEndpoint endpoint, final FragmentHandle handle, final Signal signal) {
      super(endpoint, handle);
      this.signal = signal;
    }

    @Override
    public void failed(final RpcException ex) {
      final String endpointIdentity = endpoint != null ?
        endpoint.getAddress() + ":" + endpoint.getUserPort() : "<null>";
      logger.error("Failure while attempting to {} fragment {} on endpoint {} with {}.",
        signal, QueryIdHelper.getQueryIdentifier(value), endpointIdentity, ex);
    }

    @Override
    public void success(final Ack ack, final ByteBuf buf) {
      if (!ack.getOk()) {
        logger.warn("Remote node {} responded negative on {} request for fragment {} with {}.", endpoint, signal, value,
          ack);
      }
    }

    @Override
    public void interrupted(final InterruptedException ex) {
      logger.error("Interrupted while waiting for RPC outcome of action fragment {}. " +
          "Endpoint {}, Fragment handle {}", signal, endpoint, value, ex);
    }
  }



  public QueryProfile getQueryProfile(String description, QueryState state, UserException ex) {
    final QueryProfile.Builder profileBuilder = QueryProfile.newBuilder()
        .setQuery(description)
        .setUser(context.getQueryUserName())
        .setId(queryId)
        .setState(state)
        .setForeman(context.getCurrentEndpoint())
        .setStart(startTime)
        .setEnd(endTime)
        .setPlanningStart(startPlanningTime)
        .setPlanningEnd(endPlanningTime)
        .setTotalFragments(fragmentDataMap.size())
        .setFinishedFragments(finishedFragments.get());

    if(prepareId.value != null){
      profileBuilder.setPrepareId(prepareId.value);
    }

    if(context.getSession().getClientInfos() != null) {
      profileBuilder.setClientInfo(context.getSession().getClientInfos());
    }

    UserResult.addError(ex, profileBuilder);


    if (capturer != null) {
      profileBuilder.setAccelerationProfile(capturer.getAccelerationProfile());

      final String planText = capturer.getText();
      if (planText != null) {
        profileBuilder.setPlan(planText);
      }

      final String planJson = capturer.getJson();
      if (planJson != null) {
        profileBuilder.setJsonPlan(planJson);
      }

      final List<PlanPhaseProfile> planPhases = capturer.getPlanPhases();
      if (planPhases != null) {
        profileBuilder.addAllPlanPhases(planPhases);
      }
    }

    for(MajorFragmentReporter reporter : reporters){
      final MajorFragmentProfile.Builder builder = MajorFragmentProfile.newBuilder().setMajorFragmentId(reporter.majorFragmentId);
      reporter.add(builder);
      profileBuilder.addFragmentProfile(builder);
    }

    return profileBuilder.build();
  }

  private void markStartPlanningTime() {
    startPlanningTime = System.currentTimeMillis();
  }

  private void markEndPlanningTime() {
    endPlanningTime = System.currentTimeMillis();
  }

  private void markStartTime() {
    startTime = System.currentTimeMillis();
  }

  void markEndTime() {
    endTime = System.currentTimeMillis();
  }

  /**
   * Internal class used to track the number of pending completion messages required from particular node. This allows
   * to know for each node that is part of this query, what portion of fragments are still outstanding. In the case that
   * there is a node failure, we can then correctly track how many outstanding messages will never arrive.
   */
  private class NodeTracker {
    private final NodeEndpoint endpoint;
    private final AtomicInteger totalFragments = new AtomicInteger(0);
    private final AtomicInteger completedFragments = new AtomicInteger(0);

    public NodeTracker(final NodeEndpoint endpoint) {
      this.endpoint = endpoint;
    }

    /**
     * Increments the number of fragment this node is running.
     */
    public void addFragment() {
      totalFragments.incrementAndGet();
    }

    /**
     * Increments the number of fragments completed on this node.  Once the number of fragments completed
     * equals the number of fragments running, this will be marked as a finished node and result in the finishedNodes being incremented.
     *
     * If the number of remaining nodes has been decremented to zero, this will allow the query to move to a completed state.
     */
    public void fragmentComplete() {
      if (totalFragments.get() == completedFragments.incrementAndGet()) {
        nodeComplete();
      }
    }

    /**
     * Increments the number of fragments completed on this node until we mark this node complete. Note that this uses
     * the internal fragmentComplete() method so whether we have failure or success, the nodeComplete event will only
     * occur once. (Two threads could be decrementing the fragment at the same time since this will likely come from an
     * external event).
     *
     * @return true if the node has fragments that are pending (non-terminal state); false if all fragments running on
     * this node have already terminated.
     */
    public boolean nodeDead() {
      if (completedFragments.get() == totalFragments.get()) {
        return false;
      }
      while (completedFragments.get() < totalFragments.get()) {
        fragmentComplete();
      }
      return true;
    }

    @Override
    public String toString() {
      return "NodeTracker [endpoint=" + endpoint + ", totalFragments=" + totalFragments.get() + ", completedFragments="
          + completedFragments.get() + "]";
    }


  }

  /**
   * Increments the number of currently complete nodes and returns the number of completed nodes. If the there are no
   * more pending nodes, moves the query to a terminal state.
   */
  private void nodeComplete() {
    final int finishedNodes = this.finishedNodes.incrementAndGet();
    final int totalNodes = nodeMap.size();
    Preconditions.checkArgument(finishedNodes <= totalNodes, "The finished node count exceeds the total node count");
    final int remaining = totalNodes - finishedNodes;
    if (remaining == 0) {
      // this target state may be adjusted in moveToState() based on current FAILURE/CANCELLATION_REQUESTED status
      completionListener.succeeded();
    } else {
      logger.debug("AttemptManager is still waiting for completion message from {} nodes containing {} fragments", remaining,
          this.fragmentDataMap.size() - finishedFragments.get());
    }
  }

  public FragmentStatusListener getFragmentStatusListener(){
    return fragmentStatusListener;
  }

  private final FragmentStatusListener fragmentStatusListener = new FragmentStatusListener() {
    @Override
    public void statusUpdate(final FragmentStatus status) {
      logger.debug("New fragment status was provided to QueryManager of {}", status);
      switch(status.getProfile().getState()) {
      case AWAITING_ALLOCATION:
      case RUNNING:
      case CANCELLATION_REQUESTED:
        updateFragmentStatus(status);
        break;

      case FAILED:
        logger.info("Fragment {} failed, cancelling remaining fragments.", QueryIdHelper.getQueryIdentifier(status.getHandle()));
        completionListener.failed(new UserRemoteException(status.getProfile().getError()));
        // fall-through.
      case FINISHED:
      case CANCELLED:
        fragmentDone(status);
        break;

      default:
        throw new UnsupportedOperationException(String.format("Received status of %s", status));
      }
    }
  };


  public NodeStatusListener getNodeStatusListener() {
    return nodeStatusListener;
  }

  private final NodeStatusListener nodeStatusListener = new NodeStatusListener(){

    @Override
    public void nodesRegistered(final Set<NodeEndpoint> registeredNodes) {
    }

    @Override
    public void nodesUnregistered(final Set<NodeEndpoint> unregisteredNodes) {
      final StringBuilder failedNodeList = new StringBuilder();
      boolean atLeastOneFailure = false;

      for (final NodeEndpoint ep : unregisteredNodes) {
        final NodeTracker tracker = nodeMap.get(ep);
        if (tracker == null) {
          continue; // fragments were not assigned to this SabotNode
        }

        // mark node as dead.
        if (!tracker.nodeDead()) {
          continue; // fragments assigned to this SabotNode completed
        }

        // fragments were running on the SabotNode, capture node name for exception or logging message
        if (atLeastOneFailure) {
          failedNodeList.append(", ");
        } else {
          atLeastOneFailure = true;
        }
        failedNodeList.append(ep.getAddress());
        failedNodeList.append(":");
        failedNodeList.append(ep.getUserPort());
      }

      if (atLeastOneFailure) {
        logger.warn("Nodes [{}] no longer registered in cluster.  Canceling query {}",
            failedNodeList, QueryIdHelper.getQueryId(queryId));

        completionListener.failed(
            new ForemanException(String.format("One more more nodes lost connectivity during query.  Identified nodes were [%s].",
                failedNodeList)));
      }

    }
  };

  private class MajorFragmentReporter {
    private final int majorFragmentId;
    private ImmutableList<FragmentData> fragments;

    public MajorFragmentReporter(int majorFragmentId, Collection<FragmentData> fragments) {
      super();
      this.majorFragmentId = majorFragmentId;
      this.fragments = ImmutableList.copyOf(fragments);
    }


    public void add(MajorFragmentProfile.Builder builder){
      for(FragmentData data : fragments){
        builder.addMinorFragmentProfile(data.getProfile());
      }
    }
  }

}
