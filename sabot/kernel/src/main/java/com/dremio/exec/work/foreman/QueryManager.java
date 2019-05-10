/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.EndpointHelper;
import com.dremio.exec.planner.PlanCaptureAttemptObserver;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.handlers.commands.ResourceAllocationResultObserver;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodePhaseProfile;
import com.dremio.exec.proto.UserBitShared.NodeQueryProfile;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.work.EndpointListener;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.options.OptionList;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.NodeStatusListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;

/**
 * Each AttemptManager holds its own QueryManager.
 *
 * This manages the events associated with execution of a particular query across all fragments.
 */
class QueryManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);

  private static final ObjectWriter JSON_PRETTY_SERIALIZER = new ObjectMapper().writerWithDefaultPrettyPrinter();

  private final QueryId queryId;
  private final Pointer<QueryId> prepareId;
  private final QueryContext context;
  private final CoordToExecTunnelCreator tunnelCreator;
  private final CompletionListener completionListener;
  private final PlanCaptureAttemptObserver capturer;
  private final ResourceAllocationResultObserver resourceAllocationResultObserver;
  private final Catalog catalog;
  private final OptionList nonDefaultOptions;

  private ImmutableMap<NodeEndpoint, NodeTracker> nodeMap = ImmutableMap.of();
  private ImmutableMap<FragmentHandle, FragmentData> fragmentDataMap = ImmutableMap.of();
  private ImmutableList<MajorFragmentReporter> reporters = ImmutableList.of();
  private ImmutableMap<NodeEndpoint, NodeReporter> nodeReporters = ImmutableMap.of();

  // the following mutable variables are used to capture ongoing query status
  private long commandPoolWait;
  private long startPlanningTime;
  private long endPlanningTime;  // NB: tracks the end of both planning and resource scheduling. Name kept to match the profile object, which was kept intact for legacy reasons
  private long startTime;
  private long endTime;

  // How many nodes have finished their execution.  Query is complete when all nodes are complete.
  private final AtomicInteger finishedNodes = new AtomicInteger(0);

  // How many fragments have finished their execution.
  private final AtomicInteger finishedFragments = new AtomicInteger(0);

  public QueryManager(
    final QueryId queryId,
    final QueryContext context,
    final CoordToExecTunnelCreator tunnelCreator,
    final CompletionListener completionListener,
    final Pointer<QueryId> prepareId,
    final AttemptObservers observers,
    final boolean verboseProfiles,
    final boolean includeDatasetProfiles,
    final Catalog catalog) {
    this.queryId =  queryId;
    this.tunnelCreator = tunnelCreator;
    this.completionListener = completionListener;
    this.context = context;
    this.prepareId = prepareId;
    this.catalog = catalog;
    this.nonDefaultOptions = context.getNonDefaultOptions();

    resourceAllocationResultObserver = new ResourceAllocationResultObserver();
    observers.add(resourceAllocationResultObserver);

    capturer = new PlanCaptureAttemptObserver(verboseProfiles, includeDatasetProfiles, context.getFunctionRegistry(),
      context.getAccelerationManager().newPopulator());
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
    public void commandPoolWait(long waitInMillis) {
      addCommandPoolWaitTime(waitInMillis);
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

  private void populate(List<PlanFragmentFull> fragments){
    Map<NodeEndpoint, NodeTracker> trackers = new HashMap<>();
    Map<FragmentHandle, FragmentData> dataCollectors = new HashMap<>();
    ArrayListMultimap<Integer, FragmentData> majors = ArrayListMultimap.create();
    Map<NodeEndpoint, NodeReporter> nodeReporters = new HashMap<>();

    for(PlanFragmentFull fragment : fragments) {
      final NodeEndpoint assignment = fragment.getMinor().getAssignment();

      NodeTracker tracker = trackers.get(assignment);
      if (tracker == null) {
        tracker = new NodeTracker(assignment);
        trackers.put(assignment, tracker);
      }

      tracker.addFragment();
      FragmentData data = new FragmentData(fragment.getHandle(), assignment);
      dataCollectors.put(fragment.getHandle(), data);
      majors.put(fragment.getHandle().getMajorFragmentId(), data);

      if (nodeReporters.get(assignment) == null) {
        nodeReporters.put(assignment, new NodeReporter(assignment));
      }
    }

    // Major fragments are required to be dense: numbered 0 through N-1
    MajorFragmentReporter[] tempReporters = new MajorFragmentReporter[majors.asMap().size()];
    for(Map.Entry<Integer, Collection<FragmentData>> e : majors.asMap().entrySet()) {
      Preconditions.checkElementIndex(e.getKey(), majors.asMap().size());
      Preconditions.checkState(tempReporters[e.getKey()] == null);
      tempReporters[e.getKey()] = new MajorFragmentReporter(e.getKey(), e.getValue());
    }

    this.reporters = ImmutableList.copyOf(tempReporters);
    this.nodeMap = ImmutableMap.copyOf(trackers);
    this.fragmentDataMap = ImmutableMap.copyOf(dataCollectors);
    this.nodeReporters = ImmutableMap.copyOf(nodeReporters);
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
  void cancelExecutingFragments() {
    final FragmentData fragments [] = new FragmentData[fragmentDataMap.values().size()];
    fragmentDataMap.values().toArray(fragments);

    //colocate fragments running on same node
    Arrays.sort(fragments, new Comparator<FragmentData>() {
      @Override
      public int compare(final FragmentData o1, final FragmentData o2) {
        return o1.getEndpoint().getAddress().compareTo(o2.getEndpoint().getAddress());
      }
    });

    for(final FragmentData data : fragments) {
      switch(data.getState()) {
      case SENDING:
      case AWAITING_ALLOCATION:
      case RUNNING:
        final FragmentHandle handle = data.getHandle();
        final NodeEndpoint endpoint = data.getEndpoint();
        // TODO is the CancelListener redundant? Does the FragmentStatusListener get notified of the same?
        tunnelCreator.getTunnel(endpoint).cancelFragment(new SignalListener(endpoint, handle,
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



  public QueryProfile getQueryProfile(String description, QueryState state, UserException ex, String cancelreason) {
    final QueryProfile.Builder profileBuilder = QueryProfile.newBuilder()
        .setQuery(description)
        .setUser(context.getQueryUserName())
        .setId(queryId)
        .setState(state)
        .setForeman(context.getCurrentEndpoint())
        .setStart(startTime)
        .setEnd(endTime)
        .setCommandPoolWaitMillis(commandPoolWait)
        .setPlanningStart(startPlanningTime)
        .setPlanningEnd(endPlanningTime)
        .setTotalFragments(fragmentDataMap.size())
        .setFinishedFragments(finishedFragments.get())
        .setDremioVersion(DremioVersionInfo.getVersion());

    if (cancelreason != null) {
      profileBuilder.setCancelReason(cancelreason);
    }
    if(prepareId.value != null){
      profileBuilder.setPrepareId(prepareId.value);
    }

    if(context.getSession().getClientInfos() != null) {
      profileBuilder.setClientInfo(context.getSession().getClientInfos());
    }

    UserResult.addError(ex, profileBuilder);

    final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo =
      resourceAllocationResultObserver.getResourceSchedulingDecisionInfo();

    if (resourceSchedulingDecisionInfo != null) {
      UserBitShared.ResourceSchedulingProfile.Builder resourceBuilder = UserBitShared.ResourceSchedulingProfile
        .newBuilder();
      if (resourceSchedulingDecisionInfo.getQueueId() != null) {
        resourceBuilder.setQueueId(resourceSchedulingDecisionInfo.getQueueId());
      }
      if (resourceSchedulingDecisionInfo.getQueueName() != null) {
        resourceBuilder.setQueueName(resourceSchedulingDecisionInfo.getQueueName());
      }
      if (resourceSchedulingDecisionInfo.getRuleContent() != null) {
        resourceBuilder.setRuleContent(resourceSchedulingDecisionInfo.getRuleContent());
      }
      if (resourceSchedulingDecisionInfo.getRuleId() != null) {
        resourceBuilder.setRuleId(resourceSchedulingDecisionInfo.getRuleId());
      }
      if (resourceSchedulingDecisionInfo.getRuleName() != null) {
        resourceBuilder.setRuleName(resourceSchedulingDecisionInfo.getRuleName());
      }
      if (resourceSchedulingDecisionInfo.getRuleAction() != null) {
        resourceBuilder.setRuleAction(resourceSchedulingDecisionInfo.getRuleAction());
      }
      final ResourceSchedulingProperties schedulingProperties = resourceSchedulingDecisionInfo
        .getResourceSchedulingProperties();
      if (schedulingProperties != null) {
        UserBitShared.ResourceSchedulingProperties.Builder resourcePropsBuilder =
          UserBitShared.ResourceSchedulingProperties.newBuilder();
        if (schedulingProperties.getClientType() != null) {
          resourcePropsBuilder.setClientType(schedulingProperties.getClientType());
        }
        if (schedulingProperties.getQueryType() != null) {
          resourcePropsBuilder.setQueryType(schedulingProperties.getQueryType());
        }
        if (schedulingProperties.getQueryCost() != null) {
          resourcePropsBuilder.setQueryCost(schedulingProperties.getQueryCost());
        }
        if (schedulingProperties.getTag() != null) {
          resourcePropsBuilder.setTag(schedulingProperties.getTag());
        }
        resourceBuilder.setSchedulingProperties(resourcePropsBuilder);
      }
      resourceBuilder.setResourceSchedulingStart(resourceSchedulingDecisionInfo.getSchedulingStartTimeMs());
      resourceBuilder.setResourceSchedulingEnd(resourceSchedulingDecisionInfo.getSchedulingEndTimeMs());
      profileBuilder.setResourceSchedulingProfile(resourceBuilder);
    }

    if (capturer != null) {
      profileBuilder.setAccelerationProfile(capturer.getAccelerationProfile());

      profileBuilder.addAllDatasetProfile(capturer.getDatasets());

      final String planText = capturer.getText();
      if (planText != null) {
        profileBuilder.setPlan(planText);
      }

      final String planJson = capturer.getJson();
      if (planJson != null) {
        profileBuilder.setJsonPlan(planJson);
      }

      final String fullSchema = capturer.getFullSchema();
      if (fullSchema != null) {
        profileBuilder.setFullSchema(fullSchema);
      }

      final List<PlanPhaseProfile> planPhases = capturer.getPlanPhases();
      if (planPhases != null) {
        profileBuilder.addAllPlanPhases(planPhases);
      }
    }

    try {
      profileBuilder.setNonDefaultOptionsJSON(JSON_PRETTY_SERIALIZER.writeValueAsString(nonDefaultOptions));
    } catch (Exception e) {
      logger.warn("Failed to serialize the non-default option list to JSON", e);
      profileBuilder.setNonDefaultOptionsJSON("Failed to serialize the non-default option list to JSON: " + e.getMessage());
    }

    // get stats from schema tree provider
    profileBuilder.addAllPlanPhases(catalog.getMetadataStatsCollector().getPlanPhaseProfiles());

    for(MajorFragmentReporter reporter : reporters) {
      final MajorFragmentProfile.Builder builder = MajorFragmentProfile.newBuilder()
        .setMajorFragmentId(reporter.majorFragmentId);
      reporter.add(builder);
      profileBuilder.addFragmentProfile(builder);
    }

    for (NodeReporter nodeReporter : nodeReporters.values()) {
      final NodeQueryProfile.Builder builder = NodeQueryProfile.newBuilder()
        .setEndpoint(nodeReporter.endpoint);
      nodeReporter.add(builder);
      profileBuilder.addNodeProfile(builder);
    }

    profileBuilder.setOperatorTypeMetricsMap(OperatorMetricRegistry.getCoreOperatorTypeMetricsMap());

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

  private void addCommandPoolWaitTime(long waitInMillis) {
    commandPoolWait += waitInMillis;
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

    /**
     * @return true if all fragments running on this node already finished
     */
    public boolean isDone() {
      return totalFragments.get() == completedFragments.get();
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
        logger.debug("Fragment {} failed, cancelling remaining fragments.", QueryIdHelper.getQueryIdentifier(status.getHandle()));
        completionListener.failed(UserRemoteException.create(status.getProfile().getError()));
        fragmentDone(status);
        break;
      case FINISHED:
        FragmentHandle fragmentHandle = status.getHandle();
        if (fragmentHandle.getMajorFragmentId() == 0) {
          cancelExecutingFragments();
        }
        fragmentDone(status);
        break;
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
      // let's do a pass through all unregistered nodes, check if any of them has running fragments without marking them
      // as complete for now. Otherwise, the last node may complete successfully and send a completion message to the
      // foreman which will mark the query as succeeded.
      // Note that by the time we build and send the failure message, its possible that we get a completion message
      // from the last fragments that were running on the dead nodes which may cause the query to be marked completed
      // which is actually correct in this case as the node died after all its fragments completed.
      final StringBuilder failedNodeList = new StringBuilder();
      boolean atLeastOneFailure = false;

      List<NodeTracker> nodesToMarkDead = new ArrayList<>();
      for (final NodeEndpoint ep : unregisteredNodes) {
        /*
         * The nodeMap has minimal endpoints, while this list has nodes with additional attributes.
         * Convert to a minimal endpoint prior to lookup.
         */
        final NodeTracker tracker = nodeMap.get(EndpointHelper.getMinimalEndpoint(ep));
        if (tracker == null) {
          continue; // fragments were not assigned to this SabotNode
        }
        nodesToMarkDead.add(tracker);

        // mark node as dead.
        if (tracker.isDone()) {
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
          new ForemanException(String.format("One or more nodes lost connectivity during query.  Identified nodes were [%s].",
            failedNodeList)));
      }

      // now we can call nodeDead() on all unregistered nodes
      for (final NodeTracker tracker : nodesToMarkDead) {
        tracker.nodeDead();
      }
    }
  };

  public void updateNodeQueryStatus(NodeQueryStatus status) {
    NodeEndpoint endpoint = status.getEndpoint();
    for (NodePhaseStatus phaseStatus : status.getPhaseStatusList()) {
      reporters.get(phaseStatus.getMajorFragmentId()).updatePhaseStatus(endpoint, phaseStatus);
    }
    nodeReporters.get(endpoint)
      .updateMaxMemory(status.getMaxMemoryUsed())
      .updateTimeEnqueuedBeforeSubmit(status.getTimeEnqueuedBeforeSubmitMs());
  }

  private class MajorFragmentReporter {
    private final int majorFragmentId;
    private ImmutableList<FragmentData> fragments;
    private final Map<NodeEndpoint, NodePhaseStatus> perNodeStatus = Maps.newConcurrentMap();

    public MajorFragmentReporter(int majorFragmentId, Collection<FragmentData> fragments) {
      super();
      this.majorFragmentId = majorFragmentId;
      this.fragments = ImmutableList.copyOf(fragments);
    }


    public void add(MajorFragmentProfile.Builder builder){
      for(FragmentData data : fragments){
        builder.addMinorFragmentProfile(data.getProfile());
      }
      for (Map.Entry<NodeEndpoint, NodePhaseStatus> endpointStatus : perNodeStatus.entrySet()) {
        NodePhaseProfile nodePhaseProfile = NodePhaseProfile.newBuilder()
          .setEndpoint(endpointStatus.getKey())
          .setMaxMemoryUsed(endpointStatus.getValue().getMaxMemoryUsed())
          .build();
        builder.addNodePhaseProfile(nodePhaseProfile);
      }
    }

    public void updatePhaseStatus(NodeEndpoint assignment, NodePhaseStatus status) {
      perNodeStatus.put(assignment, status);
    }
  }

  private class NodeReporter {
    private final NodeEndpoint endpoint;
    private long maxMemory;
    private long timeEnqueuedBeforeSubmitMs;

    public NodeReporter(NodeEndpoint endpoint) {
      this.endpoint = endpoint;
      this.maxMemory = 0L;
      this.timeEnqueuedBeforeSubmitMs = 0L;
    }

    public void add(NodeQueryProfile.Builder builder){
      builder.setEndpoint(endpoint);
      builder.setMaxMemoryUsed(maxMemory);
      builder.setTimeEnqueuedBeforeSubmitMs(timeEnqueuedBeforeSubmitMs);
    }

    public NodeReporter updateMaxMemory(long maxMemory) {
      this.maxMemory = maxMemory;
      return this;
    }

    public NodeReporter updateTimeEnqueuedBeforeSubmit(long value) {
      this.timeEnqueuedBeforeSubmitMs = value;
      return this;
    }
  }

}
