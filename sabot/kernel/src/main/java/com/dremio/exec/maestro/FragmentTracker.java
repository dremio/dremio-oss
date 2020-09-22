/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.maestro;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC.CancelFragments;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.work.foreman.CompletionListener;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.ListenableSet;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

class FragmentTracker implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentTracker.class);
  private final QueryId queryId;
  private final CompletionListener completionListener;
  private final Runnable queryCloser;
  private final ExecutorSetService executorSetService;
  private final ExecutorServiceClientFactory executorServiceClientFactory;
  private final AtomicReference<Exception> firstError = new AtomicReference<>();
  private final NodeStatusListener nodeStatusListener = new ExecutorNodeStatusListener();
  private final AtomicBoolean completionSuccessNotified = new AtomicBoolean(false);
  private volatile ListenableSet executorSet;
  private volatile boolean cancelled;
  private volatile boolean queryCloserInvoked;

  private Set<NodeEndpoint> pendingNodes = ConcurrentHashMap.newKeySet();
  private Set<NodeEndpoint> lostNodes = ConcurrentHashMap.newKeySet();

  public FragmentTracker(
    QueryId queryId,
    CompletionListener completionListener,
    Runnable queryCloser,
    ExecutorServiceClientFactory executorServiceClientFactory,
    ExecutorSetService executorSetService
    ) {

    this.queryId = queryId;
    this.completionListener = completionListener;
    this.queryCloser = queryCloser;
    this.executorSetService = executorSetService;
    this.executorServiceClientFactory = executorServiceClientFactory;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public void populate(List<PlanFragmentFull> fragments, ResourceSchedulingDecisionInfo decisionInfo) {
    for (PlanFragmentFull fragment : fragments) {
      final NodeEndpoint assignment = fragment.getMinor().getAssignment();
      pendingNodes.add(assignment);
    }

    executorSet = executorSetService.getExecutorSet(decisionInfo.getEngineId(), decisionInfo.getSubEngineId());
    executorSet.addNodeStatusListener(nodeStatusListener);
    validateEndpoints();
    checkAndNotifyCompletionListener();
  }

  /**
   * Throws an exception if any of the executors the query is scheduled to run on is down
   */
  private void validateEndpoints() {

    // set of all active endpoints, minimized
    Set<NodeEndpoint> minimizedActiveSet = executorSet
        .getAvailableEndpoints()
        .stream()
        .map((e) -> EndpointHelper.getMinimalEndpoint(e))
        .collect(Collectors.toSet());

    logger.debug("validating endpoints {}", minimizedActiveSet);

    // will contain the list of all failed nodes
    List<NodeEndpoint> failedNodeList = new ArrayList<>();

    for (NodeEndpoint endpoint : pendingNodes) {
      if (minimizedActiveSet.contains(endpoint)) {
        // endpoint is still active, nothing more to do
        continue;
      }

      failedNodeList.add(endpoint);
    }

    if (!failedNodeList.isEmpty()) {
      logger.warn(
              "One or more nodes are down {}, cancelling query {}",
              failedNodeList,
              queryId.toString());

      checkAndUpdateFirstError(
              new ExecutionSetupException(
                      String.format(
                              "One or more nodes lost connectivity during query.  Identified nodes were [%s].",
                              failedNodeList)));
    }
  }

  void sendOrActivateFragmentsFailed(Exception ex) {
    checkAndUpdateFirstError(ex);
  }

  public void nodeMarkFirstError(NodeQueryFirstError firstError) {
    logger.info("Fragment {} failed, cancelling remaining fragments.",
            QueryIdHelper.getQueryIdentifier(firstError.getHandle()));
    checkAndUpdateFirstError(UserRemoteException.create(firstError.getError()));
  }

  public void nodeCompleted(NodeQueryCompletion completion) {
    if (completion.hasFirstError()) {
      logger.debug("received node completion with error for query {} from node {}:{}",
        QueryIdHelper.getQueryId(queryId), completion.getEndpoint().getAddress(), completion.getEndpoint().getFabricPort());

      checkAndUpdateFirstError(UserRemoteException.create(completion.getFirstError()));
    }
    markNodeDone(completion.getEndpoint());
  }

  public void screenCompleted() {
    logger.debug("received screen completion for query {}, cancelling remaining fragments",
      QueryIdHelper.getQueryId(queryId));
    cancelExecutingFragmentsInternal();
  }

  void handleFailedNodes(final Set<NodeEndpoint> unregisteredNodes) {
    // let's do a pass through all unregistered nodes, check if any of them has running fragments without marking them
    // as complete for now. Otherwise, the last node may complete successfully and send a completion message to the
    // foreman which will mark the query as succeeded.
    // Note that by the time we build and send the failure message, its possible that we get a completion message
    // from the last fragments that were running on the dead nodes which may cause the query to be marked completed
    // which is actually correct in this case as the node died after all its fragments completed.

    List<NodeEndpoint> nodesToMarkDead = new ArrayList<>();
    for (final NodeEndpoint ep : unregisteredNodes) {
      /*
       * The nodeMap has minimal endpoints, while this list has nodes with additional attributes.
       * Convert to a minimal endpoint prior to lookup.
       */
      NodeEndpoint minimalEp = EndpointHelper.getMinimalEndpoint(ep);
      if (!pendingNodes.contains(minimalEp)) {
        // fragments were not assigned to this executor (or) all assigned fragments are
        // already complete.
        continue;
      }
      nodesToMarkDead.add(minimalEp);
    }

    if (nodesToMarkDead.size() > 0) {
      lostNodes.addAll(nodesToMarkDead);
      String failedNodeDesc = nodesToStringDesc(nodesToMarkDead);
      logger.warn("Nodes [{}] no longer registered in cluster.  Canceling query {}",
              failedNodeDesc, QueryIdHelper.getQueryId(queryId));

      checkAndUpdateFirstError(
              new ExecutionSetupException(String.format("One or more nodes lost connectivity during query.  Identified nodes were [%s].",
                      failedNodeDesc)));
    }

    // now we can deem the failed nodes as completed.
    for (final NodeEndpoint endpoint : nodesToMarkDead) {
      markNodeDone(endpoint);
    }
  }

  private String nodesToStringDesc(Collection<NodeEndpoint> nodes) {
    final StringBuilder desc = new StringBuilder();
    boolean isFirst = true;

    for (NodeEndpoint ep : nodes) {
      if (isFirst) {
        isFirst = false;
      } else {
        desc.append(", ");
      }
      desc.append(ep.getAddress());
      desc.append(":");
      desc.append(ep.getUserPort());
    }
    return desc.toString();
  }

  private void markNodeDone(NodeEndpoint endpoint) {
    if (pendingNodes.remove(endpoint)) {
      logger.debug("node completed for query {} from node {}:{}, pending nodes {}",
        QueryIdHelper.getQueryId(queryId), endpoint.getAddress(), endpoint.getFabricPort(), nodesToStringDesc(pendingNodes));
    } else {
      logger.warn("handling completion for query {} on node {}:{}, which is not in the list of pending nodes {}, ignoring message",
        QueryIdHelper.getQueryId(queryId), endpoint.getAddress(), endpoint.getFabricPort(), nodesToStringDesc(pendingNodes));
    }
    checkAndNotifyCompletionListener();
  }

  void cancelExecutingFragments() {
    cancelled = true;
    cancelExecutingFragmentsInternal();
  }

  /**
   * Cancel all fragments. Only one rpc is sent per executor.
   */
  private void cancelExecutingFragmentsInternal() {
    CancelFragments fragments = CancelFragments
      .newBuilder()
      .setQueryId(queryId)
      .build();

    for (NodeEndpoint endpoint : pendingNodes) {
      logger.debug("sending cancellation for query {} to node {}:{}",
        QueryIdHelper.getQueryId(queryId), endpoint.getAddress(), endpoint.getFabricPort());

      executorServiceClientFactory.getClientForEndpoint(endpoint).cancelFragments(fragments, new SignalListener(endpoint, fragments,
        SignalListener.Signal.CANCEL));
    }

    checkAndNotifyCompletionListener();
  }

  // Save and propagate the first reported by any executor.
  void checkAndUpdateFirstError(Exception e) {
    if (firstError.compareAndSet(null, e)) {
      completionListener.failed(e);
    }
    checkAndCloseQuery();
  }

  // notify the completion listener on success exactly once, if there are no pending
  // nodes.
  private void checkAndNotifyCompletionListener() {
    if (pendingNodes.isEmpty() && !completionSuccessNotified.getAndSet(true)) {
      completionListener.succeeded();
    }
    checkAndCloseQuery();
  }

  private void checkAndCloseQuery() {
    if (pendingNodes.isEmpty() || // nothing pending from any of the executors.
        firstError.get() != null && cancelled) { // there was an error, and the cancels have been sent.
      queryCloser.run();
      queryCloserInvoked = true;
    }
  }

  @Override
  public void close() throws Exception {
    if (executorSet != null) {
      executorSet.removeNodeStatusListener(nodeStatusListener);
      executorSet = null;
    }
  }

  private class ExecutorNodeStatusListener implements NodeStatusListener {

    @Override
    public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
      if (logger.isDebugEnabled()) {
        logger.debug("nodes Unregistered {}, received notification for query {}",
          unregisteredNodes, queryId);
      }

      try {
        handleFailedNodes(unregisteredNodes);
      } catch (Exception e) {
        logger.warn("FragmentTracker {} failed to handle unregistered nodes {}",
          QueryIdHelper.getQueryId(queryId), unregisteredNodes, e);
      } catch (Throwable e) {
        // the assumption is that the system is in bad state and this will most likely cause it shutdown. That's why
        // we do nothing about the exception, but just in case it doesn't we want to at least get it in the log in case
        // some of those queries get stuck
        logger.error("Throwable exception was thrown in nodesUnregistered, this queries may get stuck {}",
          queryId, e);
        throw e;
      }
    }

    @Override
    public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
      // nothing to do.
    }

  }

  /*
   * This assumes that the FragmentStatusListener implementation takes action when it hears
   * that the target fragment has acknowledged the signal. As a result, this listener doesn't do anything
   * but log messages.
   */
  private static class SignalListener implements StreamObserver<Empty> {
    @Override
    public void onNext(Empty empty) {

    }

    @Override
    public void onError(Throwable throwable) {
      final String endpointIdentity = endpoint != null ?
              endpoint.getAddress() + ":" + endpoint.getUserPort() : "<null>";
      logger.error("Failure while attempting to {} fragments of query {} on endpoint {} with {}.",
              signal, QueryIdHelper.getQueryId(value.getQueryId()), endpointIdentity, throwable);
    }

    @Override
    public void onCompleted() {

    }

    /**
     * An enum of possible signals that {@link SignalListener} listens to.
     */
    public enum Signal {
      CANCEL, UNPAUSE
    }

    private final Signal signal;
    private final NodeEndpoint endpoint;
    private final CancelFragments value;

    SignalListener(final NodeEndpoint endpoint, CancelFragments fragments, final Signal signal) {
      this.signal = signal;
      this.endpoint = endpoint;
      this.value = fragments;
    }
  }
}
