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
package com.dremio.service.execselector;

import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * An implementation of the {@link ExecutorSelectionService}.
 *
 * This class:
 * - Maintains a current {@link ExecutorSelector}, and allows the switch to different executor selectors
 *   based on a system option
 * - Registers a listener to the cluster coordinator, and receives callbacks whenever executors are added to
 *   or removed from the cluster
 * - Maintains a read/write lock that guards the internal state of executor selection:
 *   - read lock obtained to get the list of executors, and in general, for non-destructive calls
 *   - write lock obtained to add/remove nodes from the node membership, and in general, for destructive calls
 *   Please note: because the executor selector might provide a separate entry point for callers, we pass the
 *                read/write lock to the selector factory, allowing these separate entry points to be thread-safe.
 *
 * The presence of the read/write lock poses an interesting restriction: the code needs to take a write lock to
 * process node addition/removal, but node addition/removal happens within a callback from an RPC that deals with
 * node membership -- not an event that we'd want to block while waiting for a w-lock.
 * The problem is resolved by the presence of a queue, and a queue processor, which processes all events in the
 * queue in a separate thread. Thus, the RPC callback enqueues an event (an {@link EndpointEvent}, below), and
 * the separate thread takes the w-lock and processes the actual node addition/removal.
 */
public class ExecutorSelectionServiceImpl implements ExecutorSelectionService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecutorSelectionServiceImpl.class);

  /**
   * Signifies addition or removal of executors in the events in the event queue
   */
  enum EndpointAction {
    ADD,
    REMOVE
  }
  private class EndpointEvent {
    private final EndpointAction action;
    private final Set<NodeEndpoint> endpointSet;
    EndpointEvent(EndpointAction action, Set<NodeEndpoint> endpointSet) {
      this.action = action;
      this.endpointSet = endpointSet;
    }
    EndpointAction getAction() {
      return action;
    }
    Set<NodeEndpoint> getEndpointSet() {
      return endpointSet;
    }
  }

  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<OptionManager> optionsProvider;
  private final Provider<ExecutorSelectorFactory> factoryProvider;
  private final ExecutorSelectorProvider executorSelectorProvider;
  private final ReentrantReadWriteLock rwLock;

  // State, modified under a w-lock, but accessed under a r-lock
  private ExecutorSelector selector;
  private String selectorType;

  // Event queue. Insertions happen without taking either r-lock or w-lock. Removals happen in a separate thread
  private final QueueProcessor<EndpointEvent> eventQueue;

  // Listener for updates from the cluster coordinator
  private final NodeStatusListener nodeStatusListener;

  public ExecutorSelectionServiceImpl(
      final Provider<ClusterCoordinator> clusterCoordinatorProvider,
      final Provider<OptionManager> optionsProvider,
      final Provider<ExecutorSelectorFactory> factoryProvider,
      final ExecutorSelectorProvider executorSelectorProvider
    ) {
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.optionsProvider = optionsProvider;
    this.factoryProvider = factoryProvider;
    this.executorSelectorProvider = executorSelectorProvider;
    this.rwLock = new ReentrantReadWriteLock();
    this.selector = null; // instantiated at startup
    this.eventQueue = new QueueProcessor<>("executor-selection-service-queue",
      () -> new AutoCloseableLock(rwLock.writeLock()).open(),
      this::processEndpointEvent);
    this.nodeStatusListener = new NodeStatusListener() {
      @Override
      public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
        eventQueue.enqueue(new EndpointEvent(EndpointAction.REMOVE, unregisteredNodes));
      }
      @Override
      public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
        eventQueue.enqueue(new EndpointEvent(EndpointAction.ADD, registeredNodes));
      }
    };
  }

  private void processEndpointEvent(EndpointEvent event) {
    // NB: invoked under a w-lock that's already taken by the eventQueue
    if (event.getAction() == EndpointAction.ADD) {
      selector.nodesRegistered(event.getEndpointSet());
    } else {
      assert event.getAction() == EndpointAction.REMOVE;
      selector.nodesUnregistered(event.getEndpointSet());
    }
  }

  @Override
  public void start() {
    Preconditions.checkState(selector == null, "Service was already started");
    try (final AutoCloseableLock ignored = new AutoCloseableLock(rwLock.writeLock()).open()) {
      // NB: even though it's startup, holding the w-lock so we don't have to depend on the order of initialization
      //     between the cluster coordinator and the execution selection service
      createSelector();
    }
    eventQueue.start();
    clusterCoordinatorProvider.get()
        .getServiceSet(ClusterCoordinator.Role.EXECUTOR)
        .addNodeStatusListener(nodeStatusListener);
  }

  @Override
  public void close() throws Exception {
    clusterCoordinatorProvider.get()
        .getServiceSet(ClusterCoordinator.Role.EXECUTOR)
        .removeNodeStatusListener(nodeStatusListener);
    eventQueue.close();
    closeSelector();
  }

  @Override
  public ExecutorSelectionHandle getExecutors(int desiredNumExecutors) {
    while (true) {
      try (final AutoCloseableLock ignored = new AutoCloseableLock(rwLock.readLock()).open()) {
        if (optionsProvider.get().getOption(EXECUTOR_SELECTION_TYPE).equals(selectorType)) {
          // Common case: option stayed the same
          return selector.getExecutors(desiredNumExecutors);
        }
      }

      // selection type changed. Expensive path
      createSelectorIfNecessary();
    }
  }

  /**
   * check whether the system options have changed, and if there is a necessity to
   * create a selector, do so
   */
  @VisibleForTesting
  public void createSelectorIfNecessary() {
    try (final AutoCloseableLock ignored = new AutoCloseableLock(rwLock.writeLock()).open()) {
      if (!optionsProvider.get().getOption(EXECUTOR_SELECTION_TYPE).equals(selectorType)) {
        createSelector();
      }
    }
  }

  @VisibleForTesting
  int getNumExecutors() {
    try (final AutoCloseableLock ignored = new AutoCloseableLock(rwLock.readLock()).open()) {
      if (selector != null) {
        return selector.getNumExecutors();
      }
      return 0;
    }
  }

  /**
   * Create the selector. Assumes that caller holds the w-lock
   */
  private void createSelector() {
    String selectorType = optionsProvider.get().getOption(EXECUTOR_SELECTION_TYPE);
    logger.debug("Creating executor selector of type {}", selectorType);
    ExecutorSelector selector = factoryProvider.get().createExecutorSelector(selectorType, rwLock);
    if (this.selector != null) {
      try {
        this.selector.close();
      } catch (Exception e) {
        logger.warn("Exception while attempting to close the executor selector", e);
        // Safe to swallow the exception. The old executor selector is going away
      }
    }
    this.selectorType = selectorType;
    this.selector = selector;
    // Please note: The cluster coordinator might be sending updates between the previous line (change of selector)
    //              and the next line (apply current endpoint set to the new selector)
    //              While this changes the order in which the events apply, the end result is the same.
    //              For example, if a node is added during this gap, the event queue will contain the node add event,
    //              and the state below will also include the node. The node add will end up being processed twice.
    //              However, according to the guarantees in the ExecutorSelector interface, having repeated events
    //              is expected
    Set<NodeEndpoint> currentNodes = ImmutableSet.copyOf(clusterCoordinatorProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR).getAvailableEndpoints());
    selector.nodesRegistered(currentNodes);
    executorSelectorProvider.setCurrentSelector(selector);
  }

  private void closeSelector() throws Exception {
    try (final AutoCloseableLock ignored = new AutoCloseableLock(rwLock.writeLock()).open()) {
      AutoCloseables.close(selector);
      selector = null;
    }
  }
}
