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
package com.dremio.service.coordinator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.base.Joiner;

/**
 * Generic Listener on Service Task Leader
 * will always provide leader for the service
 */
public class TaskLeaderStatusListener implements NodeStatusListener, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TaskLeaderStatusListener.class);

  private final Provider<ClusterCoordinator> clusterCoordinator;
  private final String taskName;

  private final Object taskLeaderLock = new Object();
  private volatile CoordinationProtos.NodeEndpoint taskLeaderNode = null;
  private volatile boolean taskLeaderUp;
  private volatile boolean shutdown = false;
  private CoordinatorLostHandle leaderUnregisteredHandle;

  public TaskLeaderStatusListener(String taskName,
                                  Provider<ClusterCoordinator> clusterCoordinator) {
    this(taskName, clusterCoordinator, false, null);
  }

  public TaskLeaderStatusListener(String taskName,
                                  Provider<ClusterCoordinator> clusterCoordinator, boolean isLeader, CoordinatorLostHandle leaderUnregisteredHandle) {
    this.taskName = taskName;
    this.clusterCoordinator = clusterCoordinator;
    this.taskLeaderUp = isLeader;
    this.leaderUnregisteredHandle = leaderUnregisteredHandle;
    if (!taskLeaderUp && leaderUnregisteredHandle != null) {
      leaderUnregisteredHandle.handleMasterDown(this);
    }
  }

  public boolean isTaskLeaderUp() {
    return taskLeaderUp;
  }

  public void start() throws Exception {
    logger.info("Starting TaskLeaderStatusListener for: {}", taskName);
    clusterCoordinator.get().getOrCreateServiceSet(taskName).addNodeStatusListener(this);
    nodesRegistered(new HashSet<>(clusterCoordinator.get().getOrCreateServiceSet(taskName)
      .getAvailableEndpoints()));
    logger.info("TaskLeaderStatusListener for: {} is up", taskName);
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping TaskLeaderStatusListener for: {}", taskName);
    shutdown = true;
    synchronized (taskLeaderLock) {
      taskLeaderLock.notifyAll();
    }
    clusterCoordinator.get().getOrCreateServiceSet(taskName).removeNodeStatusListener(this);
    logger.info("Stopped TaskLeaderStatusListener for: {}", taskName);
  }

  @Override
  public void nodesUnregistered(Set<CoordinationProtos.NodeEndpoint> unregisteredNodes) {
    synchronized(taskLeaderLock) {
      if (taskLeaderNode == null) {
        logger.warn("Receiving unregistration notice for {}, but no TaskLeader for {} was registered", Joiner.on(",")
          .join(unregisteredNodes.stream()
            .map(input -> String.format("%s:%d", input.getAddress(), input.getFabricPort())).collect(Collectors.toList())),
          taskName);
        return;
      }

      if (unregisteredNodes.contains(taskLeaderNode)) {
        taskLeaderNode = null;
        taskLeaderUp = false;
        taskLeaderLock.notifyAll();
        if (leaderUnregisteredHandle != null) {
          leaderUnregisteredHandle.handleMasterDown(this);
        }
      }
    }
  }

  public CoordinationProtos.NodeEndpoint getTaskLeaderNode() {
    return taskLeaderNode;
  }

  @Override
  public void nodesRegistered(Set<CoordinationProtos.NodeEndpoint> registeredNodes) {
    Iterator<CoordinationProtos.NodeEndpoint> iterator = registeredNodes.iterator();
    if (!iterator.hasNext()) {
      logger.warn("Received empty node registration for {}", taskName);
      return;
    }

    CoordinationProtos.NodeEndpoint endpoint = iterator.next();
    synchronized(taskLeaderLock) {
      if (taskLeaderNode != null && !taskLeaderNode.equals(endpoint)) {
        logger.info("Leader for task {} for node changed. Previous was {}:{}, new is {}:{}", taskName,
          taskLeaderNode.getAddress(),
          taskLeaderNode.getFabricPort(), endpoint.getAddress(), endpoint.getFabricPort());
      } else {
        logger.info("New Leader node for task {} {}:{} registered itself.", taskName,
          endpoint.getAddress(), endpoint.getFabricPort());
      }
      taskLeaderNode = endpoint;
      taskLeaderUp = true;
      taskLeaderLock.notifyAll();
    }
  }


  public void waitForTaskLeader() throws InterruptedException {
    long waitTimeInSecs = 0;
    while (!shutdown && !isTaskLeaderUp()) {
      if (waitTimeInSecs % 60 == 0) { // log once every minute
        logger.info("Waiting for leader for task {}", taskName);
      }
      waitTimeInSecs += 5;
      synchronized (taskLeaderLock) {
        taskLeaderLock.wait(5000);
      }
    }
  }
}
