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
package com.dremio.dac.service.exec;

import java.util.Set;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.TaskLeaderStatusListener;

/**
 * Maintains status of master node using zookeeper.
 */
public class MasterStatusListener implements NodeStatusListener, Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MasterStatusListener.class);

  private TaskLeaderStatusListener taskLeaderStatusListener;
  public MasterStatusListener(Provider<ClusterCoordinator> clusterCoordinator, boolean isMaster) {
    taskLeaderStatusListener = new TaskLeaderStatusListener(ClusterCoordinator.Role.MASTER.name(), clusterCoordinator, isMaster);
  }

  public CoordinationProtos.NodeEndpoint getMasterNode() {
    return taskLeaderStatusListener.getTaskLeaderNode();
  }

  public boolean isMasterUp() {
    return taskLeaderStatusListener.isTaskLeaderUp();
  }

  public void waitForMaster() throws InterruptedException {
    taskLeaderStatusListener.waitForTaskLeader();
  }

  @Override
  public void start() throws Exception {
    taskLeaderStatusListener.start();
  }

  @Override
  public void nodesUnregistered(Set<CoordinationProtos.NodeEndpoint> unregisteredNodes) {
    taskLeaderStatusListener.nodesUnregistered(unregisteredNodes);
  }

  @Override
  public void nodesRegistered(Set<CoordinationProtos.NodeEndpoint> registeredNodes) {
    taskLeaderStatusListener.nodesRegistered(registeredNodes);
  }

  @Override
  public void close() throws Exception {
    taskLeaderStatusListener.close();
  }
}
