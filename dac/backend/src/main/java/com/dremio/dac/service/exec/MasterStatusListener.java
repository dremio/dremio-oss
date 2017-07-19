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
package com.dremio.dac.service.exec;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.google.common.base.Objects;

/**
 * Maintains status of master node using zookeeper.
 */
public class MasterStatusListener implements NodeStatusListener, Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MasterStatusListener.class);
  private final Provider<ClusterCoordinator> clusterCoordinator;
  private final String masterNode;
  private final Object masterLock = new Object();
  private volatile boolean masterUp;
  private volatile boolean shutdown = false;
  private final int masterPort;

  public MasterStatusListener(Provider<ClusterCoordinator> clusterCoordinator,
                              String masterNode, int masterPort, boolean isMaster) {
    this.clusterCoordinator = clusterCoordinator;
    this.masterNode = masterNode;
    this.masterUp = isMaster;
    this.masterPort = masterPort;
  }

  public boolean isMasterUp() {
    return masterUp;
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting MasterStatusListener with master node {}:{}", masterNode, masterPort);
    clusterCoordinator.get().getServiceSet(ClusterCoordinator.Role.COORDINATOR).addNodeStatusListener(this);
    nodesRegistered(new HashSet<>(clusterCoordinator.get().getServiceSet(ClusterCoordinator.Role.COORDINATOR)
        .getAvailableEndpoints()));
    logger.info("MasterStatusListener is up {}:{}", masterNode, masterPort);
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping MasterStatusListener");
    shutdown = true;
    synchronized (masterLock) {
      masterLock.notifyAll();
    }
    logger.info("Stopped MasterStatusListener");
  }

  @Override
  public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
    nodeChanged(unregisteredNodes, false);

  }

  @Override
  public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    nodeChanged(registeredNodes, true);
  }

  public void nodeChanged(Set<NodeEndpoint> unregisteredNodes, boolean isUp) {
    InetAddress masterAddress;
    try {
      // Java has an internal cache for DNS entries, but at some point
      // it should detect if master node address changed.
      masterAddress = InetAddress.getByName(masterNode);
      logger.debug("Master node {} resolves to {}.", masterNode, masterAddress);
    } catch(UnknownHostException e) {
      logger.warn("Not able to resolve master node {}.");
      masterAddress = null;
    }

    for (NodeEndpoint endpoint : unregisteredNodes) {
      InetAddress endpointAddress;
      try {
        endpointAddress = InetAddress.getByName(endpoint.getAddress());
        logger.debug("Remote node {} resolves to {}.", endpoint, endpointAddress);
      } catch(UnknownHostException e) {
        endpointAddress = null;
      }
      if ((Objects.equal(masterAddress, endpointAddress) || masterNode.equals(endpoint.getAddress()))
          && masterPort == endpoint.getFabricPort()) {
        logger.info("master node {} is {}", endpoint, isUp ? "up" : "down");
        synchronized (masterLock) {
          masterUp = isUp;
          masterLock.notifyAll();
        }
        break;
      }
    }
  }



  public void waitForMaster() throws InterruptedException {
    long waitTimeInSecs = 0;
    while (!shutdown && !isMasterUp()) {
      if (waitTimeInSecs % 60 == 0) { // log once every minute
        logger.info("Waiting for master {}:{}", masterNode, masterPort);
      }
      waitTimeInSecs += 5;
      synchronized (masterLock) {
        masterLock.wait(5000);
      }
    }
  }
}
