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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

/**
 * Maintains status of master node using zookeeper.
 */
public class MasterStatusListener implements NodeStatusListener, Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MasterStatusListener.class);
  private final Provider<ClusterCoordinator> clusterCoordinator;
  private final Object masterLock = new Object();
  private volatile NodeEndpoint masterNode = null;
  private volatile boolean masterUp;
  private volatile boolean shutdown = false;

  public MasterStatusListener(Provider<ClusterCoordinator> clusterCoordinator, boolean isMaster) {
    this.clusterCoordinator = clusterCoordinator;
    this.masterUp = isMaster;
  }

  public boolean isMasterUp() {
    return masterUp;
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting MasterStatusListener");
    clusterCoordinator.get().getServiceSet(ClusterCoordinator.Role.MASTER).addNodeStatusListener(this);
    nodesRegistered(new HashSet<>(clusterCoordinator.get().getServiceSet(ClusterCoordinator.Role.MASTER)
        .getAvailableEndpoints()));
    logger.info("MasterStatusListener is up");
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
    synchronized(masterLock) {
      if (masterNode == null) {
        logger.warn("Receiving unregistration notice for {}, but no master was registered", Joiner.on(",").join(Iterables.transform(unregisteredNodes, new Function<NodeEndpoint, String>() {
          @Override
          public String apply(NodeEndpoint input) {
            return String.format("%s:%d", input.getAddress(), input.getFabricPort());
          }
        })));
        return;
      }

      if (unregisteredNodes.contains(masterNode)) {
        masterNode = null;
        masterUp = false;
        masterLock.notifyAll();
      }
    }
  }

  public NodeEndpoint getMasterNode() {
    return masterNode;
  }

  @Override
  public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    Iterator<NodeEndpoint> iterator = registeredNodes.iterator();
    if (!iterator.hasNext()) {
      logger.warn("Received empty node registration");
      return;
    }

    NodeEndpoint endpoint = iterator.next();
    synchronized(masterLock) {
      if (masterNode != null && !masterNode.equals(endpoint)) {
        logger.info("Master node changed. Previous was {}:{}, new is {}:{}", masterNode.getAddress(), masterNode.getFabricPort(), endpoint.getAddress(), endpoint.getFabricPort());
      } else {
        logger.info("New master node {}:{} registered itself.", endpoint.getAddress(), endpoint.getFabricPort());
      }
      masterNode = endpoint;
      masterUp = true;
      masterLock.notifyAll();

    }
  }


  public void waitForMaster() throws InterruptedException {
    long waitTimeInSecs = 0;
    while (!shutdown && !isMasterUp()) {
      if (waitTimeInSecs % 60 == 0) { // log once every minute
        logger.info("Waiting for master");
      }
      waitTimeInSecs += 5;
      synchronized (masterLock) {
        masterLock.wait(5000);
      }
    }
  }
}
