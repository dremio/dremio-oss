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
package com.dremio.exec.server;

import java.util.Collection;
import java.util.Set;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.ServiceSet;

public class ClusterResourceInformation {

  private volatile long averageExecutorMemory = 0;

  public ClusterResourceInformation(final ClusterCoordinator coordinator) {
    final ServiceSet executorSet = coordinator.getServiceSet(ClusterCoordinator.Role.EXECUTOR);
    executorSet.addNodeStatusListener(new NodeStatusListener() {
      @Override
      public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
        computeAverageMemory(executorSet.getAvailableEndpoints());
      }

      @Override
      public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
        computeAverageMemory(executorSet.getAvailableEndpoints());
      }
    });
    computeAverageMemory(executorSet.getAvailableEndpoints());
  }

  private void computeAverageMemory(final Collection<NodeEndpoint> executors) {
    if (executors == null || executors.isEmpty()) {
      averageExecutorMemory = 0;
    } else {
      long totalDirectMemory = 0;
      for (final NodeEndpoint endpoint : executors) {
        totalDirectMemory += endpoint.getMaxDirectMemory();
      }

      averageExecutorMemory = totalDirectMemory / executors.size();
    }
  }

  /**
   * Get the average maximum direct memory of executors in the cluster.
   *
   * @return average maximum direct memory of executors
   */
  public long getAverageExecutorMemory() {
    return averageExecutorMemory;
  }
}
