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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.util.DremioVersionUtils;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Product implementation of ExecutorSetService.
 */
public class LocalExecutorSetService implements ExecutorSetService {
  private final Provider<ClusterCoordinator> coordinator;
  private final Provider<OptionManager> optionManagerProvider;
  private ListenableSet executorSet = null;
  private boolean isVersionCheckEnabled = true;

  public LocalExecutorSetService(Provider<ClusterCoordinator> coordinator,
                                 Provider<OptionManager> optionManagerProvider) {
    this.coordinator = coordinator;
    this.optionManagerProvider = optionManagerProvider;
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized ListenableSet getExecutorSet(EngineManagementProtos.EngineId engineId,
                                                   EngineManagementProtos.SubEngineId subEngineId) {
    // executorSet can be assigned in start(), assuming ClusterCoordinator
    // (specially ZKClusterCoordinator) starts before this class starts.
    // But going with "assume the least" principle, assigning executorSet here.
    if (executorSet == null) {
      ServiceSet serviceSet = coordinator.get().getServiceSet(Role.EXECUTOR);
      if (serviceSet != null) {
        // Read dremio check option at first call to this method.
        // If dremio check option is changed, coordinator needs to be restarted.
        isVersionCheckEnabled = optionManagerProvider.get().getOption(ExecutorSetService.DREMIO_VERSION_CHECK);
        executorSet = isVersionCheckEnabled ? filterCompatibleExecutors(serviceSet) : serviceSet;
      }
    }
    return executorSet;
  }

  @Override
  public Collection<NodeEndpoint> getAllAvailableEndpoints() {
    if(executorSet != null) {
      return executorSet.getAvailableEndpoints();
    }
    return Lists.newArrayList();
  }

  /**
   * Returns a new ExecutorSelectionHandle backed by given delegate
   * but with executors excluded which are running incompatible version of dremio
   * compared to version of coordinator.
   * @param delegate
   * @return
   */
  private ListenableSet filterCompatibleExecutors(ListenableSet delegate) {
    return new ListenableSet() {
      private Map<NodeStatusListener, NodeStatusListener> innerToForwarderMap =  new ConcurrentHashMap<>();

      @Override
      public Collection<NodeEndpoint> getAvailableEndpoints() {
        return DremioVersionUtils.getCompatibleNodeEndpoints(delegate.getAvailableEndpoints());
      }

      @Override
      public void addNodeStatusListener(NodeStatusListener inner) {
        NodeStatusListener forwarder = new NodeStatusListener() {
          @Override
          public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
            inner.nodesUnregistered(Sets.newHashSet(DremioVersionUtils.getCompatibleNodeEndpoints(unregisteredNodes)));
          }

          @Override
          public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
            inner.nodesRegistered(Sets.newHashSet(DremioVersionUtils.getCompatibleNodeEndpoints(registeredNodes)));
          }
        };
        delegate.addNodeStatusListener(forwarder);
        innerToForwarderMap.put(inner, forwarder);
      }

      @Override
      public void removeNodeStatusListener(NodeStatusListener inner) {
        NodeStatusListener forwarder = innerToForwarderMap.remove(inner);
        if (forwarder != null) {
          delegate.removeNodeStatusListener(forwarder);
        }
      }
    };
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public Map<EngineManagementProtos.SubEngineId, List<NodeEndpoint>> listAllEnginesExecutors() {
    if(executorSet != null) {
      Map<EngineManagementProtos.SubEngineId, List<CoordinationProtos.NodeEndpoint>> executorsGroupedByReplica =
        executorSet.getAvailableEndpoints().stream().collect(Collectors.groupingBy(w -> w.getSubEngineId()));
      return executorsGroupedByReplica;
    }
    return Collections.emptyMap();
  }
}
