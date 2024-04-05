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
package com.dremio.service.coordinator.zk;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Collections2.transform;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.Service;
import com.dremio.service.coordinator.AbstractServiceSet;
import com.dremio.service.coordinator.RegistrationHandle;
import com.google.common.base.Function;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;

final class ZKServiceSet extends AbstractServiceSet implements Service {
  private final String serviceName;
  private final ServiceDiscovery<NodeEndpoint> discovery;
  private final ServiceCache<NodeEndpoint> serviceCache;

  private volatile Collection<NodeEndpoint> endpoints = Collections.emptyList();

  private final class EndpointListener implements ServiceCacheListener {
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {}

    @Override
    public void cacheChanged() {
      ZKClusterCoordinator.logger.debug("Got cache changed --> updating endpoints");
      updateEndpoints();
    }
  }

  public ZKServiceSet(String serviceName, ServiceDiscovery<NodeEndpoint> discovery) {
    this.serviceName = serviceName;
    this.discovery = discovery;
    this.serviceCache = discovery.serviceCacheBuilder().name(serviceName).build();
  }

  @Override
  public void start() throws Exception {
    serviceCache.start();
    serviceCache.addListener(new EndpointListener());
    updateEndpoints();
  }

  @Override
  public RegistrationHandle register(NodeEndpoint endpoint) {
    try {
      ServiceInstance<NodeEndpoint> serviceInstance = newServiceInstance(serviceName, endpoint);

      discovery.registerService(serviceInstance);

      return new ZKRegistrationHandle(this, serviceInstance);
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  void unregister(ZKRegistrationHandle handle) {
    // do not remove listeners, as they are global per service

    try {
      discovery.unregisterService(handle.getServiceInstance());
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private synchronized void updateEndpoints() {
    try {
      final Collection<NodeEndpoint> newNodesSet = getAvailableEndpoints();

      // set of newly dead nodes : original nodes - new set of active nodes.
      Set<NodeEndpoint> unregisteredNodes = new HashSet<>(endpoints);
      unregisteredNodes.removeAll(newNodesSet);

      Set<NodeEndpoint> newlyRegisteredNodes = new HashSet<>(newNodesSet);
      newlyRegisteredNodes.removeAll(endpoints);

      endpoints = newNodesSet;

      if (ZKClusterCoordinator.logger.isDebugEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Active nodes set changed.  Now includes ");
        builder.append(newNodesSet.size());
        builder.append(" total nodes.  New active nodes: \n");
        for (NodeEndpoint bit : newNodesSet) {
          builder.append('\t');
          builder.append(bit.getAddress());
          builder.append(':');
          builder.append(bit.getUserPort());
          builder.append(':');
          builder.append(bit.getFabricPort());
          builder.append(':');
          builder.append(bit.getStartTime());
          builder.append('\n');
        }
        ZKClusterCoordinator.logger.debug(builder.toString());
      }

      // Notify the nodes listener for newly unregistered nodes. For now, we only care when nodes
      // are down / unregistered.
      if (!unregisteredNodes.isEmpty()) {
        nodesUnregistered(unregisteredNodes);
      }

      if (!newlyRegisteredNodes.isEmpty()) {
        nodesRegistered(newlyRegisteredNodes);
      }

    } catch (Exception e) {
      ZKClusterCoordinator.logger.error(
          "Failure while update SabotNode service location cache.", e);
    }
  }

  private ServiceInstance<NodeEndpoint> newServiceInstance(String name, NodeEndpoint endpoint)
      throws Exception {
    return ServiceInstance.<NodeEndpoint>builder().name(name).payload(endpoint).build();
  }

  @Override
  public Collection<NodeEndpoint> getAvailableEndpoints() {
    return transform(
        serviceCache.getInstances(),
        new Function<ServiceInstance<NodeEndpoint>, NodeEndpoint>() {
          @Override
          public NodeEndpoint apply(ServiceInstance<NodeEndpoint> input) {
            return input.getPayload();
          }
        });
  }

  @Override
  public void close() throws Exception {
    // might be redundant, as serviceCache clears them upon closure
    clearListeners();
    serviceCache.close();
  }
}
