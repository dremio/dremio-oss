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

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterElectionManager;
import com.dremio.service.coordinator.ClusterServiceSetManager;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.Maps;

/**
 * ZK implementation for ClusterServiceSetManager
 */
public class ZKClusterServiceSetManager implements ClusterServiceSetManager, ClusterElectionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterServiceSetManager.class);
  private final ConcurrentMap<String, ZKServiceSet> serviceSets = Maps.newConcurrentMap();

  private final ZKClusterClient zkClient;

  public ZKClusterServiceSetManager(ZKClusterConfig config) throws IOException {
    this(config, (String) null);
  }

  public ZKClusterServiceSetManager(ZKClusterConfig config, String connect) throws IOException {
    this.zkClient = new ZKClusterClient(config, connect);
  }

  public ZKClusterServiceSetManager(ZKClusterConfig config, Provider<Integer> localPort) throws IOException {
    this.zkClient = new ZKClusterClient(config, localPort);
  }

  @Override
  public void start() throws Exception {
    zkClient.start();
  }

  @Override
  public ServiceSet getServiceSet(ClusterCoordinator.Role role) {
    return serviceSets.get(role.name());
  }

  @Override
  public ServiceSet getOrCreateServiceSet(String serviceName) {
    return serviceSets.computeIfAbsent(serviceName, s -> {
      final ZKServiceSet newServiceSet = zkClient.newServiceSet(serviceName);
      try {
        newServiceSet.start();
      } catch (Exception e) {
        throw new RuntimeException(String.format("Unable to start %s service in Zookeeper", serviceName), e);
      }
      return newServiceSet;
    });
  }

  public ServiceSet getOrCreateServiceSet(String role, String serviceName) {
    return serviceSets.computeIfAbsent(role, s -> {
      final ZKServiceSet newServiceSet = zkClient.newServiceSet(serviceName);
      try {
        newServiceSet.start();
      } catch (Exception e) {
        throw new RuntimeException(String.format("Unable to start %s service in Zookeeper", serviceName), e);
      }
      return newServiceSet;
    });
  }

  @Override
  public Iterable<String> getServiceNames() throws Exception {
    return zkClient.getServiceNames();
  }

  @Override
  public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
    return zkClient.joinElection(name, listener);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(serviceSets.values(), AutoCloseables.iter(zkClient));
  }

  public ZKClusterClient getZkClient() {
    return zkClient;
  }
}
