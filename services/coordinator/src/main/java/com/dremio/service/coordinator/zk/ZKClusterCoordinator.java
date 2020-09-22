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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages cluster coordination utilizing zookeeper.
 */
public class ZKClusterCoordinator extends ClusterCoordinator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterCoordinator.class);

  private static enum Service {
    COORDINATOR(Role.COORDINATOR, "coordinator"), EXECUTOR(Role.EXECUTOR, "executor"), MASTER(Role.MASTER, "master");

    private final ClusterCoordinator.Role role;
    private final String name;
    private Service(ClusterCoordinator.Role role, String serviceName) {
      this.role = role;
      this.name = serviceName;
    }
  }

  private final ZKClusterClient zkClient;
  private final ConcurrentMap<String, ZKServiceSet> serviceSets = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  public ZKClusterCoordinator(SabotConfig config) throws IOException{
    this(config, (String) null);
  }

  public ZKClusterCoordinator(SabotConfig config, String connect) throws IOException {
    this.zkClient = new ZKClusterClient(config, connect);
  }

  public ZKClusterCoordinator(SabotConfig config, Provider<Integer> localPort) throws IOException {
    this.zkClient = new ZKClusterClient(config, localPort);
  }

  @VisibleForTesting
  ZKClusterClient getZkClient() {
    return zkClient;
  }

  @VisibleForTesting
  public void setPortProvider(Provider<Integer> portProvider) {
    this.zkClient.setPortProvider(portProvider);
  }

  @Override
  public void start() throws Exception {
    zkClient.start();

    if (!closed) {
      Thread.sleep(5);
      for(Service service: Service.values()) {
        ZKServiceSet serviceSet = zkClient.newServiceSet(service.name);
        serviceSet.start();
        serviceSets.put(service.role.name(), serviceSet);
      }
      logger.info("ZKClusterCoordination is up");
    }
  }

  @Override
  public ServiceSet getServiceSet(final Role role) {
    return serviceSets.get(role.name());
  }

  @Override
  public ServiceSet getOrCreateServiceSet(final String serviceName) {
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

  // this interface doesn't guarantee the consistency of the registered service names.
  @Override
  public Iterable<String> getServiceNames() throws Exception {
    return zkClient.getServiceNames();
  }

  @Override
  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return zkClient.getSemaphore(name, maximumLeases);
  }

  @Override
  public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
    return zkClient.joinElection(name, listener);
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      AutoCloseables.close(serviceSets.values(), AutoCloseables.iter(zkClient));
    }
  }

}
