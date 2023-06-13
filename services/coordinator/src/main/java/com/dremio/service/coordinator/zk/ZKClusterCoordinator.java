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

  private final ZKClusterServiceSetManager zkClusterServiceSetManager;

  private volatile boolean closed = false;

  public ZKClusterCoordinator(SabotConfig config) throws IOException{
    this(config, (String) null);
  }

  public ZKClusterCoordinator(SabotConfig config, String connect) throws IOException {
    this(new ZKSabotConfig(config), connect);
  }

  public ZKClusterCoordinator(SabotConfig config, Provider<Integer> localPort) throws IOException {
    this.zkClusterServiceSetManager = new ZKClusterServiceSetManager(new ZKSabotConfig(config), localPort);
  }

  public ZKClusterCoordinator(ZKClusterConfig zkClusterConfig, Provider<Integer> localPort) throws IOException {
    this.zkClusterServiceSetManager = new ZKClusterServiceSetManager(zkClusterConfig, localPort);
  }

  public ZKClusterCoordinator(ZKClusterConfig zkClusterConfig, String connect) throws IOException {
    this.zkClusterServiceSetManager = new ZKClusterServiceSetManager(zkClusterConfig, connect);
  }

  @VisibleForTesting
  ZKClusterClient getZkClient() {
    return zkClusterServiceSetManager.getZkClient();
  }

  @VisibleForTesting
  public void setPortProvider(Provider<Integer> portProvider) {
    this.zkClusterServiceSetManager.getZkClient().setPortProvider(portProvider);
  }

  @Override
  public void start() throws Exception {
    zkClusterServiceSetManager.start();

    if (!closed) {
      Thread.sleep(5);
      for(Service service: Service.values()) {
        zkClusterServiceSetManager.getOrCreateServiceSet(service.role.name(), service.name);
      }
      logger.info("ZKClusterCoordination is up");
    }
  }

  @Override
  public ServiceSet getServiceSet(final Role role) {
    return zkClusterServiceSetManager.getServiceSet(role);
  }

  @Override
  public ServiceSet getOrCreateServiceSet(final String serviceName) {
    return zkClusterServiceSetManager.getOrCreateServiceSet(serviceName);
  }

  @Override
  public void deleteServiceSet(String serviceName) {
    zkClusterServiceSetManager.deleteServiceSet(serviceName);
  }

  // this interface doesn't guarantee the consistency of the registered service names.
  @Override
  public Iterable<String> getServiceNames() throws Exception {
    return zkClusterServiceSetManager.getServiceNames();
  }

  @Override
  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return zkClusterServiceSetManager.getZkClient().getSemaphore(name, maximumLeases);
  }

  @Override
  public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
    return zkClusterServiceSetManager.joinElection(name, listener);
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      AutoCloseables.close(zkClusterServiceSetManager);
    }
  }
}
