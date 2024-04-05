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
package com.dremio.service.scheduler;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.io.file.Path;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.zk.KillZkSession;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.dremio.test.DremioTest;
import com.google.common.collect.Sets;
import java.util.UUID;
import org.junit.Assert;

/** Simulates a test client. Used for the new {@link ClusteredSingletonTaskScheduler} testing. */
final class TestClient implements AutoCloseable {
  private static final String ROOT_PATH_FORMAT = "%s/dremio/test/clustered-singleton";
  private static final int FABRIC_PORT_START = 1234;
  private static final int USER_PORT_START = 2345;
  private static final String SERVICE_UUID = UUID.randomUUID().toString();
  private static final String SERVICE_NAME = "TestCoordinator";
  private static final String SERVICE_ROOT_PATH =
      Path.SEPARATOR + SERVICE_NAME + Path.SEPARATOR + SERVICE_UUID;
  private final ClusterCoordinator clusterCoordinator;
  private final CoordinationProtos.NodeEndpoint endpoint;
  private final ClusteredSingletonTaskScheduler singletonScheduler;

  TestClient(int clientNum, String connectString) {
    this(clientNum, connectString, "test-version");
  }

  TestClient(int clientNum, String connectString, String dremioVersion) {
    try {
      clusterCoordinator =
          new ZKClusterCoordinator(
              DremioTest.DEFAULT_SABOT_CONFIG, String.format(ROOT_PATH_FORMAT, connectString));
      clusterCoordinator.start();
      endpoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host" + clientNum)
              .setFabricPort(FABRIC_PORT_START + clientNum)
              .setUserPort(USER_PORT_START + clientNum)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .setDremioVersion(dremioVersion)
              .build();
      final ScheduleTaskGroup defaultConfig = ScheduleTaskGroup.create("scheduler", 10);
      singletonScheduler =
          new ClusteredSingletonTaskScheduler(
              defaultConfig,
              SERVICE_ROOT_PATH,
              DirectProvider.wrap(clusterCoordinator),
              DirectProvider.wrap(endpoint),
              1);
      singletonScheduler.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(singletonScheduler, clusterCoordinator);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  ClusterCoordinator getClusterCoordinator() {
    return clusterCoordinator;
  }

  CoordinationProtos.NodeEndpoint getEndpoint() {
    return endpoint;
  }

  public ClusteredSingletonTaskScheduler getSingletonScheduler() {
    return singletonScheduler;
  }

  public void injectSessionExpiration() throws Exception {
    if (clusterCoordinator instanceof ZKClusterCoordinator) {
      KillZkSession.injectSessionExpiration((ZKClusterCoordinator) clusterCoordinator);
    }
  }

  protected static class TestGroup implements ScheduleTaskGroup {
    private final int capacity;

    protected TestGroup(int capacity) {
      this.capacity = capacity;
    }

    @Override
    public String getGroupName() {
      return "test-group";
    }

    @Override
    public int getCapacity() {
      return capacity;
    }
  }
}
