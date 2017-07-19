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
package com.dremio.service.coordinator.zk;

import static com.dremio.service.coordinator.ClusterCoordinator.Options.CLUSTER_ID;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ROOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import javax.inject.Provider;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.test.DremioTest;
import com.google.common.base.Throwables;
import com.typesafe.config.ConfigValueFactory;

/**
 * Test for {@code TestZKClusterClient}
 */
public class TestZKClusterClient extends DremioTest {

  private static final class ZooKeeperServerResource extends ExternalResource {
    private TestingServer testingServer;
    private CuratorZookeeperClient zkClient;

    @Override
    protected void before() throws Throwable {
      testingServer = new TestingServer(true);
      zkClient = new CuratorZookeeperClient(testingServer.getConnectString(), 5000, 5000, null, new RetryOneTime(1000));
      zkClient.start();
      zkClient.blockUntilConnectedOrTimedOut();
    }

    @Override
    protected void after() {
      try {
        zkClient.close();
        testingServer.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    public ZooKeeper getZKClient() throws Exception {
      return zkClient.getZooKeeper();
    }

    public int getPort() {
      return testingServer.getPort();
    }

    public String getConnectString() {
      return testingServer.getConnectString();
    }
  }

  @Rule
  public final ZooKeeperServerResource zooKeeperServer = new ZooKeeperServerResource();

  @Test
  public void testDefaultConnection() throws Exception {
    // Default root from sabot-module.conf
    assertNull(zooKeeperServer.getZKClient().exists("/dremio/test-path", false));

    final SabotConfig config = DEFAULT_SABOT_CONFIG
        .withValue(ZK_ROOT, ConfigValueFactory.fromAnyRef("dremio/test-path"))
        .withValue(CLUSTER_ID, ConfigValueFactory.fromAnyRef("test-cluster-id"));

    try(ZKClusterClient client = new ZKClusterClient(config, new Provider<Integer>() {
      @Override
      public Integer get() {
        return zooKeeperServer.getPort();
      }
    })) {
      client.start();
      ZKServiceSet serviceSet = client.newServiceSet("coordinator");
      serviceSet.register(NodeEndpoint.newBuilder().setAddress("foo").build());


      Stat stat = zooKeeperServer.getZKClient().exists("/dremio/test-path/test-cluster-id/coordinator", false);
      assertNotNull(stat);
      assertEquals(1, stat.getNumChildren());
    }
  }

  @Test
  public void test1ComponentConnection() throws Exception {
    assertNull(zooKeeperServer.getZKClient().exists("/dremio1", false));

    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG,
        String.format("%s/dremio1", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ZKServiceSet serviceSet = client.newServiceSet("coordinator");
      serviceSet.register(NodeEndpoint.newBuilder().setAddress("foo").build());


      Stat stat = zooKeeperServer.getZKClient().exists("/dremio1/coordinator", false);
      assertNotNull(stat);
      assertEquals(1, stat.getNumChildren());
    }
  }

  @Test
  public void test2ComponentsConnection() throws Exception {
    assertNull(zooKeeperServer.getZKClient().exists("/dremio2/test-cluster-id", false));

    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG,
        String.format("%s/dremio2/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ZKServiceSet serviceSet = client.newServiceSet("coordinator");
      serviceSet.register(NodeEndpoint.newBuilder().setAddress("foo").build());


      Stat stat = zooKeeperServer.getZKClient().exists("/dremio2/test-cluster-id/coordinator", false);
      assertNotNull(stat);
      assertEquals(1, stat.getNumChildren());
    }
  }

  @Test
  public void test3ComponentsConnection() throws Exception {
    assertNull(zooKeeperServer.getZKClient().exists("/dremio3/test/test-cluster-id", false));

    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG,
        String.format("%s/dremio3/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ZKServiceSet serviceSet = client.newServiceSet("coordinator");
      serviceSet.register(NodeEndpoint.newBuilder().setAddress("foo").build());


      Stat stat = zooKeeperServer.getZKClient().exists("/dremio3/test/test-cluster-id/coordinator", false);
      assertNotNull(stat);
      assertEquals(1, stat.getNumChildren());
    }
  }

}
