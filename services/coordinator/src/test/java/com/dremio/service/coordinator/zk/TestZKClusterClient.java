/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Provider;

import org.apache.zookeeper.data.Stat;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.test.DremioTest;
import com.typesafe.config.ConfigValueFactory;

/**
 * Test for {@code TestZKClusterClient}
 */
public class TestZKClusterClient extends DremioTest {

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

  @Test
  public void testElection() throws Exception {
    final CountDownLatch firstElection = new CountDownLatch(1);
    final CountDownLatch secondElection = new CountDownLatch(1);
    final AtomicBoolean join1 = new AtomicBoolean(false);
    final AtomicBoolean join2 = new AtomicBoolean(false);


    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG,
        String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ElectionRegistrationHandle node1 = client.joinElection("test-election", new ElectionListener() {

        @Override
        public void onElected() {
          join1.set(true);
          if (firstElection.getCount() == 0) {
            secondElection.countDown();
          } else {
            firstElection.countDown();
          }
        }

        @Override
        public void onCancelled() {
        }
      });

      ElectionRegistrationHandle node2 = client.joinElection("test-election", new ElectionListener() {
        @Override
        public void onElected() {
          join2.set(true);
          if (firstElection.getCount() == 0) {
            secondElection.countDown();
          } else {
            firstElection.countDown();
          }
        }

        @Override
        public void onCancelled() {
        }
      });

      assertTrue(firstElection.await(5, TimeUnit.SECONDS));
      assertTrue("Both nodes were elected master (or no election happened)", join1.get() ^ join2.get());

      // Confirming that the second node is taking over when the first node leaves the election
      if (join1.get()) {
        node1.close();
      } else {
        node2.close();
      }

      assertTrue(secondElection.await(5, TimeUnit.SECONDS));
      assertTrue("Second node didn't get elected", join1.get() && join2.get());
    }
  }

  @Test
  public void testElectionDisconnection() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch cancelled = new CountDownLatch(1);

    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG
        .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("20ms"))
        .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("100ms")),
        String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ElectionRegistrationHandle node1 = client.joinElection("test-election", new ElectionListener() {

        @Override
        public void onElected() {
          elected.countDown();
        }

        @Override
        public void onCancelled() {
          cancelled.countDown();
        }
      });

      assertTrue("No election happened", elected.await(5, TimeUnit.SECONDS));

      // Kill the server to force disconnection
      zooKeeperServer.closeServer();

      assertTrue("Node was not notified about cancellation", cancelled.await(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testElectionSuspended() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch cancelled = new CountDownLatch(1);
    final CountDownLatch loss = new CountDownLatch(1);
    final CountDownLatch reconnected = new CountDownLatch(1);

    try(ZKClusterClient client = new ZKClusterClient(
        DEFAULT_SABOT_CONFIG
        .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("250ms"))
        .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("5s")),
        String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
        ) {
      client.start();
      ElectionRegistrationHandle node1 = client.joinElection("test-election", new ZKElectionListener() {

        @Override
        public void onElected() {
          elected.countDown();
        }

        @Override
        public void onCancelled() {
          cancelled.countDown();
        }

        @Override
        public void onConnectionLoss() {
          loss.countDown();
        }

        @Override
        public void onReconnection() {
          reconnected.countDown();
        }
      });

      assertTrue("No election happened", elected.await(5, TimeUnit.SECONDS));

      // Restart the server
      zooKeeperServer.restartServer();

      assertTrue("Node was not disconnected", loss.await(5, TimeUnit.SECONDS));
      assertTrue("Node was not reconnected", reconnected.await(5, TimeUnit.SECONDS));
      assertEquals("Node was cancelled", 1L, cancelled.getCount());
    }
  }
}
