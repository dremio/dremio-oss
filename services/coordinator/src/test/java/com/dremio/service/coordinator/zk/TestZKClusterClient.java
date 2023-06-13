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

import static com.dremio.service.coordinator.ClusterCoordinator.Options.CLUSTER_ID;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ROOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Provider;

import org.apache.zookeeper.data.Stat;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.test.DremioTest;
import com.typesafe.config.ConfigValueFactory;

/**
 * Test for {@code TestZKClusterClient}
 */
public class TestZKClusterClient extends DremioTest {

  private static final ZKClusterConfig DEFAULT_ZK_CLUSTER_CONFIG = new ZKSabotConfig(DEFAULT_SABOT_CONFIG);

  @Rule
  public final ZooKeeperServerResource zooKeeperServer = new ZooKeeperServerResource();

  @Test
  public void testDefaultConnection() throws Exception {
    // Default root from sabot-module.conf
    assertNull(zooKeeperServer.getZKClient().exists("/dremio/test-path", false));

    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
        .withValue(ZK_ROOT, ConfigValueFactory.fromAnyRef("dremio/test-path"))
        .withValue(CLUSTER_ID, ConfigValueFactory.fromAnyRef("test-cluster-id"));

    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);

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
      DEFAULT_ZK_CLUSTER_CONFIG,
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
      DEFAULT_ZK_CLUSTER_CONFIG,
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
      DEFAULT_ZK_CLUSTER_CONFIG,
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
  public void test4ComponentsConnection() throws Exception {
    assertNull(zooKeeperServer.getZKClient().exists("/dremio4/test/test-cluster-id", false));

    try(ZKClusterClient client = new ZKClusterClient(
      DEFAULT_ZK_CLUSTER_CONFIG,
      String.format("%s/dremio4/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ZKServiceSet serviceSet = client.newServiceSet("coordinator");
      RegistrationHandle registrationHandle = serviceSet.register(NodeEndpoint.newBuilder().setAddress("foo").build());

      Stat stat = zooKeeperServer.getZKClient().exists("/dremio4/test/test-cluster-id/coordinator", false);
      assertNotNull(stat);
      assertEquals(1, stat.getNumChildren());

      serviceSet.unregister((ZKRegistrationHandle)registrationHandle);
      client.deleteServiceSetZkNode("coordinator");
      stat = zooKeeperServer.getZKClient().exists("/dremio4/test/test-cluster-id/coordinator", false);
      assertNull(stat);
    }
  }

  @Test
  public void testElection() throws Exception {
    final CountDownLatch firstElection = new CountDownLatch(1);
    final CountDownLatch secondElection = new CountDownLatch(1);
    final AtomicBoolean join1 = new AtomicBoolean(false);
    final AtomicBoolean join2 = new AtomicBoolean(false);


    try(ZKClusterClient client = new ZKClusterClient(
      DEFAULT_ZK_CLUSTER_CONFIG,
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

      assertTrue(firstElection.await(20, TimeUnit.SECONDS));
      assertTrue("Both nodes were elected master (or no election happened)", join1.get() ^ join2.get());

      // Confirming that the second node is taking over when the first node leaves the election
      if (join1.get()) {
        node1.close();
      } else {
        node2.close();
      }

      assertTrue(secondElection.await(20, TimeUnit.SECONDS));
      assertTrue("Second node didn't get elected", join1.get() && join2.get());
    }
  }

  @Test
  public void testElectionDisconnection() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch cancelled = new CountDownLatch(1);
    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("20ms"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("100ms"));
    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);

    try(ZKClusterClient client = new ZKClusterClient(
      config,
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

      assertTrue("No election happened", elected.await(20, TimeUnit.SECONDS));

      // Kill the server to force disconnection
      zooKeeperServer.closeServer();

      assertTrue("Node was not notified about cancellation", cancelled.await(20, TimeUnit.SECONDS));
      node1.close();
    }
  }

  @Test
  public void testElectionSuspended() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch cancelled = new CountDownLatch(1);
    final CountDownLatch loss = new CountDownLatch(1);
    final CountDownLatch reconnected = new CountDownLatch(1);
    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("250ms"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("10s"));
    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);

    try(ZKClusterClient client = new ZKClusterClient(
      config,
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

      assertTrue("No election happened", elected.await(20, TimeUnit.SECONDS));

      // Restart the server
      zooKeeperServer.restartServer();
      assertTrue("Node was not disconnected", loss.await(20, TimeUnit.SECONDS));
      assertTrue("Node was not reconnected", reconnected.await(20, TimeUnit.SECONDS));
      assertEquals("Node was cancelled", 1L, cancelled.getCount());
      node1.close();
    }
  }

  // This is a -ve test. Without a zero delay for ZK_ELECTION_DELAY_FOR_LEADER_CALLBACK, and a simulated delay in
  // curator's isLeader() callback, the node will lose it's leader status on zk reconnect.
  // With timeout values, the behavior is unpredictable & ends up as flaky test.
  @Ignore
  @Test
  public void testElectionDelayLeaderCallbackNegative() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch loss = new CountDownLatch(1);
    final CountDownLatch reconnected = new CountDownLatch(1);
    AtomicBoolean hadConnectionLoss = new AtomicBoolean(false);
    AtomicBoolean lostLeaderAfterReconnect = new AtomicBoolean(false);
    AtomicBoolean remainedLeaderAfterReconnect = new AtomicBoolean(false);
    final CountDownLatch leaderChosenAfterReconnect = new CountDownLatch(1);
    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("10ms"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("100s"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_DELAY_FOR_LEADER_CALLBACK, ConfigValueFactory.fromAnyRef("0ms"));
    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);

    try(ZKClusterClient client = new ZKClusterClient(
      config,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ElectionRegistrationHandle node1 = client.joinElection("test-election", new ZKElectionListener() {

        @Override
        public void onBeginIsLeader() {
          try {
            TimeUnit.MILLISECONDS.sleep(500);
          } catch (InterruptedException ignore) {}
        }

        @Override
        public void onElected() {
          elected.countDown();
          if (hadConnectionLoss.get()) {
            leaderChosenAfterReconnect.countDown();
            remainedLeaderAfterReconnect.set(true);
          }
        }

        @Override
        public void onCancelled() {
          if (hadConnectionLoss.get()) {
            leaderChosenAfterReconnect.countDown();
            lostLeaderAfterReconnect.set(true);
          }
        }

        @Override
        public void onConnectionLoss() {
          loss.countDown();
          hadConnectionLoss.set(true);
        }

        @Override
        public void onReconnection() {
          reconnected.countDown();
        }
      });

      assertTrue("No election happened", elected.await(20, TimeUnit.SECONDS));

      // Restart the server
      zooKeeperServer.restartServer();

      assertTrue("Node was not disconnected", loss.await(20, TimeUnit.SECONDS));
      assertTrue("Node was not reconnected", reconnected.await(20, TimeUnit.SECONDS));
      assertTrue("New leader was not chosen after reconnect", leaderChosenAfterReconnect.await(20, TimeUnit.SECONDS));
      assertEquals("Lost master after re-election", true, lostLeaderAfterReconnect.get());
      assertEquals("Not leader after re-election", false, remainedLeaderAfterReconnect.get());
    }
  }

  @Test
  @Ignore("Disabling flaky test DX-41442")
  public void testElectionDelayLeaderCallback() throws Exception {
    final CountDownLatch elected = new CountDownLatch(1);
    final CountDownLatch loss = new CountDownLatch(1);
    final CountDownLatch reconnected = new CountDownLatch(1);
    AtomicBoolean hadConnectionLoss = new AtomicBoolean(false);
    AtomicBoolean lostLeaderAfterReconnect = new AtomicBoolean(false);
    AtomicBoolean remainedLeaderAfterReconnect = new AtomicBoolean(false);
    final CountDownLatch leaderChosenAfterReconnect = new CountDownLatch(1);
    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("10ms"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("100s"));
      // Tests with the default value for ZK_ELECTION_DELAY_FOR_LEADER_CALLBACK i.e 500ms.
    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);

    try(ZKClusterClient client = new ZKClusterClient(
      config,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      client.start();
      ElectionRegistrationHandle node1 = client.joinElection("test-election", new ZKElectionListener() {

        @Override
        public void onBeginIsLeader() {
          try {
            // To avoid flaky tests, keep this low (about 1/10th of the value of ZK_ELECTION_DELAY_FOR_LEADER_CALLBACK)
            TimeUnit.MILLISECONDS.sleep(50);
          } catch (InterruptedException ignore) {}
        }

        @Override
        public void onElected() {
          elected.countDown();
          if (hadConnectionLoss.get()) {
            leaderChosenAfterReconnect.countDown();
            remainedLeaderAfterReconnect.set(true);
          }
        }

        @Override
        public void onCancelled() {
          if (hadConnectionLoss.get()) {
            leaderChosenAfterReconnect.countDown();
            lostLeaderAfterReconnect.set(true);
          }
        }

        @Override
        public void onConnectionLoss() {
          loss.countDown();
          hadConnectionLoss.set(true);
        }

        @Override
        public void onReconnection() {
          reconnected.countDown();
        }
      });

      assertTrue("No election happened", elected.await(20, TimeUnit.SECONDS));

      // Restart the server
      zooKeeperServer.restartServer();

      assertTrue("Node was not disconnected", loss.await(20, TimeUnit.SECONDS));
      assertTrue("Node was not reconnected", reconnected.await(20, TimeUnit.SECONDS));
      assertTrue("New leader was not chosen after reconnect", leaderChosenAfterReconnect.await(20, TimeUnit.SECONDS));
      assertEquals("Lost master after re-election", false, lostLeaderAfterReconnect.get());
      assertEquals("Not leader after re-election", true, remainedLeaderAfterReconnect.get());
    }
  }

  private ZKClusterClient getZkClientInstance() throws Exception {
    final SabotConfig sabotConfig = DEFAULT_SABOT_CONFIG
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_POLLING, ConfigValueFactory.fromAnyRef("250ms"))
      .withValue(ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT, ConfigValueFactory.fromAnyRef("5s"));
    final ZKClusterConfig config = new ZKSabotConfig(sabotConfig);
    ZKClusterClient client = new ZKClusterClient(
      config,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()));
    assertNotNull(client);
    return client;
  }

  // testElectionWithMultipleParticipants was executed repeatedly around 200 to 300 times to reproduce the
  // issue in DX-30714
  @Test
  public void testElectionWithMultipleParticipants() throws Exception {
    try (ZKClusterClient client1 = getZkClientInstance();
         ZKClusterClient client2 = getZkClientInstance()) {
      client1.start();
      client2.start();
      TestElectionListener electionListener1 = new TestElectionListener();
      TestElectionListener electionListener2 = new TestElectionListener();

      ElectionRegistrationHandle electionRegistrationHandle1 = client1.joinElection("test-election", electionListener1);
      ElectionRegistrationHandle electionRegistrationHandle2 = client2.joinElection("test-election", electionListener2);

      // For the client that gained leadership, relinquish leadership and reenter the election
      // After relinquishing leadership, asynchronously restart zk to simulate leadership lost
      // while the client reenters leadership
      Thread.sleep(1000);
      if (electionListener1.isLeader) {
        // close
        electionRegistrationHandle1.close();
        // restart the zk server to simulate leadership lost
        CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(1000);
            zooKeeperServer.restartServer();
          } catch (Exception e) {
          }
        });
        TestElectionListener temp = new TestElectionListener();
        CompletableFuture<ElectionRegistrationHandle> future = CompletableFuture.supplyAsync(() -> {
          return client1.joinElection("test-election", temp);
        });
        electionListener1 = temp;
        electionRegistrationHandle1 = future.get();
      } else if (electionListener2.isLeader) {
        electionRegistrationHandle2.close();
        CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(1000);
            zooKeeperServer.restartServer();
          } catch (Exception e) {
          }
        });
        TestElectionListener temp = new TestElectionListener();
        CompletableFuture<ElectionRegistrationHandle> future = CompletableFuture.supplyAsync(() -> {
          return client2.joinElection("test-election", temp);
        });
        electionListener2 = temp;
        electionRegistrationHandle2 = future.get();
      }

      // After zk restart, leadership would be lost and regained to one of the client.
      // wait till one of the clients gain leadership
      do {
        Thread.sleep(1000);
      } while (!electionListener1.isLeader && !electionListener2.isLeader);

      Thread.sleep(5000);

      // verify if one of the clients is leader
      boolean bothLeaders = electionListener1.isLeader && electionListener2.isLeader;
      // wait for 10 seconds to confirm if both leaders continue to exist after 10 seconds also
      if (bothLeaders) {
        Thread.sleep(10000);
        bothLeaders = electionListener1.isLeader && electionListener2.isLeader;
      }

      assertEquals(2, electionRegistrationHandle1.instanceCount());
      assertEquals(2, electionRegistrationHandle2.instanceCount());

      // assert that there is only one leader
      assertFalse("Two leaders are not expected.", bothLeaders);
    }
  }

  private class TestElectionListener implements ElectionListener {
    private volatile boolean isLeader = false;

    @Override
    public void onElected() {
      isLeader = true;
    }

    @Override
    public void onCancelled() {
      isLeader = false;
    }
  }
}
