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

import static com.dremio.test.DremioTest.DEFAULT_SABOT_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.TaskLeaderChangeListener;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * To test TaskLeaderElection
 */
public class TestTaskLeaderElection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTaskLeaderElection.class);

  private static final String SERVICE_NAME = "myTestService";
  @Rule
  public final ZooKeeperServerResource zooKeeperServer = new ZooKeeperServerResource();

  @Rule
  public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

  @Test
  public void testElectionsWithRegistration() throws Exception {
    try (ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
      DEFAULT_SABOT_CONFIG,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host1")
        .setFabricPort(1234)
        .setUserPort(2345)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host2")
        .setFabricPort(1235)
        .setUserPort(2346)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint3 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host3")
        .setFabricPort(1236)
        .setUserPort(2347)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();


      TaskLeaderElection taskLeaderElection1 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 60 * 60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 60 * 60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint2),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection3 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 60 * 60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint3),
          Executors.newSingleThreadScheduledExecutor()
        );

      List<TaskLeaderElection> taskLeaderElectionList =
        Lists.newArrayList(
          taskLeaderElection1,
          taskLeaderElection2,
          taskLeaderElection3);

      taskLeaderElectionList.forEach(v -> {
        try {
          v.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // wait for a leader
      while (taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection leader = getCurrentLeader(taskLeaderElectionList);

      // stop leader
      assertNotNull(leader);
      leader.close();
      taskLeaderElectionList.remove(leader);

      // wait until the leader is removed
      waitUntilLeaderRemoved(taskLeaderElectionList, leader);

      while (taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection secondLeader = getCurrentLeader(taskLeaderElectionList);

      assertNotNull(secondLeader);
      // leadership should change
      assertTrue(!leader.getCurrentEndPoint().equals(secondLeader.getCurrentEndPoint()));

      // stop second leader
      assertNotNull(secondLeader);
      secondLeader.close();
      taskLeaderElectionList.remove(secondLeader);

      // wait until second leader is removed
      waitUntilLeaderRemoved(taskLeaderElectionList, secondLeader);

      while (taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection thirdLeader = getCurrentLeader(taskLeaderElectionList);

      assertNotNull(thirdLeader);
      // leadership should change
      assertTrue(!leader.getCurrentEndPoint().equals(thirdLeader.getCurrentEndPoint()));
      assertTrue(!secondLeader.getCurrentEndPoint().equals(thirdLeader.getCurrentEndPoint()));

      // stop third leader
      assertNotNull(thirdLeader);
      thirdLeader.close();
    }
  }

  @Test
  public void testGivingUpLeadership() throws Exception {
    try (ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
      DEFAULT_SABOT_CONFIG,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host1")
        .setFabricPort(1234)
        .setUserPort(2345)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host2")
        .setFabricPort(1235)
        .setUserPort(2346)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      TaskLeaderElection taskLeaderElection1 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 5L, // 5 sec.
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 5L, // 5 sec.
          DirectProvider.wrap(nodeEndpoint2),
          Executors.newSingleThreadScheduledExecutor()
        );

      List<TaskLeaderElection> taskLeaderElectionList =
        ImmutableList.of(
          taskLeaderElection1,
          taskLeaderElection2);

      taskLeaderElectionList.forEach(v -> {
        try {
          v.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      while (taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(50);
      }

      int i = 0;
      while (i++ < 3) {
        // may not really be a leader by the time it comes back
        TaskLeaderElection leader = getCurrentLeaderFilter(taskLeaderElectionList);
        assertNotNull(leader);

        CoordinationProtos.NodeEndpoint leadEndPoint = leader.getTaskLeader();
        CoordinationProtos.NodeEndpoint taskEndPpoint = leader.getCurrentEndPoint();

        while (leadEndPoint.equals(taskEndPpoint)) {
          Thread.sleep(100);
          leadEndPoint = leader.getTaskLeader();
          taskEndPpoint = leader.getCurrentEndPoint();
        }
      }
      taskLeaderElectionList.forEach(
        v -> {
          try {
            v.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    }
  }

  @Test
  public void testTaskLeaderChangeListener () throws Exception {
    try (ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
      DEFAULT_SABOT_CONFIG,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))
    ) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host1")
        .setFabricPort(1234)
        .setUserPort(2345)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host2")
        .setFabricPort(1235)
        .setUserPort(2346)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CountDownLatch gained1 = new CountDownLatch(1);
      CountDownLatch lost1 = new CountDownLatch(1);
      CountDownLatch relinquished1 = new CountDownLatch(1);

      TaskLeaderChangeListener taskLeaderChangeListener1 = new TaskLeaderChangeListener() {
        @Override
        public void onLeadershipGained() {
          gained1.countDown();
        }

        @Override
        public void onLeadershipLost() {
          lost1.countDown();
        }

        @Override
        public void onLeadershipRelinquished() {
          relinquished1.countDown();
        }
      };

      TaskLeaderElection taskLeaderElectionService1 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 5L, // 5 sec.
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor()
        );

      taskLeaderElectionService1.addListener(taskLeaderChangeListener1);

      CountDownLatch gained2 = new CountDownLatch(1);
      CountDownLatch lost2 = new CountDownLatch(1);
      CountDownLatch relinquished2 = new CountDownLatch(1);

      TaskLeaderChangeListener taskLeaderChangeListener2 = new TaskLeaderChangeListener() {
        @Override
        public void onLeadershipGained() {
          gained2.countDown();
        }

        @Override
        public void onLeadershipLost() {
          lost2.countDown();
        }

        @Override
        public void onLeadershipRelinquished() {
          relinquished2.countDown();
        }
      };

      TaskLeaderElection taskLeaderElectionService2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          1000 * 5L, // 5 sec.
          DirectProvider.wrap(nodeEndpoint2),
          Executors.newSingleThreadScheduledExecutor()
        );

      taskLeaderElectionService2.addListener(taskLeaderChangeListener2);

      List<TaskLeaderElection> taskLeaderElectionServiceList =
        ImmutableList.of(
          taskLeaderElectionService1,
          taskLeaderElectionService2);


      taskLeaderElectionServiceList.forEach(v -> {
        try {
          v.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      while (taskLeaderElectionServiceList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(50);
      }

      // may not really be a leader by the time it comes back
      TaskLeaderElection leader = getCurrentLeaderFilter(taskLeaderElectionServiceList);
      assertNotNull(leader);

      CoordinationProtos.NodeEndpoint leadEndPoint = leader.getTaskLeader();
      CoordinationProtos.NodeEndpoint taskEndPpoint = leader.getCurrentEndPoint();

      while (leadEndPoint.equals(taskEndPpoint)) {
        Thread.sleep(100);
        leadEndPoint = leader.getTaskLeader();
        taskEndPpoint = leader.getCurrentEndPoint();
      }

      assertEquals(0, gained1.getCount());
      assertEquals(0, gained2.getCount());
      assertEquals(1, lost1.getCount());
      assertEquals(1, lost2.getCount());
      assertTrue(relinquished1.getCount() == 0 || relinquished2.getCount() == 0);

      taskLeaderElectionServiceList.forEach(
        v -> {
          try {
            v.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    }
  }

  @Test
  public void testReEnterElection() throws Exception {
    try (ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
      DEFAULT_SABOT_CONFIG,
      String.format("%s/dremio/test/test-cluster-id", zooKeeperServer.getConnectString()))) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host1")
        .setFabricPort(1234)
        .setUserPort(2345)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("host2")
        .setFabricPort(1235)
        .setUserPort(2346)
        .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
        .build();


      TestElectionListenerProvider electionListenerProvider = new TestElectionListenerProvider();

      TaskLeaderElection taskLeaderElectionService1 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          5000L, // 5secs
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor(),
          20L,
          electionListenerProvider
        );

      TaskLeaderElection taskLeaderElectionService2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          DirectProvider.wrap(coordinator),
          5000L, // 5secs
          DirectProvider.wrap(nodeEndpoint2),
          Executors.newSingleThreadScheduledExecutor(),
          20L,
          electionListenerProvider
        );

      List<TaskLeaderElection> taskLeaderElectionServiceList =
        ImmutableList.of(
          taskLeaderElectionService1,
          taskLeaderElectionService2);


      taskLeaderElectionServiceList.forEach(v -> {
        try {
          v.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // wait till atleast one participant is elected.
      while (taskLeaderElectionServiceList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(50);
      }

      // set ignore election such that after relinquish, the test simulates the situation where there is no leader
      electionListenerProvider.setIgnoreElection();

      // wait till there are no leaders after leadership relinquish. when electedCount == 2
      // TestPreConditionElectionListener return false.
      while (taskLeaderElectionServiceList.stream().anyMatch(TaskLeaderElection::isTaskLeader))  {
        Thread.sleep(50);
      }

      List<TaskLeaderElection> leaders = taskLeaderElectionServiceList
        .stream()
        .filter(TaskLeaderElection::isTaskLeader).collect(Collectors.toList());
      Assert.assertEquals(0, leaders.size());

      electionListenerProvider.resetIgnoreElection();

      // due to reelection there must be a leader elected
      while (taskLeaderElectionServiceList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(50);
      }

      TaskLeaderElection leader = getCurrentLeaderFilter(taskLeaderElectionServiceList);
      assertNotNull(leader);
    }
  }

  private void waitUntilLeaderRemoved(List<TaskLeaderElection> taskLeaderElectionMap, TaskLeaderElection leader) throws Exception {
    while (taskLeaderElectionMap.stream().anyMatch(v -> { return leader.getCurrentEndPoint().equals(v.getTaskLeader()); })) {
      Thread.sleep(100);
    }
  }

  private TaskLeaderElection getCurrentLeader(List<TaskLeaderElection> taskLeaderElectionMap) {
      List<TaskLeaderElection> leaders = taskLeaderElectionMap
        .stream()
        .peek(v -> {
          if (v.isTaskLeader()) {
            assertEquals(v.getCurrentEndPoint(), v.getTaskLeader());
          } else {
            assertNotEquals(v.getCurrentEndPoint(), v.getTaskLeader());
          }
          logger.info("endpoint: {}, lead endpoint: {}", v.getCurrentEndPoint(), v.getTaskLeader());
        }).filter(TaskLeaderElection::isTaskLeader).collect(Collectors.toList());
      assertEquals(1, leaders.size());
      return leaders.get(0);
  }

  private TaskLeaderElection getCurrentLeaderFilter(List <TaskLeaderElection> taskLeaderElectionMap) throws InterruptedException {
    final int retries = 10;
    for (int i = 0; i <= retries; i++) {
      List<TaskLeaderElection> leaders = taskLeaderElectionMap
        .stream()
        .filter(TaskLeaderElection::isTaskLeader).collect(Collectors.toList());
      assertTrue("The number of leader was more than 1.", leaders.size() <= 1);
      if (leaders.size() == 1) {
        return leaders.get(0);
      } else {
        logger.warn("Failed to get current leader. Will wait a while for the new leader.");
        Thread.sleep(1000);
      }
    }
    throw new RuntimeException("Failed to get current leader.");
  }

  private static class TestElectionListenerProvider implements Function<ElectionListener, ElectionListener> {
    private final AtomicBoolean ignoreElection = new AtomicBoolean(false);

    @Override
    public ElectionListener apply(ElectionListener innerListener) {
      return new TestElectionListener(innerListener, ignoreElection);
    }

    void setIgnoreElection() {
      ignoreElection.set(true);
    }

    void resetIgnoreElection() {
      ignoreElection.set(false);
    }
  }

  private static class TestElectionListener implements ElectionListener {

    private final ElectionListener innerListener;
    private final AtomicBoolean atomicBoolean;


    TestElectionListener(ElectionListener innerListener, AtomicBoolean atomicBoolean) {
      this.innerListener = innerListener;
      this.atomicBoolean = atomicBoolean;
    }

    @Override
    public void onElected() {
      logger.info("Called onElected {}", atomicBoolean.get());
      if (!atomicBoolean.get()) {
        innerListener.onElected();
      }
    }

    @Override
    public void onCancelled() {
      innerListener.onCancelled();
    }
  }

}
