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

import static com.dremio.test.DremioTest.DEFAULT_SABOT_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.google.common.collect.ImmutableList;
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
    try(ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
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
      1000*60*60L, // 1 hour
          1000*60*60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          1000*60*60L, // 1 hour
          1000*60*60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint2),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection3 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          1000*60*60L, // 1 hour
          1000*60*60L, // 1 hour
          DirectProvider.wrap(nodeEndpoint3),
          Executors.newSingleThreadScheduledExecutor()
        );

      List<TaskLeaderElection> taskLeaderElectionList =
        ImmutableList.of(
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
      while(taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection leader = getCurrentLeader(taskLeaderElectionList);

      // stop leader
      assertNotNull(leader);
      leader.close();

      while(taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection secondLeader = getCurrentLeader(taskLeaderElectionList);

      assertNotNull(secondLeader);
      // leadership should change
      assertTrue(!leader.getCurrentEndPoint().equals(secondLeader.getCurrentEndPoint()));

      // stop second leader
      assertNotNull(secondLeader);
      secondLeader.close();

      while(taskLeaderElectionList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
        Thread.sleep(100);
      }

      TaskLeaderElection thirdLeader = getCurrentLeader(taskLeaderElectionList);

      assertNotNull(thirdLeader);
      // leadership should change
      assertTrue(!leader.getCurrentEndPoint().equals(thirdLeader.getCurrentEndPoint()));
      assertTrue(!secondLeader.getCurrentEndPoint().equals(thirdLeader.getCurrentEndPoint()));

      // stop second leader
      assertNotNull(thirdLeader);
      thirdLeader.close();
    }
  }

  @Test
  public void testGivingUpLeadership() throws Exception {
    try(ZKClusterCoordinator coordinator = new ZKClusterCoordinator(
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
          1000 * 5L, // 5 sec.
          1000 * 5L, // 5 sec.
          DirectProvider.wrap(nodeEndpoint1),
          Executors.newSingleThreadScheduledExecutor()
        );

      TaskLeaderElection taskLeaderElection2 =
        new TaskLeaderElection(
          SERVICE_NAME,
          DirectProvider.wrap(coordinator),
          1000 * 5L, // 5 sec.
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

  private TaskLeaderElection getCurrentLeader(List<TaskLeaderElection> taskLeaderElectionMap) {
    List<TaskLeaderElection> leaders = taskLeaderElectionMap
      .stream()
      .map( v -> {
        if (v.isTaskLeader()) {
          assertEquals(v.getCurrentEndPoint(), v.getTaskLeader());
        } else {
          assertNotEquals(v.getCurrentEndPoint(), v.getTaskLeader());
        }
        logger.info("endpoint: {}, lead endpoint: {}", v.getCurrentEndPoint(), v.getTaskLeader());
        return v;
      }).filter(TaskLeaderElection::isTaskLeader).collect(Collectors.toList());
    assertEquals(1, leaders.size());
    return leaders.get(0);
  }

  private TaskLeaderElection getCurrentLeaderFilter(List<TaskLeaderElection>
                                                      taskLeaderElectionMap) {
    List<TaskLeaderElection> leaders = taskLeaderElectionMap
      .stream()
      .filter(TaskLeaderElection::isTaskLeader).collect(Collectors.toList());
    assertEquals(1, leaders.size());
    return leaders.get(0);
  }

}

