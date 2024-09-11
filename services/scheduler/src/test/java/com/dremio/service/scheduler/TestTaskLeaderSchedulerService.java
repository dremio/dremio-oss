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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.TaskLeaderChangeListener;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.test.DremioTest;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/** To test LocalSchedulerService with Distributed Master */
public class TestTaskLeaderSchedulerService extends DremioTest {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestTaskLeaderSchedulerService.class);

  private static final String SERVICE_NAME = "myTestService";

  @Rule public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

  @Test
  public void testTaskLeaderScheduling() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host2")
              .setFabricPort(1235)
              .setUserPort(2346)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint3 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host3")
              .setFabricPort(1236)
              .setUserPort(2347)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool1 =
          new CloseableSchedulerThreadPool("test-scheduler1", 1);
      CloseableSchedulerThreadPool schedulerPool2 =
          new CloseableSchedulerThreadPool("test-scheduler2", 1);
      CloseableSchedulerThreadPool schedulerPool3 =
          new CloseableSchedulerThreadPool("test-scheduler3", 1);

      LocalSchedulerService schedulerService1 =
          new LocalSchedulerService(
              schedulerPool1,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint1),
              true);

      LocalSchedulerService schedulerService2 =
          new LocalSchedulerService(
              schedulerPool2,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint2),
              true);

      LocalSchedulerService schedulerService3 =
          new LocalSchedulerService(
              schedulerPool3,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint3),
              true);

      try {
        assertTrue(schedulerService1.getTaskLeaderElectionServices().isEmpty());
        assertTrue(schedulerService2.getTaskLeaderElectionServices().isEmpty());
        assertTrue(schedulerService3.getTaskLeaderElectionServices().isEmpty());

        AtomicInteger wasRun1 = new AtomicInteger(0);
        schedulerService1.schedule(
            Schedule.Builder.everySeconds(5L)
                .asClusteredSingleton(SERVICE_NAME)
                .releaseOwnershipAfter(1, TimeUnit.HOURS)
                .build(),
            () -> {
              wasRun1.incrementAndGet();
            });

        AtomicInteger wasRun2 = new AtomicInteger(0);
        schedulerService2.schedule(
            Schedule.Builder.everySeconds(5L)
                .asClusteredSingleton(SERVICE_NAME)
                .releaseOwnershipAfter(1, TimeUnit.HOURS)
                .build(),
            () -> {
              wasRun2.incrementAndGet();
            });

        AtomicInteger wasRun3 = new AtomicInteger(0);
        schedulerService3.schedule(
            Schedule.Builder.everySeconds(5L)
                .asClusteredSingleton(SERVICE_NAME)
                .releaseOwnershipAfter(1, TimeUnit.HOURS)
                .build(),
            () -> {
              wasRun3.incrementAndGet();
            });

        List<TaskLeaderElection> taskLeaderElectionServiceList = new ArrayList<>();
        taskLeaderElectionServiceList.addAll(schedulerService1.getTaskLeaderElectionServices());
        taskLeaderElectionServiceList.addAll(schedulerService2.getTaskLeaderElectionServices());
        taskLeaderElectionServiceList.addAll(schedulerService3.getTaskLeaderElectionServices());

        assertEquals(3, taskLeaderElectionServiceList.size());

        // wait for a leader
        while (taskLeaderElectionServiceList.stream().noneMatch(TaskLeaderElection::isTaskLeader)) {
          Thread.sleep(5);
        }

        // wait for a while to let a scheduled task which will increase a counter to run
        Thread.sleep(100);

        TaskLeaderElection leader =
            checkLeader(nodeEndpoint1, wasRun1.get(), taskLeaderElectionServiceList);

        assertEquals(
            leader, checkLeader(nodeEndpoint2, wasRun2.get(), taskLeaderElectionServiceList));
        assertEquals(
            leader, checkLeader(nodeEndpoint3, wasRun3.get(), taskLeaderElectionServiceList));

      } finally {
        AutoCloseables.close(schedulerService1, schedulerService2, schedulerService3);
      }
    }
  }

  @Test
  public void testLeaderReelectionTaskFailover() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host2")
              .setFabricPort(1235)
              .setUserPort(2346)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool1 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);
      CloseableSchedulerThreadPool schedulerPool2 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService1 =
          new LocalSchedulerService(
              schedulerPool1,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint1),
              true);

      LocalSchedulerService schedulerService2 =
          new LocalSchedulerService(
              schedulerPool2,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint2),
              true);

      try {
        CountDownLatch wasRun1 = new CountDownLatch(1);
        schedulerService1.schedule(
            Schedule.Builder.everySeconds(5L)
                .asClusteredSingleton(SERVICE_NAME)
                .releaseOwnershipAfter(1, TimeUnit.HOURS)
                .build(),
            () -> {
              wasRun1.countDown();
            });

        CountDownLatch wasRun2 = new CountDownLatch(1);
        schedulerService2.schedule(
            Schedule.Builder.everySeconds(5L)
                .asClusteredSingleton(SERVICE_NAME)
                .releaseOwnershipAfter(1, TimeUnit.HOURS)
                .build(),
            () -> {
              wasRun2.countDown();
            });

        List<TaskLeaderElection> taskLeaderElectionServiceList = new ArrayList<>();
        taskLeaderElectionServiceList.addAll(schedulerService1.getTaskLeaderElectionServices());
        taskLeaderElectionServiceList.addAll(schedulerService2.getTaskLeaderElectionServices());

        TaskLeaderElection leader = getCurrentLeader(taskLeaderElectionServiceList);

        if (leader.equals(schedulerService1.getTaskLeaderElectionServices().iterator().next())) {
          assertEquals(0, wasRun1.getCount());
        } else {
          assertEquals(0, wasRun2.getCount());
        }

        // should relinquish leadership
        leader.close();
        taskLeaderElectionServiceList.remove(leader);

        TaskLeaderElection secondLeader = getCurrentLeader(taskLeaderElectionServiceList);

        assertNotEquals(leader, secondLeader);

        if (leader.equals(schedulerService1.getTaskLeaderElectionServices().iterator().next())) {
          assertEquals(0, wasRun1.getCount());
        } else {
          assertEquals(0, wasRun2.getCount());
        }

      } finally {
        AutoCloseables.close(schedulerService1, schedulerService2);
      }
    }
  }

  @Test
  public void testTaskLeaderChangeListener() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);
      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicBoolean checkLost = new AtomicBoolean(false);
      AtomicBoolean checkRelinquish = new AtomicBoolean(false);
      AtomicBoolean checkGainedLeadership = new AtomicBoolean(false);
      AtomicInteger regularRuns = new AtomicInteger(0);
      Cancellable cancellable1 =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(3L).asClusteredSingleton("test").build(),
              new Runnable() {
                @Override
                public void run() {
                  synchronized (this) {
                    logger.info("Entering run");
                    regularRuns.incrementAndGet();
                    if (checkLost.get()) {
                      logger.info("Checking cancel");
                      try {
                        wait(2 * 1000);
                        checkLost.set(false);
                      } catch (InterruptedException e) {
                        fail("Interrupted, but should wait to completetion on losing leadership");
                      }
                      return;
                    }
                    if (checkRelinquish.get()) {
                      logger.info("Checking relinguish");
                      try {
                        wait(2 * 1000);
                        checkRelinquish.set(false);
                      } catch (InterruptedException e) {
                        fail(
                            "Interrupted, but should wait to completetion on relinquishing leadership");
                      }
                      return;
                    }
                    if (checkGainedLeadership.get()) {
                      checkGainedLeadership.set(false);
                      logger.info("Regular run");
                    }
                  }
                }
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable1);

      // should interrupt run
      checkLost.set(true);
      // let run start
      Thread.sleep(4 * 1000);
      taskLeaderElection.onCancelledLeadership();

      while (checkLost.get()) {
        Thread.sleep(50);
      }
      assertFalse(cancellable1.isCancelled());

      int runsCount = regularRuns.get();
      // should start scheduling, even it was cancelled before
      checkGainedLeadership.compareAndSet(false, true);
      Thread.sleep(4 * 1000);
      taskLeaderElection.onElectedLeadership();

      while (checkGainedLeadership.get()) {
        Thread.sleep(50);
      }
      assertFalse(cancellable1.isCancelled());

      assertTrue(regularRuns.get() > runsCount);

      // should wait till completion
      checkRelinquish.compareAndSet(false, true);
      Thread.sleep(4 * 1000);
      taskLeaderElection.onLeadershipReliquish();

      while (checkRelinquish.get()) {
        Thread.sleep(50);
      }
      assertFalse(cancellable1.isCancelled());

      runsCount = regularRuns.get();
      // should start scheduling, even it was cancelled before
      checkGainedLeadership.compareAndSet(false, true);
      taskLeaderElection.onElectedLeadership();

      while (checkGainedLeadership.get()) {
        Thread.sleep(50);
      }
      assertFalse(cancellable1.isCancelled());

      assertTrue(regularRuns.get() > runsCount);

      schedulerService.close();
    }
  }

  @Test
  public void testAfterCancellation() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);
      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicBoolean checkLost = new AtomicBoolean(true);
      AtomicBoolean checkRelinquish = new AtomicBoolean(false);
      AtomicBoolean checkGainedLeadership = new AtomicBoolean(false);
      AtomicInteger regularRuns = new AtomicInteger(0);
      Cancellable cancellable1 =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(3L).asClusteredSingleton("abc").build(),
              new Runnable() {
                @Override
                public void run() {
                  synchronized (this) {
                    logger.info("Entering run");
                    regularRuns.incrementAndGet();
                    if (checkLost.get()) {
                      logger.info("Checking cancel");
                      try {
                        wait(2 * 1000);
                        checkLost.set(false);
                      } catch (InterruptedException e) {
                        fail(
                            "Interrupted, but should wait to completetion on cancellation leadership");
                      }
                      return;
                    }
                    if (checkRelinquish.get()) {
                      try {
                        wait(2 * 1000);
                        checkRelinquish.set(false);
                      } catch (InterruptedException e) {
                        fail(
                            "Interrupted, but should wait to completetion on relinquishing leadership");
                      }
                      return;
                    }
                    if (checkGainedLeadership.get()) {
                      checkGainedLeadership.set(false);
                      logger.info("Regular run");
                    }
                  }
                }
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable1);

      // should not interrupt run
      // let run start
      Thread.sleep(4 * 1000);

      cancellable1.cancel(true);
      int runsCount = regularRuns.get();

      while (checkLost.get()) {
        Thread.sleep(50);
      }
      assertTrue(cancellable1.isCancelled());

      // should not start scheduling, though it was cancelled before
      // because it was cancelled not due to lost or relinquished leadership
      checkGainedLeadership.compareAndSet(false, true);
      taskLeaderElection.onElectedLeadership();

      Thread.sleep(500);
      // still cancelled
      assertTrue(cancellable1.isCancelled());

      assertEquals(runsCount, regularRuns.get());
      assertFalse(checkRelinquish.get());
      assertTrue(checkGainedLeadership.get());

      schedulerService.close();
    }
  }

  @Test
  public void testRemoveListener() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      CountDownLatch wasRun = new CountDownLatch(1);
      // even though this is not a single shot chain treat it as one as this test expects
      // isToRunExactlyOnce to be
      // false and is part of the confusion of schedule utils.
      // TODO: DX-68199 remove all this tests as these tests test the old clustered singleton which
      // will soon
      // be decommissioned.
      schedulerService.schedule(
          Schedule.Builder.singleShotChain()
              .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 5))
              .asClusteredSingleton(SERVICE_NAME)
              .releaseOwnershipAfter(1, TimeUnit.HOURS)
              .build(),
          () -> {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            wasRun.countDown();
          });

      Collection<TaskLeaderChangeListener> listeners =
          schedulerService
              .getTaskLeaderElectionServices()
              .iterator()
              .next()
              .getTaskLeaderChangeListeners();
      assertEquals(1, listeners.size());
      wasRun.await();
      while (!schedulerService
          .getTaskLeaderElectionServices()
          .iterator()
          .next()
          .getTaskLeaderChangeListeners()
          .isEmpty()) {
        Thread.sleep(50);
      }

      schedulerService.close();
    }
  }

  @Test
  public void testNoDistributedExecution() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host2")
              .setFabricPort(1235)
              .setUserPort(2346)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool1 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);
      CloseableSchedulerThreadPool schedulerPool2 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService1 =
          new LocalSchedulerService(
              schedulerPool1,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint1),
              false);

      LocalSchedulerService schedulerService2 =
          new LocalSchedulerService(
              schedulerPool2,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint2),
              false);

      CountDownLatch wasRun1 = new CountDownLatch(1);
      // even though this is not a single shot chain treat it as one as this test expects
      // isToRunExactlyOnce to be
      // false and is part of the confusion of schedule utils.
      // TODO: DX-68199 remove all this tests as these tests test the old clustered singleton which
      // will soon
      // be decommissioned.
      schedulerService1.schedule(
          Schedule.Builder.singleShotChain().asClusteredSingleton("abc").build(),
          () -> {
            wasRun1.countDown();
            logger.info("Schedule1 run");
          });

      CountDownLatch wasRun2 = new CountDownLatch(1);
      // even though this is not a single shot chain treat it as one as this test expects
      // isToRunExactlyOnce to be
      // false and is part of the confusion of schedule utils.
      // TODO: DX-68199 remove all this tests as these tests test the old clustered singleton which
      // will soon
      // be decommissioned.
      schedulerService2.schedule(
          Schedule.Builder.singleShotChain().asClusteredSingleton("abc").build(),
          () -> {
            wasRun2.countDown();
            logger.info("Schedule2 run");
          });

      wasRun1.await();
      wasRun2.await();

      schedulerService1.close();
      schedulerService2.close();
    }
  }

  @Test
  public void testOneTimeTaskWithLeader() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint nodeEndpoint1 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CoordinationProtos.NodeEndpoint nodeEndpoint2 =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host2")
              .setFabricPort(1235)
              .setUserPort(2346)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool1 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);
      CloseableSchedulerThreadPool schedulerPool2 =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService1 =
          new LocalSchedulerService(
              schedulerPool1,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint1),
              true);

      LocalSchedulerService schedulerService2 =
          new LocalSchedulerService(
              schedulerPool2,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(nodeEndpoint2),
              true);

      CountDownLatch globalRun = new CountDownLatch(1);
      CountDownLatch wasRun1 = new CountDownLatch(1);
      final Cancellable task1 =
          schedulerService1.schedule(
              Schedule.SingleShotBuilder.now().asClusteredSingleton("abc").build(),
              () -> {
                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                wasRun1.countDown();
                globalRun.countDown();
                logger.info("Schedule1 run");
              });

      CountDownLatch wasRun2 = new CountDownLatch(1);
      final Cancellable task2 =
          schedulerService2.schedule(
              Schedule.SingleShotBuilder.now().asClusteredSingleton("abc").build(),
              () -> {
                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                wasRun2.countDown();
                globalRun.countDown();
                logger.info("Schedule2 run");
              });

      globalRun.await();
      // just giving some time for the run to finish completely
      Thread.sleep(1000);

      // at least one should run
      assertTrue((wasRun1.getCount() == 0) ^ (wasRun2.getCount() == 0));

      if (wasRun1.getCount() == 0) {
        assertTrue(task1.isDone());
      } else {
        assertTrue(task2.isDone());
      }

      schedulerService1.close();
      schedulerService2.close();
    }
  }

  @Test
  public void testCancel() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicInteger count = new AtomicInteger(0);
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(2)
                  .asClusteredSingleton(SERVICE_NAME)
                  .releaseOwnershipAfter(1, TimeUnit.HOURS)
                  .build(),
              () -> count.incrementAndGet());

      while (count.get() < 2) {
        Thread.sleep(50);
      }

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);
      assertNotNull(taskLeaderElection);
      assertEquals(1, taskLeaderElection.getTaskLeaderChangeListeners().size());
      cancellable.cancel(true);

      assertTrue(cancellable.isCancelled());
      // After cancellation, corresponding taskLeaderElection object should be closed and removed
      // from taskLeaderElectionServiceMap
      assertNull(schedulerService.getTaskLeaderElection(cancellable));

      schedulerService.close();
    }
  }

  @Test
  public void testCleanupListener() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      CountDownLatch wasCleaned = new CountDownLatch(1);
      CountDownLatch wasRun = new CountDownLatch(1);
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(2)
                  .asClusteredSingleton("abc")
                  .startingAt(Instant.now())
                  .withCleanup(wasCleaned::countDown)
                  .build(),
              wasRun::countDown);

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);

      wasRun.await();
      assertEquals(1, wasCleaned.getCount());
      taskLeaderElection.onCancelledLeadership();
      wasCleaned.await();

      schedulerService.close();
    }
  }

  @Test
  public void testLosingAndRegainingLeadership() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicInteger counter = new AtomicInteger(0);
      CountDownLatch cleanupLatch = new CountDownLatch(1);
      CountDownLatch restartLatch = new CountDownLatch(1);
      AtomicBoolean restartFlag = new AtomicBoolean(false);
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(2)
                  .asClusteredSingleton("abc")
                  .startingAt(Instant.now())
                  .withCleanup(
                      () -> {
                        counter.set(0);
                        restartFlag.set(true);
                        cleanupLatch.countDown();
                      })
                  .build(),
              () -> {
                counter.incrementAndGet();
                if (restartFlag.get()) {
                  restartLatch.countDown();
                }
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);

      // simulate losing the leadership
      taskLeaderElection.onCancelledLeadership();

      cleanupLatch.await();
      Assert.assertEquals(0, counter.get());

      // simulate gaining the leadership back.
      // the task should be scheduled again
      taskLeaderElection.onElectedLeadership();
      boolean taskScheduledAgain = restartLatch.await(5, TimeUnit.SECONDS);
      Assert.assertTrue("Task did not get scheduled.", taskScheduledAgain);
      Assert.assertTrue(counter.get() > 0);

      schedulerService.close();
    }
  }

  @Test
  public void testLosingAndRegainingLeadershipAndTaskStillRunningInTheNode() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      // setting the number of threads to 2 here, otherwise we cannot validate the scenario
      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 2);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicInteger counter = new AtomicInteger(0);
      CountDownLatch cleanupLatch = new CountDownLatch(1);
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(2)
                  .asClusteredSingleton("abc")
                  .startingAt(Instant.now())
                  .build(),
              () -> {
                try {
                  counter.incrementAndGet();
                  cleanupLatch.await();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);

      Thread.sleep(3000);

      // simulate losing the leadership. Here we should have only 1 running
      taskLeaderElection.onCancelledLeadership();
      Thread.sleep(1000);
      Assert.assertEquals(1, counter.get());

      // simulate gain the leadership again. Previous task was not finished, we should continue
      // having only 1 task
      taskLeaderElection.onElectedLeadership();
      Thread.sleep(1000);
      Assert.assertEquals(1, counter.get());

      cleanupLatch.countDown();

      // after finish the run, the task should be re-scheduled again
      Thread.sleep(3000);
      Assert.assertTrue(counter.get() > 1);

      schedulerService.close();
    }
  }

  @Test
  public void testLostLeadershipWhileRunning() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicInteger counter = new AtomicInteger(0);
      CountDownLatch cancelLatch = new CountDownLatch(1);
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.everySeconds(1)
                  .asClusteredSingleton("abc")
                  .startingAt(Instant.now())
                  .withCleanup(
                      () -> {
                        try {
                          // sleep for five seconds; if the task
                          // gets scheduled again in a second
                          // should be within 5 seconds
                          // the task will run after this
                          Thread.sleep(4000);
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                      })
                  .build(),
              () -> {
                counter.incrementAndGet();
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);

      // simulate losing the leadership task will start and wait meanwhile
      taskLeaderElection.onCancelledLeadership();

      // wait for three seconds for actual task to complete
      Thread.sleep(3000);
      // task should have cancelled itself
      Assert.assertEquals(0, counter.get());

      schedulerService.close();
    }
  }

  @Test
  public void testLosingAndRegainingLeadershipSingleRun() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      AtomicInteger counter = new AtomicInteger(0);
      CountDownLatch cleanupLatch = new CountDownLatch(1);
      CountDownLatch restartLatch = new CountDownLatch(1);
      // even though this is not a single shot chain treat it as one as this test expects
      // isToRunExactlyOnce to be
      // false and is part of the confusion of schedule utils and the old scheduler.
      // TODO: DX-68199 remove all this tests as these tests test the old clustered singleton which
      // will soon
      // be decommissioned.
      Cancellable cancellable =
          schedulerService.schedule(
              Schedule.Builder.singleShotChain()
                  .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 3000))
                  .asClusteredSingleton("TEST")
                  .withCleanup(cleanupLatch::countDown)
                  .build(),
              () -> {
                counter.incrementAndGet();
                restartLatch.countDown();
              });

      TaskLeaderElection taskLeaderElection = schedulerService.getTaskLeaderElection(cancellable);
      // simulate losing the leadership
      taskLeaderElection.onCancelledLeadership();
      cleanupLatch.await();
      Assert.assertEquals(0, counter.get());

      // simulate gaining the leadership back.
      // the task should be scheduled again
      taskLeaderElection.onElectedLeadership();
      boolean done = restartLatch.await(5, TimeUnit.SECONDS);
      Assert.assertTrue("Task did not get scheduled.", done);
      Assert.assertTrue(counter.get() > 0);

      schedulerService.close();
    }
  }

  @Test
  public void testChangeRelinquishTime() throws Exception {
    try (ClusterCoordinator coordinator = LocalClusterCoordinator.newRunningCoordinator()) {
      coordinator.start();

      CoordinationProtos.NodeEndpoint currentEndPoint =
          CoordinationProtos.NodeEndpoint.newBuilder()
              .setAddress("host1")
              .setFabricPort(1234)
              .setUserPort(2345)
              .setRoles(
                  ClusterCoordinator.Role.toEndpointRoles(
                      Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
              .build();

      CloseableSchedulerThreadPool schedulerPool =
          new CloseableSchedulerThreadPool("test-scheduler", 1);

      LocalSchedulerService schedulerService =
          new LocalSchedulerService(
              schedulerPool,
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(coordinator),
              DirectProvider.wrap(currentEndPoint),
              true);

      CountDownLatch wasRun = new CountDownLatch(1);

      schedulerService.schedule(
          Schedule.Builder.everySeconds(2)
              .asClusteredSingleton("abc")
              .startingAt(Instant.now())
              .releaseOwnershipAfter(2, TimeUnit.SECONDS)
              .build(),
          wasRun::countDown);

      Collection<TaskLeaderElection> electionServices =
          schedulerService.getTaskLeaderElectionServices();
      assertEquals(1, electionServices.size());

      assertEquals(
          2000, electionServices.stream().findFirst().get().getLeaseExpirationTime().longValue());

      schedulerService.schedule(
          Schedule.Builder.everySeconds(2)
              .startingAt(Instant.now())
              .asClusteredSingleton("abc")
              .releaseOwnershipAfter(3, TimeUnit.SECONDS)
              .build(),
          wasRun::countDown);

      electionServices = schedulerService.getTaskLeaderElectionServices();
      assertEquals(1, electionServices.size());

      assertEquals(
          3000, electionServices.stream().findFirst().get().getLeaseExpirationTime().longValue());

      schedulerService.close();
    }
  }

  private TaskLeaderElection checkLeader(
      CoordinationProtos.NodeEndpoint serviceEndPoint,
      long count,
      List<TaskLeaderElection> taskLeaderElectionServiceList) {
    TaskLeaderElection leader = getCurrentLeader(taskLeaderElectionServiceList);
    logger.info("TaskLeader: {}", leader.getTaskLeader());
    if (serviceEndPoint.equals(leader.getTaskLeader())) {
      assertTrue(count > 0);
    } else {
      assertEquals(0, count);
    }
    return leader;
  }

  private TaskLeaderElection getCurrentLeader(
      List<TaskLeaderElection> taskLeaderElectionServiceList) {
    List<TaskLeaderElection> leaders =
        taskLeaderElectionServiceList.stream()
            .peek(
                v -> {
                  if (v.isTaskLeader()) {
                    assertEquals(v.getCurrentEndPoint(), v.getTaskLeader());
                  } else {
                    assertNotEquals(v.getCurrentEndPoint(), v.getTaskLeader());
                  }
                  logger.info(
                      "endpoint: {}, lead endpoint: {}", v.getCurrentEndPoint(), v.getTaskLeader());
                })
            .filter(TaskLeaderElection::isTaskLeader)
            .collect(Collectors.toList());
    assertEquals(1, leaders.size());
    return leaders.get(0);
  }
}
