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

import com.dremio.common.util.TestTools;
import com.dremio.test.DremioTest;
import com.dremio.test.zookeeper.ZkTestServerRule;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests basic functionality of the new implementation of the {@link
 * ClusteredSingletonTaskScheduler}.
 */
public class TestWeightBasedClusteredSingletonScheduler extends DremioTest {
  private static final int NUM_TEST_CLIENTS = 2;
  private static final int NUM_TEST_SCHEDULES = 100;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(120, TimeUnit.SECONDS);

  @ClassRule
  public static final ZkTestServerRule zkServerResource = new ZkTestServerRule("/css/dremio");

  private final TestClient[] testClients = new TestClient[NUM_TEST_CLIENTS];

  @SuppressWarnings("resource")
  @Before
  public void setup() throws Exception {
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i] = new TestClient(i, zkServerResource.getConnectionString(), 10, 5);
    }
  }

  @After
  public void tearDown() throws Exception {
    Arrays.stream(testClients).forEach(TestClient::close);
  }

  @Test
  public void testBasicWeightBasedScheduling() throws InterruptedException {
    Schedule testSchedule =
        Schedule.Builder.runOnceEverySwitchOver()
            .asClusteredSingleton(testName.getMethodName())
            .withWeightProvider(() -> 1)
            .withCleanup(() -> {})
            .build();
    createAndRunScheduleOnce(testSchedule);
    // create another schedule with same name
    createAndRunScheduleOnce(testSchedule);
  }

  @Test
  public void testBasicWeightBasedSchedulingSwitchOver() throws InterruptedException {
    AtomicInteger cleanupCount = new AtomicInteger(0);
    Schedule testSchedule =
        Schedule.Builder.runOnceEverySwitchOver()
            .asClusteredSingleton(testName.getMethodName())
            .withWeightProvider(() -> 1)
            .withCleanup(cleanupCount::incrementAndGet)
            .build();
    CountDownLatch[] latches = new CountDownLatch[11];
    for (int i = 0; i < 11; i++) {
      latches[i] = new CountDownLatch(i + 1);
    }
    final AtomicInteger incrementer = new AtomicInteger();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      createSchedule(i, testSchedule, incrementer, latches);
      Thread.yield();
      Thread.yield();
      Thread.yield();
    }
    latches[0].await();
    Assertions.assertThat(incrementer.get()).isEqualTo(1);
    Thread.sleep(100);

    // now start a continuous loop of alternate restarts
    for (int i = 0; i < 10; i++) {
      int idx = i % NUM_TEST_CLIENTS;
      testClients[idx].close();
      latches[idx + 1].await();
      testClients[idx] = new TestClient(idx, zkServerResource.getConnectionString(), 10, 5);
      createSchedule(idx, testSchedule, incrementer, latches);
      Assertions.assertThat(incrementer.get()).isGreaterThanOrEqualTo(i);
    }
  }

  @Test
  public void testBasicWeightBasedLoadBalancing() throws InterruptedException {
    final AtomicInteger incrementer = new AtomicInteger();
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final Schedule[] testSchedules = new Schedule[NUM_TEST_SCHEDULES];
    for (int i = 0; i < NUM_TEST_SCHEDULES; i++) {
      testSchedules[i] =
          Schedule.Builder.runOnceEverySwitchOver()
              .asClusteredSingleton(testName.getMethodName() + i)
              .withWeightProvider(() -> 1)
              .withCleanup(cleanupCount::incrementAndGet)
              .build();
    }
    // make all the schedules loaded onto a single instance by closing the other
    testClients[1].close();
    CountDownLatch[] latches = new CountDownLatch[2];
    latches[0] = new CountDownLatch(NUM_TEST_SCHEDULES);
    // slightly less than half will move
    var expectedToMove = (NUM_TEST_SCHEDULES / 2) - 3;
    latches[1] = new CountDownLatch(NUM_TEST_SCHEDULES + expectedToMove);
    for (int i = 0; i < NUM_TEST_SCHEDULES; i++) {
      createSchedule(0, testSchedules[i], incrementer, latches);
    }
    latches[0].await();
    Assertions.assertThat(incrementer.get()).isEqualTo(100);
    Thread.sleep(1000);
    Assertions.assertThat(incrementer.get()).isEqualTo(100);

    // now start the second instance
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString(), 10, 5);
    for (int i = 0; i < NUM_TEST_SCHEDULES; i++) {
      createSchedule(1, testSchedules[i], incrementer, latches);
    }
    latches[1].await();
    Assertions.assertThat(incrementer.get())
        .isGreaterThanOrEqualTo(NUM_TEST_SCHEDULES + expectedToMove);
    Assertions.assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(expectedToMove);
    // assert that some tasks have moved to 1
    Thread.sleep(2000);
    var schedulesOwnedByOne = numOwnedBy(1, testName.getMethodName(), NUM_TEST_SCHEDULES);
    var schedulesOwnedByZero = numOwnedBy(0, testName.getMethodName(), NUM_TEST_SCHEDULES);
    Assertions.assertThat(schedulesOwnedByOne).isGreaterThanOrEqualTo(expectedToMove);
    Assertions.assertThat(schedulesOwnedByZero).isLessThan(NUM_TEST_SCHEDULES - expectedToMove);

    // now add 10 more schedules with increasing weight
    Schedule[] nextSchedules = new Schedule[10];
    var valBeforeNextMove = incrementer.get();
    var cleanupBeforeNextMove = cleanupCount.get();
    latches = new CountDownLatch[2];
    latches[0] = new CountDownLatch(10);
    latches[1] = new CountDownLatch(12);
    for (int i = 0; i < 10; i++) {
      final var weightToReturn = (i + 1) * 2;
      nextSchedules[i] =
          Schedule.Builder.runOnceEverySwitchOver()
              .asClusteredSingleton(testName.getMethodName() + (NUM_TEST_SCHEDULES + i))
              .withWeightProvider(() -> weightToReturn)
              .withCleanup(cleanupCount::incrementAndGet)
              .build();
      createSchedule(0, nextSchedules[i], incrementer, latches);
    }
    latches[0].await();
    Thread.yield();
    Thread.yield();
    for (int i = 0; i < 10; i++) {
      createSchedule(1, nextSchedules[i], incrementer, latches);
    }
    latches[1].await();
    Assertions.assertThat(incrementer.get()).isGreaterThan(valBeforeNextMove);
    Assertions.assertThat(cleanupCount.get()).isGreaterThan(cleanupBeforeNextMove);
    Thread.sleep(2000);
    // assert that some tasks have moved to 1
    var schedulesOwnedByOneNow = numOwnedBy(1, testName.getMethodName(), NUM_TEST_SCHEDULES + 10);
    Assertions.assertThat(schedulesOwnedByOneNow).isGreaterThan(schedulesOwnedByOne);
  }

  /// run two schedules S1 and S2 on same node 0, start node 1 with S2 migrating; kill node 1;
  /// 2 should come back to node 0;
  @Test
  public void testWeightBasedLoadBalancingWithSwitchOver() throws InterruptedException {
    final AtomicInteger[] incrementers = new AtomicInteger[2];
    final AtomicInteger[] cleanup = new AtomicInteger[2];
    final Schedule[] testSchedules = new Schedule[2];
    // choose schedule name in such a way that the hashcode for the second schedule false to zero
    final String[] scheduleNames = {testName.getMethodName() + "0", testName.getMethodName() + "2"};
    Assertions.assertThat((scheduleNames[1].hashCode() & Integer.MAX_VALUE) % 2).isEqualTo(0);
    for (int i = 0; i < 2; i++) {
      incrementers[i] = new AtomicInteger();
      cleanup[i] = new AtomicInteger();
      var bldr = Schedule.Builder.runOnceEverySwitchOver().asClusteredSingleton(scheduleNames[i]);
      if (i == 0) {
        testSchedules[i] =
            bldr.withWeightProvider(() -> 20).withCleanup(cleanup[i]::incrementAndGet).build();
      } else {
        testSchedules[i] =
            bldr.withWeightProvider(() -> 5).withCleanup(cleanup[i]::incrementAndGet).build();
      }
    }
    // make all the schedules loaded onto a single instance by closing the other
    testClients[1].close();
    var latchesFor2 = new CountDownLatch[3];
    for (int i = 0; i < 3; i++) {
      latchesFor2[i] = new CountDownLatch(i + 1);
    }
    createSchedule(0, testSchedules[0], incrementers[0], null);
    createSchedule(0, testSchedules[1], incrementers[1], latchesFor2);

    latchesFor2[0].await();
    Assertions.assertThat(incrementers[1].get()).isEqualTo(1);

    // now start the second instance
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString(), 10, 5);
    createSchedule(1, testSchedules[0], incrementers[0], null);
    createSchedule(1, testSchedules[1], incrementers[1], latchesFor2);
    // now schedule 2 should migrate to node 2; wait for it
    latchesFor2[1].await();
    Assertions.assertThat(incrementers[1].get()).isEqualTo(2);
    Assertions.assertThat(cleanup[1].get()).isGreaterThanOrEqualTo(1);
    Thread.sleep(2000);
    Assertions.assertThat(incrementers[1].get()).isEqualTo(2);
    var schedulesOwnedByOne = numOwnedBy(1, testName.getMethodName(), 3);
    var schedulesOwnedByZero = numOwnedBy(0, testName.getMethodName(), 3);
    Assertions.assertThat(schedulesOwnedByOne).isEqualTo(1);
    Assertions.assertThat(schedulesOwnedByZero).isEqualTo(1);

    // now kill node 2
    testClients[1].close();
    latchesFor2[2].await();
    Assertions.assertThat(incrementers[1].get()).isEqualTo(3);
    Thread.sleep(2000);
    schedulesOwnedByZero = numOwnedBy(0, testName.getMethodName(), 3);
    Assertions.assertThat(schedulesOwnedByZero).isEqualTo(2);
  }

  private int numOwnedBy(int client, String baseTaskName, int numSchedules) {
    int numOwned = 0;
    var otherEndpoint = testClients[(client + 1) % NUM_TEST_CLIENTS].getEndpoint();
    String endpointToCheck = testClients[client].getEndpoint().getAddress();
    for (int i = 0; i < numSchedules; i++) {
      var taskOwner =
          testClients[client]
              .getSingletonScheduler()
              .getCurrentTaskOwner(baseTaskName + i)
              .orElse(otherEndpoint)
              .getAddress();
      if (taskOwner.equals(endpointToCheck)) {
        numOwned++;
      }
    }
    return numOwned;
  }

  private Cancellable createSchedule(
      int client, Schedule testSchedule, AtomicInteger incrementer, CountDownLatch[] execLatches) {
    return testClients[client]
        .getSingletonScheduler()
        .schedule(
            testSchedule,
            () -> {
              incrementer.incrementAndGet();
              if (execLatches != null) {
                for (int i = 0; i < execLatches.length; i++) {
                  execLatches[i].countDown();
                }
              }
            });
  }

  private void createAndRunScheduleOnce(Schedule testSchedule) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger();
    Cancellable[] cancellables = new Cancellable[NUM_TEST_CLIENTS];
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i] =
          testClients[i]
              .getSingletonScheduler()
              .schedule(
                  testSchedule,
                  () -> {
                    latch.countDown();
                    incrementer.incrementAndGet();
                  });
    }
    latch.await();
    Assertions.assertThat(incrementer.get()).isGreaterThanOrEqualTo(1);
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i].cancel(false);
    }
  }
}
