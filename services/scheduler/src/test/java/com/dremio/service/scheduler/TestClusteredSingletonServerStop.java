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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.util.TestTools;
import com.dremio.test.DremioTest;
import com.dremio.test.zookeeper.ZkTestServerRule;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests the full ZK server shutdown case where sessions do not expire even on a ZK Lost on the
 * client.
 */
@Ignore(
    "Takes several minutes to run this test. Remove this line and run manually when a relevant change is made")
public class TestClusteredSingletonServerStop extends DremioTest {
  // number of schedules per test.
  private static final int NUM_TEST_CLIENTS = 6;
  private static final int NUM_NORMAL_SCHEDULES = 10;
  private static final int NUM_CHAINED_SCHEDULES = 5;
  private static final int TOTAL_SCHEDULES = NUM_NORMAL_SCHEDULES + NUM_CHAINED_SCHEDULES;
  private static final int ML = 3;
  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(1200, TimeUnit.SECONDS);

  @ClassRule
  public static final ZkTestServerRule zkServerResource = new ZkTestServerRule("/css/dremio");

  private final TestClient[] testClients = new TestClient[NUM_TEST_CLIENTS];

  @SuppressWarnings("resource")
  @Before
  public void setup() throws Exception {
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i] = new TestClient(i, zkServerResource.getConnectionString());
    }
  }

  @After
  public void tearDown() throws Exception {
    Arrays.stream(testClients).filter(Objects::nonNull).forEach(TestClient::close);
  }

  @Test
  public void testServerStopAndStartNormalSchedules() throws Exception {
    final AtomicInteger currentIndex = new AtomicInteger(0);
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final CountDownLatch[] cleanupLatch = initLatch(1);
    Schedule[] testSchedules = createNormalSchedules(1, currentIndex, cleanupCount, cleanupLatch);
    final CountDownLatch[] latch1 = initLatch(2);
    final CountDownLatch[] latch2 = initLatch(500);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          int idx = currentIndex.get();
          if (idx >= ML) {
            // test is done
            return;
          }
          incrementer.incrementAndGet();
          latch1[idx].countDown();
          latch2[idx].countDown();
        };
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(testSchedules[0], task);
      Thread.yield();
      Thread.yield();
    }

    doTestLoop(currentIndex, cleanupLatch, latch1, latch2);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo((500 * ML) - 1);
    assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(ML);
  }

  @Test
  public void testServerStopStartChainedSchedule() throws Exception {
    final AtomicInteger currentIndex = new AtomicInteger(0);
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final CountDownLatch[] cleanupLatch = initLatch(1);
    Schedule[] testSchedules =
        createChainedSchedules(0, 1, currentIndex, cleanupCount, cleanupLatch, 300);
    final CountDownLatch[] latch1 = initLatch(2);
    final CountDownLatch[] latch2 = initLatch(400);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable[] tasks = new Runnable[1];
    tasks[0] =
        () -> {
          int idx = currentIndex.get();
          if (idx >= ML) {
            return;
          }
          incrementer.incrementAndGet();
          latch1[idx].countDown();
          latch2[idx].countDown();
          final Schedule nextInChain =
              createSingleShotChain(
                  testName.getMethodName(), currentIndex, cleanupCount, cleanupLatch, 200);
          for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
            testClients[i].getSingletonScheduler().schedule(nextInChain, tasks[0]);
          }
        };
    for (int i = NUM_TEST_CLIENTS - 1; i >= 0; i--) {
      testClients[i].getSingletonScheduler().schedule(testSchedules[0], tasks[0]);
      Thread.yield();
      Thread.yield();
    }

    doTestLoop(currentIndex, cleanupLatch, latch1, latch2);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo((400 * ML) - 1);
    assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(ML);
  }

  @Test
  public void testServerStopStartMixedSchedules() throws Exception {
    final AtomicInteger currentIndex = new AtomicInteger(0);
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final CountDownLatch[] cleanupLatch = initLatch(1);
    Schedule[] normalSchedules =
        createNormalSchedules(NUM_NORMAL_SCHEDULES, currentIndex, cleanupCount, cleanupLatch);
    Schedule[] chainedSchedules =
        createChainedSchedules(
            NUM_NORMAL_SCHEDULES,
            NUM_CHAINED_SCHEDULES,
            currentIndex,
            cleanupCount,
            cleanupLatch,
            200);
    final CountDownLatch[][] latches1 = new CountDownLatch[TOTAL_SCHEDULES][];
    final CountDownLatch[][] latches2 = new CountDownLatch[TOTAL_SCHEDULES][];
    final AtomicInteger[] incrementers = new AtomicInteger[TOTAL_SCHEDULES];
    for (int i = 0; i < TOTAL_SCHEDULES; i++) {
      latches1[i] = initLatch(2);
      latches2[i] = initLatch(300);
      incrementers[i] = new AtomicInteger(0);
    }
    Runnable[] tasks = new Runnable[TOTAL_SCHEDULES];
    for (int i = 0; i < TOTAL_SCHEDULES; i++) {
      final int scheduleNo = i;
      tasks[scheduleNo] =
          () -> {
            int idx = currentIndex.get();
            if (idx >= ML) {
              idx = ML - 1;
            }
            incrementers[scheduleNo].incrementAndGet();
            latches1[scheduleNo][idx].countDown();
            latches2[scheduleNo][idx].countDown();
            if (scheduleNo >= NUM_NORMAL_SCHEDULES) {
              Schedule nextInChain =
                  createSingleShotChain(
                      testName.getMethodName() + scheduleNo,
                      currentIndex,
                      cleanupCount,
                      cleanupLatch,
                      150);
              for (int k = 0; k < NUM_TEST_CLIENTS; k++) {
                testClients[k].getSingletonScheduler().schedule(nextInChain, tasks[scheduleNo]);
              }
            }
          };
    }
    for (int i = 0; i < TOTAL_SCHEDULES; i++) {
      int start = i % NUM_TEST_CLIENTS;
      int current = -1;
      while (current != start) {
        if (current < 0) {
          current = start;
        }
        if (i < NUM_NORMAL_SCHEDULES) {
          testClients[current].getSingletonScheduler().schedule(normalSchedules[i], tasks[i]);
        } else {
          testClients[current]
              .getSingletonScheduler()
              .schedule(chainedSchedules[i - NUM_NORMAL_SCHEDULES], tasks[i]);
        }
        Thread.yield();
        Thread.yield();
        current = (current + 1) % NUM_TEST_CLIENTS;
      }
    }

    doTestLoop(currentIndex, cleanupLatch, latches1[0], latches2[0]);
    for (int i = 0; i < TOTAL_SCHEDULES; i++) {
      latches2[i][ML - 1].await();
      assertThat(incrementers[i].get()).isGreaterThanOrEqualTo(300 * (ML - 1));
    }
  }

  // does a controlled switch back and forth between the two clustered singleton instances
  private void doTestLoop(
      AtomicInteger currentIndex,
      CountDownLatch[] cleanupLatch,
      CountDownLatch[] latch1,
      CountDownLatch[] latch2)
      throws Exception {
    for (int i = 0; i < ML; i++) {
      int idx = currentIndex.get();
      latch1[idx].await();

      // stop server without loosing state
      zkServerResource.stopServer();

      // wait for ZK Lost
      cleanupLatch[idx].await();

      zkServerResource.restartServer();

      // wait for recovery to kick in post session expiration and schedules to start on singleton
      // instance 0
      latch2[idx].await();
      currentIndex.incrementAndGet();
    }
  }

  private CountDownLatch[] initLatch(int val) {
    CountDownLatch[] latches = new CountDownLatch[ML];
    for (int i = 0; i < ML; i++) {
      latches[i] = new CountDownLatch(val);
    }
    return latches;
  }

  private Schedule[] createNormalSchedules(
      int numSchedules,
      AtomicInteger currentIndex,
      AtomicInteger cleanupCount,
      CountDownLatch[] cleanupLatch) {
    final Schedule[] testSchedules = new Schedule[numSchedules];
    for (int i = 0; i < numSchedules; i++) {
      final String taskName = testName.getMethodName() + (numSchedules == 1 ? "" : i);
      testSchedules[i] =
          Schedule.Builder.everyMillis(250)
              .asClusteredSingleton(taskName)
              .withCleanup(
                  () -> {
                    int idx = currentIndex.get();
                    cleanupCount.incrementAndGet();
                    if (idx < ML) {
                      cleanupLatch[idx].countDown();
                    }
                  })
              .build();
    }
    return testSchedules;
  }

  private Schedule[] createChainedSchedules(
      int start,
      int numSchedules,
      AtomicInteger currentIndex,
      AtomicInteger cleanupCount,
      CountDownLatch[] cleanupLatch,
      int delay) {
    final Schedule[] testSchedules = new Schedule[numSchedules];
    for (int i = 0; i < numSchedules; i++) {
      final String taskName = testName.getMethodName() + (numSchedules == 1 ? "" : start + i);
      testSchedules[i] =
          createSingleShotChain(taskName, currentIndex, cleanupCount, cleanupLatch, delay);
    }
    return testSchedules;
  }

  private Schedule createSingleShotChain(
      String taskName,
      AtomicInteger currentIndex,
      AtomicInteger cleanupCount,
      CountDownLatch[] cleanupLatch,
      int delay) {
    return Schedule.Builder.singleShotChain()
        .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + delay))
        .asClusteredSingleton(taskName)
        .withCleanup(
            () -> {
              int idx = currentIndex.get();
              cleanupCount.incrementAndGet();
              if (idx < ML) {
                cleanupLatch[idx].countDown();
              }
            })
        .sticky()
        .build();
  }
}
