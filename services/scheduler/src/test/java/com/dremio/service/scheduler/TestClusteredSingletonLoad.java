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

import com.dremio.test.DremioTest;
import com.dremio.test.zookeeper.ZkTestServerRule;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the load/stealing aspects of the new implementation of the ClusteredSingleton */
public class TestClusteredSingletonLoad extends DremioTest {
  // number of schedules per test. Keep it less than or equal to 10 (size of default pool) to drive
  // the
  // non-stealing recovery test path
  private static final int NUM_TEST_CLIENTS = 3;
  private static final int LOW_POOL_SIZE = 2;

  @ClassRule
  public static final ZkTestServerRule zkServerResource = new ZkTestServerRule("/css/dremio");

  private final TestClient[] testClients = new TestClient[NUM_TEST_CLIENTS];
  private final ScheduleTaskGroup testGroup = new TestClient.TestGroup(LOW_POOL_SIZE);

  private final Random testRand = new Random(System.currentTimeMillis());

  @SuppressWarnings("resource")
  @Before
  public void setup() throws Exception {
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i] = new TestClient(i, zkServerResource.getConnectionString());
      testClients[i].getSingletonScheduler().addTaskGroup(testGroup);
    }
  }

  @After
  public void tearDown() throws Exception {
    Arrays.stream(testClients).forEach(TestClient::close);
  }

  @Test
  public void testHighLoadWithLowPoolCapacity() throws Exception {
    final CountDownLatch[] firstLatches = new CountDownLatch[10];
    final Schedule[] schedules = new Schedule[10];
    final AtomicInteger[] incrementers = new AtomicInteger[10];
    final Runnable[] tasks = new Runnable[10];
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          int idx = Integer.parseInt(old.getTaskName().split("XXX")[1]);
          final Duration nextDuration =
              (incrementers[idx].get() < 25) ? Duration.ofMillis(10) : Duration.ofHours(1000);
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, nextDuration).build();
        };
    for (int i = 0; i < 10; i++) {
      firstLatches[i] = new CountDownLatch(25);
      schedules[i] =
          Schedule.Builder.everyMillis(10L)
              .asClusteredSingleton(testName.getMethodName() + "XXX" + i)
              .scheduleModifier(scheduleModifierFn)
              .taskGroup(testGroup.getGroupName())
              .build();
      incrementers[i] = new AtomicInteger(0);
      final int k = i;
      tasks[i] =
          () -> {
            testLoop();
            incrementers[k].incrementAndGet();
            firstLatches[k].countDown();
          };
    }
    for (int j = 0; j < 10; j++) {
      for (int k = 0; k < NUM_TEST_CLIENTS; k++) {
        testClients[k].getSingletonScheduler().schedule(schedules[j], tasks[j]);
      }
    }
    for (int i = 0; i < 10; i++) {
      firstLatches[i].await();
    }
    assertThat(incrementers[0].get()).isBetween(25, 26);
    assertThat(incrementers[9].get()).isBetween(25, 26);
  }

  @Test
  public void testStickySchedulesWithLowPoolCapacity() throws Exception {
    final CountDownLatch[] firstLatches = new CountDownLatch[10];
    final Schedule[] schedules = new Schedule[10];
    final AtomicInteger[] incrementers = new AtomicInteger[10];
    final Runnable[] tasks = new Runnable[10];
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          int idx = Integer.parseInt(old.getTaskName().split("XXX")[1]);
          final Duration nextDuration =
              (incrementers[idx].get() < 25) ? Duration.ofMillis(10) : Duration.ofHours(1000);
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, nextDuration).build();
        };
    for (int i = 0; i < 10; i++) {
      firstLatches[i] = new CountDownLatch(25);
      schedules[i] =
          Schedule.Builder.everyMillis(10L)
              .asClusteredSingleton(testName.getMethodName() + "XXX" + i)
              .scheduleModifier(scheduleModifierFn)
              .taskGroup(testGroup.getGroupName())
              .sticky()
              .build();
      incrementers[i] = new AtomicInteger(0);
      final int k = i;
      tasks[i] =
          () -> {
            testLoop();
            incrementers[k].incrementAndGet();
            firstLatches[k].countDown();
          };
    }
    final AtomicInteger[][] track = new AtomicInteger[10][NUM_TEST_CLIENTS];
    for (int j = 0; j < 10; j++) {
      for (int k = 0; k < NUM_TEST_CLIENTS; k++) {
        track[j][k] = new AtomicInteger(0);
      }
    }
    for (int j = 0; j < 10; j++) {
      for (int k = 0; k < NUM_TEST_CLIENTS; k++) {
        final int scheduleNo = j;
        final int clientId = k;
        testClients[k]
            .getSingletonScheduler()
            .schedule(
                schedules[j],
                () -> {
                  track[scheduleNo][clientId].incrementAndGet();
                  tasks[scheduleNo].run();
                });
      }
    }
    for (int i = 0; i < 10; i++) {
      firstLatches[i].await();
    }
    assertThat(incrementers[0].get()).isBetween(25, 26);
    assertThat(incrementers[9].get()).isBetween(25, 26);
    boolean found;
    for (int j = 0; j < 10; j++) {
      found = false;
      for (int k = 0; k < NUM_TEST_CLIENTS; k++) {
        int val = track[j][k].get();
        if (val > 0) {
          assertThat(found).isEqualTo(false);
          assertThat(val).isBetween(25, 26);
          found = true;
        }
      }
    }
  }

  @Test
  public void testHighLoadWithChangingPoolCapacity() throws Exception {
    final CountDownLatch[] firstLatches = new CountDownLatch[10];
    final CountDownLatch[] secondLatches = new CountDownLatch[10];
    final Schedule[] schedules = new Schedule[10];
    final AtomicInteger[] incrementers = new AtomicInteger[10];
    final Runnable[] tasks = new Runnable[10];
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          int idx = Integer.parseInt(old.getTaskName().split("XXX")[1]);
          final Duration nextDuration =
              (incrementers[idx].get() < 100) ? Duration.ofMillis(10) : Duration.ofHours(1000);
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, nextDuration).build();
        };

    for (int i = 0; i < 10; i++) {
      firstLatches[i] = new CountDownLatch(10);
      secondLatches[i] = new CountDownLatch(100);
      schedules[i] =
          Schedule.Builder.everyMillis(10L)
              .asClusteredSingleton(testName.getMethodName() + "XXX" + i)
              .scheduleModifier(scheduleModifierFn)
              .taskGroup(testGroup.getGroupName())
              .build();
      incrementers[i] = new AtomicInteger(0);
      final int k = i;
      tasks[i] =
          () -> {
            testLoop();
            incrementers[k].incrementAndGet();
            firstLatches[k].countDown();
            secondLatches[k].countDown();
          };
    }
    List<Integer> clientIdxList =
        IntStream.range(0, NUM_TEST_CLIENTS).boxed().collect(Collectors.toList());
    for (int j = 0; j < 10; j++) {
      Collections.shuffle(clientIdxList, testRand);
      final int k = j;
      clientIdxList.forEach(
          (i) -> testClients[i].getSingletonScheduler().schedule(schedules[k], tasks[k]));
    }
    for (int i = 0; i < 10; i++) {
      firstLatches[i].await();
    }
    ScheduleTaskGroup changedGroup = new TestClient.TestGroup(10);
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i]
          .getSingletonScheduler()
          .modifyTaskGroup(testGroup.getGroupName(), changedGroup);
    }
    for (int i = 0; i < 10; i++) {
      secondLatches[i].await();
    }
    assertThat(incrementers[0].get()).isEqualTo(100);
  }

  private void testLoop() {
    for (int i = 0; i < 200; i++) {
      if (i % 50 == 0) {
        Thread.yield();
      }
    }
  }
}
