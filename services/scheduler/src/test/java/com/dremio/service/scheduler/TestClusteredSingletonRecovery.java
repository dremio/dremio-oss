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

import static com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.test.DremioTest;
import com.dremio.test.zookeeper.ZkTestServerRule;
import java.time.Duration;
import java.time.Instant;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the recovery aspects of the new implementation of the ClusteredSingleton */
public class TestClusteredSingletonRecovery extends DremioTest {
  // number of schedules per test. Keep it less than or equal to 10 (size of default pool) to drive
  // the
  // non-stealing recovery test path
  private static final int NUM_TEST_SCHEDULES_LO = 10;
  private static final int NUM_TEST_CLIENTS = 3;

  @ClassRule
  public static final ZkTestServerRule zkServerResource = new ZkTestServerRule("/css/dremio");

  private final ScheduleTaskGroup taskGroup = new TestClient.TestGroup(1);

  private final TestClient[] testClients = new TestClient[NUM_TEST_CLIENTS];
  private final Random testRand = new Random(System.currentTimeMillis());

  @SuppressWarnings("resource")
  @Before
  public void setup() throws Exception {
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i] = new TestClient(i, zkServerResource.getConnectionString());
      testClients[i].getSingletonScheduler().addTaskGroup(taskGroup);
    }
  }

  @After
  public void tearDown() throws Exception {
    Arrays.stream(testClients).forEach(TestClient::close);
  }

  @Test
  public void testBasicRecovery() throws Exception {
    // firstLatch controls when to stop one service instance
    CountDownLatch firstLatch = new CountDownLatch(1);
    // secondLatch controls when to end the test and assert
    CountDownLatch secondLatch = new CountDownLatch(15);
    final AtomicInteger incrementer = new AtomicInteger();
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          firstLatch.countDown();
          secondLatch.countDown();
          final Duration nextDuration =
              (incrementer.get() <= 15) ? Duration.ofMillis(100) : Duration.ofHours(1000);
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, nextDuration).build();
        };
    final Schedule schedule =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(testName.getMethodName())
            .scheduleModifier(scheduleModifierFn)
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, incrementer::incrementAndGet);
    }
    firstLatch.await();
    // Crash client 0 and this will force recovery
    testClients[0].close();
    secondLatch.await();
    assertThat(incrementer.get()).isBetween(15, 17);
  }

  @Test
  public void testRandomRecovery() throws Exception {
    CountDownLatch firstLatch = new CountDownLatch(10);
    CountDownLatch secondLatch = new CountDownLatch(20);
    CountDownLatch thirdLatch = new CountDownLatch(30);
    final Schedule[] schedules = new Schedule[10];
    final AtomicInteger[] incrementers = new AtomicInteger[10];
    final Runnable[] tasks = new Runnable[10];

    for (int i = 0; i < 10; i++) {
      schedules[i] =
          Schedule.Builder.everyMillis(500L)
              .asClusteredSingleton(testName.getMethodName() + i)
              .build();
      incrementers[i] = new AtomicInteger(0);
      final int k = i;
      tasks[i] =
          () -> {
            firstLatch.countDown();
            secondLatch.countDown();
            thirdLatch.countDown();
            incrementers[k].incrementAndGet();
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
    firstLatch.await();
    // this will force recovery
    testClients[0].close();
    secondLatch.await();
    testClients[1].close();
    try (TestClient ignored = new TestClient(0, zkServerResource.getConnectionString())) {
      thirdLatch.await();
      for (int i = 0; i < 10; i++) {
        assertThat(incrementers[i].get()).isGreaterThanOrEqualTo(1);
      }
    }
  }

  @Test
  public void testSingleShotRecovery() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch opposingLatch = new CountDownLatch(1);
    final CountDownLatch counterOpposingLatch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger(0);
    final Runnable task =
        singleShotRecoverableTask(latch, opposingLatch, counterOpposingLatch, null, incrementer);
    final Schedule schedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 50))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, task);
    }
    latch.await();
    // simulate a crash by closing the client
    testClients[0].close();
    opposingLatch.countDown();
    // now wait for the schedule to recover
    counterOpposingLatch.await();
    assertThat(incrementer.get()).isEqualTo(2);
  }

  @Test
  public void testManySingleShotRecovery() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] opposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] counterOpposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final AtomicInteger[] incrementers = new AtomicInteger[NUM_TEST_SCHEDULES_LO];
    final Runnable[] tasks = new Runnable[NUM_TEST_SCHEDULES_LO];
    final Schedule[] schedules = new Schedule[NUM_TEST_SCHEDULES_LO];
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i] = new CountDownLatch(1);
      opposingLatches[i] = new CountDownLatch(1);
      counterOpposingLatches[i] = new CountDownLatch(1);
      incrementers[i] = new AtomicInteger(0);
      tasks[i] =
          singleShotRecoverableTask(
              latches[i], opposingLatches[i], counterOpposingLatches[i], null, incrementers[i]);
      final String taskName =
          testName.getMethodName()
              + IntStream.range(0, i + 1)
                  .boxed()
                  .map((x) -> Integer.toString(x))
                  .collect(Collectors.joining());
      schedules[i] =
          Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
              .asClusteredSingleton(taskName)
              .build();
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      for (int j = 0; j < NUM_TEST_CLIENTS; j++) {
        testClients[j].getSingletonScheduler().schedule(schedules[i], tasks[i]);
      }
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i].await();
    }
    // simulate a crash by closing the client
    testClients[1].close();
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      opposingLatches[i].countDown();
    }
    // now wait for the schedule to recover
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      counterOpposingLatches[i].await();
    }
    // do a random sleep just to expose potential windows
    Thread.sleep(10 + testRand.nextInt(500));
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      assertThat(incrementers[i].get()).isBetween(1, 2);
    }
  }

  @Test
  public void testRunOnceOnDeathRecovery() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch opposingLatch = new CountDownLatch(1);
    final CountDownLatch counterOpposingLatch = new CountDownLatch(1);
    final CountDownLatch onDeathLatch1 = new CountDownLatch(3);
    final CountDownLatch onDeathLatch2 = new CountDownLatch(4);
    final AtomicInteger incrementer = new AtomicInteger(0);
    final Runnable task =
        singleShotRecoverableTask(
            latch,
            opposingLatch,
            counterOpposingLatch,
            new CountDownLatch[] {onDeathLatch1, onDeathLatch2},
            incrementer);
    final Schedule schedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 50))
            .asClusteredSingleton(testName.getMethodName())
            .setSingleShotType(Schedule.SingleShotType.RUN_ONCE_EVERY_MEMBER_DEATH)
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, task);
    }
    latch.await();
    // simulate a crash by closing the client
    testClients[0].close();
    opposingLatch.countDown();
    // now wait for the schedule to recover
    counterOpposingLatch.await();
    assertThat(incrementer.get()).isEqualTo(2);
    testClients[0] = new TestClient(0, zkServerResource.getConnectionString());
    testClients[0].getSingletonScheduler().schedule(schedule, task);
    testClients[1].close();
    onDeathLatch1.await();
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString());
    testClients[1].getSingletonScheduler().schedule(schedule, task);
    testClients[2].close();
    onDeathLatch2.await();
    assertThat(incrementer.get()).isEqualTo(4);
  }

  @Test
  public void testManyRunOnceOnDeathRecovery() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] opposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] counterOpposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] onDeathLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final AtomicInteger[] incrementers = new AtomicInteger[NUM_TEST_SCHEDULES_LO];
    final Runnable[] tasks = new Runnable[NUM_TEST_SCHEDULES_LO];
    final Schedule[] schedules = new Schedule[NUM_TEST_SCHEDULES_LO];
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i] = new CountDownLatch(1);
      opposingLatches[i] = new CountDownLatch(1);
      counterOpposingLatches[i] = new CountDownLatch(1);
      incrementers[i] = new AtomicInteger(0);
      onDeathLatches[i] = new CountDownLatch(3);
      tasks[i] =
          singleShotRecoverableTask(
              latches[i],
              opposingLatches[i],
              counterOpposingLatches[i],
              new CountDownLatch[] {onDeathLatches[i]},
              incrementers[i]);
      final String taskName =
          testName.getMethodName()
              + IntStream.range(0, i + 1)
                  .boxed()
                  .map((x) -> Integer.toString(x))
                  .collect(Collectors.joining());
      schedules[i] =
          Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
              .asClusteredSingleton(taskName)
              .setSingleShotType(Schedule.SingleShotType.RUN_ONCE_EVERY_MEMBER_DEATH)
              .build();
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      for (int j = 0; j < NUM_TEST_CLIENTS; j++) {
        testClients[j].getSingletonScheduler().schedule(schedules[i], tasks[i]);
      }
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i].await();
    }
    // simulate a crash by closing the client
    testClients[1].close();
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      opposingLatches[i].countDown();
    }
    // now wait for the schedule to recover
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      counterOpposingLatches[i].await();
    }
    Thread.sleep(10 + testRand.nextInt(500));
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString());
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      testClients[1].getSingletonScheduler().schedule(schedules[i], tasks[i]);
    }
    testClients[2].close();
    Thread.sleep(10 + testRand.nextInt(500));
    testClients[0].close();
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      onDeathLatches[i].await();
      assertThat(incrementers[i].get()).isBetween(3, 4);
    }
  }

  @Test
  public void testManyRunOnceOnDeathRecoveryDelayedStart() throws Exception {
    final CountDownLatch[] latches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] opposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] counterOpposingLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final CountDownLatch[] onDeathLatches = new CountDownLatch[NUM_TEST_SCHEDULES_LO];
    final AtomicInteger[] incrementers = new AtomicInteger[NUM_TEST_SCHEDULES_LO];
    final Runnable[] tasks = new Runnable[NUM_TEST_SCHEDULES_LO];
    final Schedule[] schedules = new Schedule[NUM_TEST_SCHEDULES_LO];
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i] = new CountDownLatch(1);
      opposingLatches[i] = new CountDownLatch(1);
      counterOpposingLatches[i] = new CountDownLatch(1);
      incrementers[i] = new AtomicInteger(0);
      onDeathLatches[i] = new CountDownLatch(3);
      tasks[i] =
          singleShotRecoverableTask(
              latches[i],
              opposingLatches[i],
              counterOpposingLatches[i],
              new CountDownLatch[] {onDeathLatches[i]},
              incrementers[i]);
      final String taskName =
          testName.getMethodName()
              + IntStream.range(0, i + 1)
                  .boxed()
                  .map((x) -> Integer.toString(x))
                  .collect(Collectors.joining());
      schedules[i] =
          Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
              .asClusteredSingleton(taskName)
              .setSingleShotType(Schedule.SingleShotType.RUN_ONCE_EVERY_MEMBER_DEATH)
              .build();
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      for (int j = 0; j < NUM_TEST_CLIENTS; j++) {
        testClients[j].getSingletonScheduler().schedule(schedules[i], tasks[i]);
      }
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      latches[i].await();
    }
    // simulate a crash by closing the client
    testClients[1].close();
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      opposingLatches[i].countDown();
    }
    // now wait for the schedule to recover
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      counterOpposingLatches[i].await();
    }
    Thread.sleep(10 + testRand.nextInt(500));
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString());
    testClients[2].close();
    Thread.sleep(10 + testRand.nextInt(500));
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      testClients[1].getSingletonScheduler().schedule(schedules[i], tasks[i]);
    }
    Thread.sleep(10 + testRand.nextInt(500));
    testClients[0].close();
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      onDeathLatches[i].await();
      assertThat(incrementers[i].get()).isBetween(3, 4);
    }
  }

  // number of loops. To control number of schedules in continuous recovery test. Must be even.
  private static final int NUM_LOOPS = 6;

  @Test
  public void testContinuousRecovery() throws Exception {
    doContinuousRecovery(500, false, false);
  }

  @Test
  public void testContinuousRecoveryWithLoadAndMinCapacityPool() throws Exception {
    doContinuousRecovery(10, true, true);
  }

  @Test
  public void testOwnershipQuery() throws Exception {
    // firstLatch controls when to stop one service instance
    final CountDownLatch[] latches = new CountDownLatch[7];
    for (int i = 0; i < 7; i++) {
      latches[i] = new CountDownLatch(i + 1);
    }
    final AtomicInteger incrementer = new AtomicInteger();
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          final Duration nextDuration =
              (incrementer.get() <= 9) ? Duration.ofMillis(200) : Duration.ofHours(1000);
          for (CountDownLatch latch : latches) {
            latch.countDown();
          }
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, nextDuration).build();
        };
    final String taskName = testName.getMethodName();
    final Schedule schedule =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(taskName)
            .scheduleModifier(scheduleModifierFn)
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, incrementer::incrementAndGet);
    }
    latches[0].await();
    assertEndpoints(0, 1, taskName, 3);
    // Crash client 0 and this will force recovery
    testClients[0].close();
    latches[2].await();
    final NodeEndpoint ep1 =
        testClients[1].getSingletonScheduler().getCurrentTaskOwner(taskName).orElse(null);
    Assert.assertNotNull(ep1);
    int expectedIdx = ep1.equals(testClients[1].getEndpoint()) ? 1 : 2;
    int notExpectedIdx = (expectedIdx == 1) ? 2 : 1;
    assertEndpoints(expectedIdx, 0, taskName, 2);
    assertEndpoints(expectedIdx, notExpectedIdx, taskName, 2);
    latches[4].await();
    // Crash client 0 and this will force recovery
    testClients[1].close();
    latches[6].await();
    assertEndpoints(2, 0, taskName, 1);
    assertEndpoints(2, 1, taskName, 1);
  }

  void assertEndpoints(
      int expectedClientIdx, int notExpectedClientIdx, String taskName, int numClients) {
    final NodeEndpoint[] eps = new NodeEndpoint[numClients];
    for (int i = 2, j = 0; i >= 3 - numClients; i--, j++) {
      eps[j] = testClients[i].getSingletonScheduler().getCurrentTaskOwner(taskName).orElse(null);
    }
    for (NodeEndpoint ep : eps) {
      Assert.assertNotNull(ep);
      assertThat(ep).isEqualTo(testClients[expectedClientIdx].getEndpoint());
      assertThat(ep).isNotEqualTo(testClients[notExpectedClientIdx].getEndpoint());
    }
  }

  private void doContinuousRecovery(int periodInMillis, boolean withTaskPool, boolean smallDelay)
      throws Exception {
    CountDownLatch[] latches = new CountDownLatch[100];
    for (int i = 0; i < NUM_LOOPS; i++) {
      latches[i] = new CountDownLatch((i + 1) * 10);
    }
    final Schedule[] schedules = new Schedule[NUM_TEST_SCHEDULES_LO];
    final AtomicInteger[] incrementers = new AtomicInteger[NUM_TEST_SCHEDULES_LO];
    final Runnable[] tasks = new Runnable[NUM_TEST_SCHEDULES_LO];

    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      Schedule.ClusteredSingletonBuilder b =
          Schedule.Builder.everyMillis(periodInMillis)
              .asClusteredSingleton(testName.getMethodName() + i);
      if (withTaskPool) {
        b = b.taskGroup(taskGroup.getGroupName());
      }
      schedules[i] = b.build();
      incrementers[i] = new AtomicInteger(0);
      final int k = i;
      tasks[i] =
          () -> {
            for (int j = 0; j < NUM_LOOPS; j++) {
              latches[j].countDown();
            }
            if (smallDelay) {
              Thread.yield();
              Thread.yield();
            }
            incrementers[k].incrementAndGet();
          };
    }
    List<Integer> clientIdxList =
        IntStream.range(0, NUM_TEST_CLIENTS).boxed().collect(Collectors.toList());
    for (int j = 0; j < NUM_TEST_SCHEDULES_LO; j++) {
      Collections.shuffle(clientIdxList, testRand);
      final int k = j;
      clientIdxList.forEach(
          (i) -> testClients[i].getSingletonScheduler().schedule(schedules[k], tasks[k]));
    }
    for (int i = 0; i < NUM_LOOPS; i += 2) {
      latches[i].await();
      final int clientToClose = testRand.nextInt(NUM_TEST_CLIENTS);
      testClients[clientToClose].close();
      latches[i + 1].await();
      testClients[clientToClose] =
          new TestClient(clientToClose, zkServerResource.getConnectionString());
      if (withTaskPool) {
        testClients[clientToClose].getSingletonScheduler().addTaskGroup(taskGroup);
      }
      for (int j = 0; j < NUM_TEST_SCHEDULES_LO; j++) {
        testClients[clientToClose].getSingletonScheduler().schedule(schedules[j], tasks[j]);
      }
    }
    for (int i = 0; i < NUM_TEST_SCHEDULES_LO; i++) {
      assertThat(incrementers[i].get()).isGreaterThan(1);
    }
  }

  private Runnable singleShotRecoverableTask(
      CountDownLatch latch,
      CountDownLatch opposingLatch,
      CountDownLatch counterOpposingLatch,
      CountDownLatch[] onDeathLatches,
      AtomicInteger incrementer) {
    return () -> {
      latch.countDown();
      int val = incrementer.getAndIncrement();
      if (val == 0) {
        try {
          opposingLatch.await();
          // if await is not interrupted we are done
          counterOpposingLatch.countDown();
        } catch (InterruptedException ignored) {
          // retain the interrupt flag
          Thread.currentThread().interrupt();
        }
      } else {
        // this schedule should be called a second time on recovery as the first one did not
        // complete
        counterOpposingLatch.countDown();
      }
      if (onDeathLatches != null && onDeathLatches.length >= 1) {
        for (CountDownLatch l : onDeathLatches) {
          l.countDown();
        }
      }
    };
  }
}
