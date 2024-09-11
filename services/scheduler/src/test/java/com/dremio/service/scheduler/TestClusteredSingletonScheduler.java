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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.test.DremioTest;
import com.dremio.test.zookeeper.ZkTestServerRule;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests basic functionality of the new implementation of the {@link
 * ClusteredSingletonTaskScheduler}.
 */
public class TestClusteredSingletonScheduler extends DremioTest {
  private static final int NUM_TEST_CLIENTS = 3;

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
    Arrays.stream(testClients).forEach(TestClient::close);
  }

  @Test
  public void testWeighBasedSchedulingNotSupportedYet() {
    Schedule testSchedule =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(testName.getMethodName())
            .withWeightProvider(() -> 1)
            .withCleanup(() -> {})
            .build();
    final AtomicInteger incrementer = new AtomicInteger();
    try {
      testClients[0].getSingletonScheduler().schedule(testSchedule, incrementer::incrementAndGet);
      fail("Weight based scheduling is not supported yet");
    } catch (IllegalArgumentException e) {
      Assertions.assertThat(e.getMessage()).contains("not enabled");
    }
  }

  @Test
  public void testBasicScheduling() throws Exception {
    assertFalse(
        testClients[0]
            .getSingletonScheduler()
            .isRollingUpgradeInProgress(testName.getMethodName()));
    CountDownLatch latch = new CountDownLatch(1);
    // make schedule modification high so that it will not run the next schedule for hours
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          latch.countDown();
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, Duration.ofHours(1000))
              .build();
        };
    Schedule testSchedule =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(testName.getMethodName())
            .scheduleModifier(scheduleModifierFn)
            .build();
    assertFalse(testSchedule.isLargePeriodicity());
    AtomicInteger incrementer = new AtomicInteger();
    Cancellable[] cancellables = new Cancellable[NUM_TEST_CLIENTS];
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i] =
          testClients[i]
              .getSingletonScheduler()
              .schedule(testSchedule, incrementer::incrementAndGet);
    }
    latch.await();
    // incrementer should be called only once from only one of the schedulers.
    Assert.assertEquals(incrementer.get(), 1);
    Arrays.stream(cancellables).forEach((c) -> c.cancel(true));
  }

  @Test
  public void testPeriodicStartNow() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Schedule testSchedule =
        Schedule.Builder.everyHours(1)
            .startingAt(Instant.now())
            .asClusteredSingleton(testName.getMethodName())
            .build();
    Thread.sleep(200);
    AtomicInteger incrementer = new AtomicInteger();
    Cancellable[] cancellables = new Cancellable[NUM_TEST_CLIENTS];
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i] =
          testClients[i]
              .getSingletonScheduler()
              .schedule(
                  testSchedule,
                  () -> {
                    incrementer.incrementAndGet();
                    latch.countDown();
                  });
    }
    latch.await();
    // incrementer should be called only once from only one of the schedulers.
    Assert.assertEquals(incrementer.get(), 1);
    Arrays.stream(cancellables).forEach((c) -> c.cancel(true));
  }

  @Test
  public void testCleanupCalledOnCancel() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger cleanupCount = new AtomicInteger();
    Schedule testSchedule =
        Schedule.Builder.everyHours(1)
            .startingAt(Instant.now())
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(
                () -> {
                  cleanupCount.incrementAndGet();
                  latch.countDown();
                })
            .build();
    Thread.sleep(200);
    AtomicInteger incrementer = new AtomicInteger();
    Cancellable[] cancellables = new Cancellable[NUM_TEST_CLIENTS];
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i] =
          testClients[i]
              .getSingletonScheduler()
              .schedule(testSchedule, incrementer::incrementAndGet);
      Thread.yield();
      Thread.yield();
      Thread.yield();
    }
    // incrementer should be called only once from only one of the schedulers.
    Assert.assertEquals(incrementer.get(), 1);
    Arrays.stream(cancellables).forEach((c) -> c.cancel(true));
    latch.await();
    Assert.assertEquals(cleanupCount.get(), 1);
  }

  @Test
  public void testSchedulingMultiple() throws Exception {
    CountDownLatch latch = new CountDownLatch(30);
    // make schedule modification low so that multiple schedules will run for all schedules
    final Function<Schedule, Schedule> scheduleModifierFn =
        (old) -> {
          latch.countDown();
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, Duration.ofMillis(100))
              .build();
        };
    final Schedule[] schedules = new Schedule[3];
    final AtomicInteger[] incrementers = new AtomicInteger[3];

    for (int i = 0; i < 3; i++) {
      schedules[i] =
          Schedule.Builder.everySeconds(1L)
              .asClusteredSingleton(testName.getMethodName() + i)
              .scheduleModifier(scheduleModifierFn)
              .build();
      incrementers[i] = new AtomicInteger();
    }
    List<Integer> clientIdxList =
        IntStream.range(0, NUM_TEST_CLIENTS).boxed().collect(Collectors.toList());
    Random rand = new Random(System.currentTimeMillis());
    for (int j = 0; j < 3; j++) {
      Collections.shuffle(clientIdxList, rand);
      final int k = j;
      clientIdxList.forEach(
          (i) ->
              testClients[i]
                  .getSingletonScheduler()
                  .schedule(schedules[k], incrementers[k]::incrementAndGet));
    }
    latch.await();
    for (int i = 0; i < 3; i++) {
      Assertions.assertThat(incrementers[i].get()).isGreaterThan(1);
    }
  }

  @Test
  public void testDuplicateSingleShotScheduling() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch.countDown();
        };
    final Schedule schedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, task);
    }
    final Schedule schedule1 =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 10000))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    try {
      testClients[0].getSingletonScheduler().schedule(schedule1, task);
      Assert.fail("Schedule modification should fail for single shot schedules");
    } catch (Exception e) {
      Assertions.assertThat(e.getMessage()).contains("Illegal schedule modification");
    }
    latch.await();
    Assertions.assertThat(incrementer.get()).isEqualTo(1);
  }

  @Test
  public void testLockStepScheduling() {
    final Random rand = new Random();
    int client1 = doLockStep(rand);
    int client2 = doLockStep(rand);
    int client3 = doLockStep(rand);
    Assertions.assertThat(client1).isEqualTo(client2);
    Assertions.assertThat(client1).isEqualTo(client3);
  }

  @Test
  public void testDoneHandling() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch.countDown();
        };
    final Schedule schedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(schedule, task);
    }
    latch.await();
    Assertions.assertThat(incrementer.get()).isEqualTo(1);
  }

  @Test
  public void testSingleShotScheduling() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch.countDown();
        };
    final Schedule schedule =
        Schedule.SingleShotBuilder.now().asClusteredSingleton(testName.getMethodName()).build();
    final Cancellable[] cancellables = new Cancellable[NUM_TEST_CLIENTS];
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      cancellables[i] = testClients[i].getSingletonScheduler().schedule(schedule, task);
    }
    latch.await();
    Assertions.assertThat(incrementer.get()).isEqualTo(1);
    for (Cancellable c : cancellables) {
      int i = 0;
      while (!c.isDone() && i++ < 100) {
        Thread.sleep(50);
      }
      assertTrue(c.isDone());
    }
  }

  @Test
  public void testScheduleChaining() throws Exception {
    CountDownLatch latch = new CountDownLatch(10);
    final AtomicInteger incrementer = new AtomicInteger();
    final Runnable[] tasks = new Runnable[NUM_TEST_CLIENTS];
    final Schedule schedule =
        Schedule.Builder.singleShotChain()
            .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 50))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    assertFalse(schedule.isLargePeriodicity());
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      final int k = i;
      tasks[i] =
          () -> {
            latch.countDown();
            incrementer.incrementAndGet();
            final Schedule schedule1 =
                Schedule.Builder.singleShotChain()
                    .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 100))
                    .asClusteredSingleton(testName.getMethodName())
                    .build();
            for (int j = 0; j < NUM_TEST_CLIENTS; j++) {
              testClients[j].getSingletonScheduler().schedule(schedule1, tasks[k]);
            }
          };
      testClients[i].getSingletonScheduler().schedule(schedule, tasks[i]);
    }
    latch.await();
    Assertions.assertThat(incrementer.get()).isGreaterThanOrEqualTo(9);
  }

  @Test
  public void testClusteredSingletonSchedulingMultipleCancel() throws Exception {
    final CountDownLatch latch = new CountDownLatch(2);
    final Schedule[] schedules = new Schedule[2];
    final AtomicInteger[] incrementers = new AtomicInteger[2];

    // make schedule modification high so that it will not run the next schedule for hours after 4
    // schedules
    final Function<Schedule, Schedule> scheduleModifierFn1 =
        (old) -> {
          if (incrementers[0].get() == 2) {
            latch.countDown();
          }
          if (incrementers[0].get() > 3) {
            return Schedule.ClusteredSingletonBuilder.fromSchedule(old, Duration.ofHours(1000))
                .build();
          } else {
            return null;
          }
        };

    // make schedule modification high so that it will not run the next schedule for hours after 3
    // schedules
    final Function<Schedule, Schedule> scheduleModifierFn2 =
        (old) -> {
          latch.countDown();
          return Schedule.ClusteredSingletonBuilder.fromSchedule(old, Duration.ofHours(1000))
              .build();
        };

    schedules[0] =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(testName.getMethodName() + 0)
            .scheduleModifier(scheduleModifierFn1)
            .build();
    incrementers[0] = new AtomicInteger();

    schedules[1] =
        Schedule.Builder.everySeconds(1L)
            .asClusteredSingleton(testName.getMethodName() + 1)
            .scheduleModifier(scheduleModifierFn2)
            .build();
    incrementers[1] = new AtomicInteger();

    final Cancellable[][] cancellables = new Cancellable[NUM_TEST_CLIENTS][2];
    for (int i = NUM_TEST_CLIENTS - 1; i >= 0; i--) {
      for (int j = 0; j < 2; j++) {
        cancellables[i][j] =
            testClients[i]
                .getSingletonScheduler()
                .schedule(schedules[j], incrementers[j]::incrementAndGet);
      }
    }
    latch.await();
    cancellables[NUM_TEST_CLIENTS - 1][0].cancel(false);
    Assertions.assertThat(incrementers[0].get()).isGreaterThan(1);
    Assertions.assertThat(incrementers[1].get()).isEqualTo(1);
  }

  private int doLockStep(Random rand) {
    CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          try {
            Thread.sleep(5 + rand.nextInt(100));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          incrementer.incrementAndGet();
          latch.countDown();
        };
    final Schedule schedule =
        Schedule.SingleShotBuilder.now()
            .asClusteredSingleton(testName.getMethodName())
            .inLockStep()
            .build();
    final AtomicInteger clientId = new AtomicInteger(100);
    List<CompletableFuture<Cancellable>> futures = new ArrayList<>();
    for (int i = 0; i < NUM_TEST_CLIENTS; i++) {
      final int client = i;
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  Thread.sleep(1 + rand.nextInt(10));
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                Cancellable sched =
                    testClients[client].getSingletonScheduler().schedule(schedule, task);
                if (sched.isScheduled()) {
                  try {
                    int oldVal = clientId.getAndSet(client);
                    Assertions.assertThat(oldVal).isEqualTo(100);
                    latch.await();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }
                return sched;
              }));
    }
    futures.forEach(
        (f) -> {
          try {
            Cancellable t = f.get();
            assertTrue(t.isDone() || t.isScheduled());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    return clientId.get();
  }
}
