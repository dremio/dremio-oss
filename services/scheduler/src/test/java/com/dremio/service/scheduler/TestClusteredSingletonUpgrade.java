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
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the upgrade aspects of the new implementation of the ClusteredSingleton */
public class TestClusteredSingletonUpgrade extends DremioTest {
  // number of schedules per test.
  private static final int NUM_TEST_CLIENTS_AT_START = 3;
  private static final int NUM_UPGRADE_CLIENTS = 3;
  private static final int TOTAL_CLIENTS = NUM_UPGRADE_CLIENTS + NUM_TEST_CLIENTS_AT_START;

  @ClassRule
  public static final ZkTestServerRule zkServerResource = new ZkTestServerRule("/css/dremio");

  private final ScheduleTaskGroup taskGroup = new TestClient.TestGroup(1);

  private final TestClient[] testClients = new TestClient[TOTAL_CLIENTS];

  @SuppressWarnings("resource")
  @Before
  public void setup() throws Exception {
    for (int i = 0; i < NUM_TEST_CLIENTS_AT_START; i++) {
      testClients[i] = new TestClient(i, zkServerResource.getConnectionString());
      testClients[i].getSingletonScheduler().addTaskGroup(taskGroup);
    }
    for (int i = 0; i < NUM_UPGRADE_CLIENTS; i++) {
      testClients[i + NUM_TEST_CLIENTS_AT_START] = null;
    }
  }

  @After
  public void tearDown() throws Exception {
    Arrays.stream(testClients).filter(Objects::nonNull).forEach(TestClient::close);
  }

  @Test
  public void testSingleShotRunOnceOnUpgrade() throws Exception {
    Schedule testSchedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
            .asClusteredSingleton(testName.getMethodName())
            .setSingleShotType(Schedule.SingleShotType.RUN_ONCE_EVERY_UPGRADE)
            .build();
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(2);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
          latch2.countDown();
        };
    for (int i = 0; i < NUM_TEST_CLIENTS_AT_START; i++) {
      testClients[i].getSingletonScheduler().schedule(testSchedule, task);
    }
    latch1.await();
    assertThat(incrementer.get()).isEqualTo(1);
    bringUpNewVersionClients();
    startSchedulesOnUpgradeClients(testSchedule, task);
    stopOldVersionClients();
    latch2.await();
    assertThat(incrementer.get()).isEqualTo(2);
  }

  @Test
  public void testSingleShotRunOnceAlways() throws Exception {
    Schedule testSchedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    final CountDownLatch latch1 = new CountDownLatch(1);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
        };
    for (int i = 0; i < NUM_TEST_CLIENTS_AT_START; i++) {
      testClients[i].getSingletonScheduler().schedule(testSchedule, task);
    }
    latch1.await();
    assertThat(incrementer.get()).isEqualTo(1);
    bringUpNewVersionClients();
    startSchedulesOnUpgradeClients(testSchedule, task);
    stopOldVersionClients();
    assertThat(incrementer.get()).isEqualTo(1);
  }

  @Test
  public void testSingleShotToMultiShotUpgrade() throws Exception {
    Schedule testSchedule =
        Schedule.SingleShotBuilder.at(Instant.ofEpochMilli(System.currentTimeMillis() + 500))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(10);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
        };
    for (int i = 0; i < NUM_TEST_CLIENTS_AT_START; i++) {
      testClients[i].getSingletonScheduler().schedule(testSchedule, task);
    }
    latch1.await();
    assertThat(incrementer.get()).isEqualTo(1);
    bringUpNewVersionClients();
    Schedule testSchedule1 =
        Schedule.Builder.singleShotChain()
            .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 50))
            .asClusteredSingleton(testName.getMethodName())
            .build();
    final Runnable[] task1 = new Runnable[1];
    task1[0] =
        () -> {
          incrementer.incrementAndGet();
          latch2.countDown();
          for (int j = NUM_TEST_CLIENTS_AT_START; j < TOTAL_CLIENTS; j++) {
            testClients[j]
                .getSingletonScheduler()
                .schedule(
                    Schedule.Builder.singleShotChain()
                        .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 50))
                        .asClusteredSingleton(testName.getMethodName())
                        .build(),
                    task1[0]);
          }
        };
    startSchedulesOnUpgradeClients(testSchedule1, task1[0]);
    stopOldVersionClients();
    latch2.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(10);
  }

  private void bringUpNewVersionClients() {
    for (int i = 0; i < NUM_UPGRADE_CLIENTS; i++) {
      testClients[i + NUM_TEST_CLIENTS_AT_START] =
          new TestClient(i, zkServerResource.getConnectionString(), "upgrade-version");
      testClients[i].getSingletonScheduler().addTaskGroup(taskGroup);
    }
  }

  private void startSchedulesOnUpgradeClients(Schedule testSchedule, Runnable task) {
    for (int i = 0; i < NUM_UPGRADE_CLIENTS; i++) {
      testClients[i + NUM_TEST_CLIENTS_AT_START]
          .getSingletonScheduler()
          .schedule(testSchedule, task);
    }
  }

  private void stopOldVersionClients() {
    for (int i = 0; i < NUM_TEST_CLIENTS_AT_START; i++) {
      testClients[i].close();
      testClients[i] = null;
    }
    try {
      // pause for 1/2 a second just to ensure, surviving nodes is seeing the close
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }
  }
}
