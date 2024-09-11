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
import java.util.function.IntSupplier;
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
public class TestChangingWeights extends DremioTest {
  private static final int NUM_TEST_CLIENTS = 2;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(240, TimeUnit.SECONDS);

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

  /// run two schedules S1 and S2 on same node 0, start node 1 with S2 migrating; kill node 1;
  /// 2 should come back to node 0;
  @Test
  public void testMigrationBackChangingWeights() throws InterruptedException {
    final AtomicInteger[] incrementers = new AtomicInteger[3];
    final AtomicInteger[] cleanup = new AtomicInteger[3];
    final WeightChanger[] weightChangers = new WeightChanger[3];
    final Schedule[] testSchedules = new Schedule[3];
    for (int i = 0; i < 3; i++) {
      incrementers[i] = new AtomicInteger();
      cleanup[i] = new AtomicInteger();
      switch (i) {
        case 0:
          weightChangers[i] = new WeightChanger(55);
          break;
        case 1:
          weightChangers[i] = new WeightChanger(7);
          break;
        default:
          weightChangers[i] = new WeightChanger(6);
          break;
      }
      testSchedules[i] =
          Schedule.Builder.runOnceEverySwitchOver()
              .asClusteredSingleton(testName.getMethodName() + i)
              .withWeightProvider(weightChangers[i])
              .withCleanup(cleanup[i]::incrementAndGet)
              .build();
    }
    var latchesFor2 = new CountDownLatch[3];
    var latchesFor3 = new CountDownLatch[3];
    for (int i = 0; i < 3; i++) {
      latchesFor2[i] = new CountDownLatch(i + 1);
      latchesFor3[i] = new CountDownLatch(i + 1);
    }
    createSchedule(0, testSchedules[0], incrementers[0], null);
    createSchedule(0, testSchedules[1], incrementers[1], latchesFor2);
    createSchedule(0, testSchedules[2], incrementers[2], latchesFor3);
    Thread.yield();
    Thread.yield();
    Thread.yield();
    createSchedule(1, testSchedules[0], incrementers[0], null);
    createSchedule(1, testSchedules[1], incrementers[1], latchesFor2);
    createSchedule(1, testSchedules[2], incrementers[2], latchesFor3);

    // 2 and 3 will migrate to 1; weight 0 = 55(1); weight 1 = 13(2)
    latchesFor2[1].await();
    latchesFor3[1].await();

    Thread.sleep(1000);
    var schedulesOwnedByOne = numOwnedBy(1, testName.getMethodName(), 3);
    var schedulesOwnedByZero = numOwnedBy(0, testName.getMethodName(), 3);
    Assertions.assertThat(schedulesOwnedByOne).isEqualTo(2);
    Assertions.assertThat(schedulesOwnedByZero).isEqualTo(1);
    // now change the weight of the first one
    weightChangers[0].setCurrentWeight(1);
    // the weight 2 should migrate back
    latchesFor2[2].await();
    schedulesOwnedByOne = numOwnedBy(1, testName.getMethodName(), 3);
    schedulesOwnedByZero = numOwnedBy(0, testName.getMethodName(), 3);
    Assertions.assertThat(schedulesOwnedByOne).isEqualTo(1);
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

  private static final class WeightChanger implements IntSupplier {
    private volatile int currentWeight;

    private WeightChanger(int startWeight) {
      this.currentWeight = startWeight;
    }

    private void setCurrentWeight(int newWeight) {
      this.currentWeight = newWeight;
    }

    @Override
    public int getAsInt() {
      return currentWeight;
    }
  }
}
