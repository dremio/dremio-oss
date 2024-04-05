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
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the session expiration (which includes re-mastering) aspects of the new implementation of
 * the ClusteredSingleton
 */
public class TestClusteredSingletonSessionExpiration extends DremioTest {
  // number of schedules per test.
  private static final int NUM_TEST_CLIENTS = 2;

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
  public void testCleanupOnExpiration() throws Exception {
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    Schedule testSchedule =
        Schedule.Builder.everyMillis(100)
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(cleanupCount::incrementAndGet)
            .build();
    final CountDownLatch latch1 = new CountDownLatch(10);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
        };
    testClients[0].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();
    for (int i = 1; i < NUM_TEST_CLIENTS; i++) {
      testClients[i].getSingletonScheduler().schedule(testSchedule, task);
    }
    // inject session expiration to client 0
    testClients[0].injectSessionExpiration();
    latch1.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(10);
    assertThat(cleanupCount.get()).isEqualTo(1);
  }

  @Test
  public void testMultiClientExpiration() throws Exception {
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    Schedule testSchedule =
        Schedule.Builder.everyMillis(100)
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(cleanupCount::incrementAndGet)
            .build();
    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(5);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
          latch2.countDown();
        };
    testClients[1].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();
    testClients[0].getSingletonScheduler().schedule(testSchedule, task);
    // inject session expiration to client 0
    testClients[1].injectSessionExpiration();
    latch1.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(2);
    assertThat(cleanupCount.get()).isEqualTo(1);
    testClients[0].injectSessionExpiration();
    latch2.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(5);
    // not guaranteed in this case that the schedule would have switched over. So cannot
    // guarantee a cleanup count of 2
    assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(1);
  }

  @Ignore("Takes close to 40 secs to run this test. Run locally only")
  @Test
  public void testRemasteringOnExpiration() throws Exception {
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    Schedule testSchedule =
        Schedule.Builder.everyMillis(100)
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(cleanupCount::incrementAndGet)
            .build();
    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(5);
    final CountDownLatch latch3 = new CountDownLatch(10);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
          latch2.countDown();
          latch3.countDown();
        };
    testClients[1].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();
    testClients[0].getSingletonScheduler().schedule(testSchedule, task);
    // inject session expiration to client 0
    testClients[1].injectSessionExpiration();
    latch1.await();
    testClients[1].close();
    latch2.await();
    testClients[1] = new TestClient(1, zkServerResource.getConnectionString());
    testClients[1].getSingletonScheduler().schedule(testSchedule, task);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(5);
    assertThat(cleanupCount.get()).isEqualTo(1);
    testClients[0].injectSessionExpiration();
    latch3.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(10);
    assertThat(cleanupCount.get()).isEqualTo(2);
  }

  @Test
  public void testRemasteringAndCancelReUse() throws Exception {
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    Schedule testSchedule =
        Schedule.Builder.everyMillis(100)
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(cleanupCount::incrementAndGet)
            .build();
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(20);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          incrementer.incrementAndGet();
          latch1.countDown();
          latch2.countDown();
        };
    runTaskAndCancel(testSchedule, task, latch1, true);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(1);
    assertThat(cleanupCount.get()).isEqualTo(0);
    runTaskAndCancel(testSchedule, task, latch2, false);
    latch2.await();
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(20);
    assertThat(cleanupCount.get()).isEqualTo(0);
  }

  private void runTaskAndCancel(
      Schedule testSchedule, Runnable task, CountDownLatch latch, boolean injectSessionLoss)
      throws Exception {
    final Cancellable task0 = testClients[0].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();
    final Cancellable task1 = testClients[1].getSingletonScheduler().schedule(testSchedule, task);
    latch.await();
    task0.cancel(false);
    task1.cancel(false);
    if (injectSessionLoss) {
      testClients[1].injectSessionExpiration();
    }
  }
}
