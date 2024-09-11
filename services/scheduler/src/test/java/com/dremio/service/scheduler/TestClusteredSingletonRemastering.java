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
 * Tests the session expiration and remastering aspects of the new implementation of the
 * ClusteredSingleton
 */
@Ignore(
    "Takes several minutes to run this test. Remove this line and run manually when a relevant change is made")
public class TestClusteredSingletonRemastering extends DremioTest {
  // number of schedules per test.
  private static final int NUM_TEST_CLIENTS = 2;
  private static final int ML = 4;
  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(600, TimeUnit.SECONDS);

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
  public void testRemasteringOnExpirationNormalSchedule() throws Exception {
    final AtomicInteger currentIndex = new AtomicInteger(0);
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final CountDownLatch[] cleanupLatch1 = initLatch(1);
    final CountDownLatch[] cleanupLatch2 = initLatch(2);
    Schedule testSchedule =
        Schedule.Builder.everyMillis(100)
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(
                () -> {
                  int idx = currentIndex.get();
                  cleanupCount.incrementAndGet();
                  if (idx < ML) {
                    cleanupLatch1[idx].countDown();
                    cleanupLatch2[idx].countDown();
                  }
                })
            .build();
    final CountDownLatch[] latch1 = initLatch(2);
    final CountDownLatch[] latch2 = initLatch(5);
    final CountDownLatch[] latch3 = initLatch(10);
    final CountDownLatch[] latch4 = initLatch(50);
    final CountDownLatch[] latch5 = initLatch(100);
    final CountDownLatch[] counterLatch1 = initLatch(1);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable task =
        () -> {
          int idx = currentIndex.get();
          if (idx >= ML) {
            // task is done
            return;
          }
          if (incrementer.get() == 2) {
            try {
              counterLatch1[idx].await();
            } catch (InterruptedException e) {
            }
          }
          incrementer.incrementAndGet();
          latch1[idx].countDown();
          latch2[idx].countDown();
          latch3[idx].countDown();
          latch4[idx].countDown();
          latch5[idx].countDown();
        };
    testClients[1].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();
    testClients[0].getSingletonScheduler().schedule(testSchedule, task);
    Thread.yield();

    doTestLoop(
        currentIndex,
        counterLatch1,
        cleanupLatch1,
        cleanupLatch2,
        latch1,
        latch2,
        latch3,
        latch4,
        latch5);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(100 * ML);
    assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(2 * ML);
  }

  @Test
  public void testRemasteringOnExpirationChainedSchedule() throws Exception {
    final AtomicInteger currentIndex = new AtomicInteger(0);
    final AtomicInteger cleanupCount = new AtomicInteger(0);
    final CountDownLatch[] cleanupLatch1 = initLatch(1);
    final CountDownLatch[] cleanupLatch2 = initLatch(2);
    Schedule testSchedule =
        Schedule.Builder.singleShotChain()
            .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 100))
            .asClusteredSingleton(testName.getMethodName())
            .withCleanup(
                () -> {
                  int idx = currentIndex.get();
                  cleanupCount.incrementAndGet();
                  cleanupLatch1[idx].countDown();
                  cleanupLatch2[idx].countDown();
                })
            .sticky()
            .build();
    final CountDownLatch[] latch1 = initLatch(2);
    final CountDownLatch[] latch2 = initLatch(5);
    final CountDownLatch[] latch3 = initLatch(10);
    final CountDownLatch[] latch4 = initLatch(50);
    final CountDownLatch[] latch5 = initLatch(100);
    final CountDownLatch[] counterLatch1 = initLatch(1);
    AtomicInteger incrementer = new AtomicInteger();
    Runnable[] tasks = new Runnable[1];
    tasks[0] =
        () -> {
          int idx = currentIndex.get();
          if (idx >= ML) {
            // task is done
            return;
          }
          if (incrementer.get() == 2) {
            try {
              counterLatch1[idx].await();
            } catch (InterruptedException e) {
            }
          }
          incrementer.incrementAndGet();
          latch1[idx].countDown();
          latch2[idx].countDown();
          latch3[idx].countDown();
          latch4[idx].countDown();
          latch5[idx].countDown();
          final Schedule t =
              Schedule.Builder.singleShotChain()
                  .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + 100))
                  .asClusteredSingleton(testName.getMethodName())
                  .withCleanup(
                      () -> {
                        int idx1 = currentIndex.get();
                        cleanupCount.incrementAndGet();
                        if (idx1 < ML) {
                          cleanupLatch1[idx1].countDown();
                          cleanupLatch2[idx1].countDown();
                        }
                      })
                  .sticky()
                  .build();
          for (int i = 0; i < 2; i++) {
            testClients[i].getSingletonScheduler().schedule(t, tasks[0]);
          }
        };
    testClients[1].getSingletonScheduler().schedule(testSchedule, tasks[0]);
    Thread.yield();
    testClients[0].getSingletonScheduler().schedule(testSchedule, tasks[0]);

    doTestLoop(
        currentIndex,
        counterLatch1,
        cleanupLatch1,
        cleanupLatch2,
        latch1,
        latch2,
        latch3,
        latch4,
        latch5);
    assertThat(incrementer.get()).isGreaterThanOrEqualTo(100 * ML);
    assertThat(cleanupCount.get()).isGreaterThanOrEqualTo(2 * ML);
  }

  // does a controlled switch back and forth between the two clustered singleton instances
  private void doTestLoop(
      AtomicInteger currentIndex,
      CountDownLatch[] counterLatch1,
      CountDownLatch[] cleanupLatch1,
      CountDownLatch[] cleanupLatch2,
      CountDownLatch[] latch1,
      CountDownLatch[] latch2,
      CountDownLatch[] latch3,
      CountDownLatch[] latch4,
      CountDownLatch[] latch5)
      throws Exception {
    for (int i = 0; i < ML; i++) {
      int idx = currentIndex.get();
      latch1[idx].await();

      // inject session expiration to client 1 after instructing the client 1 scheduler to act dead
      // and asking client 1 to ignore reconnects to avoid immediate reconnects
      testClients[1].getSingletonScheduler().actDead();
      testClients[0].getSingletonScheduler().ignoreReconnects();
      testClients[1].injectSessionExpiration();

      // now wait for the first cleanup to be called
      cleanupLatch1[idx].await();
      counterLatch1[idx].countDown();

      // wait for recovery to kick in post session expiration and schedules to start on singleton
      // instance 0
      latch2[idx].await();

      // bring singleton 1 alive so that it can now rejoin back on a reconnect. Ask singleton 0 to
      // stop
      // ignoring reconnect requests from singleton 1 so that cluster can now have 2 members
      testClients[1].getSingletonScheduler().bringAlive();
      testClients[0].getSingletonScheduler().allowReconnects();
      testClients[1].injectSessionExpiration();

      // wait for a few more schedules to run in the 2 member cluster
      latch3[idx].await();

      // Now it is singleton's 0 turn to act dead and inject session expiration. Here now singleton
      // 1 will
      // ignore reconnects/rejoins from singleton 0
      testClients[0].getSingletonScheduler().actDead();
      testClients[1].getSingletonScheduler().ignoreReconnects();
      testClients[0].injectSessionExpiration();

      // wait for the second cleanup to complete.
      cleanupLatch2[idx].await();

      // wait for some more schedules to run post recovery on the 1 node cluster
      latch4[idx].await();

      // now ask instance 1 to allow instance 0 to join post another session expiration
      testClients[0].getSingletonScheduler().bringAlive();
      testClients[1].getSingletonScheduler().allowReconnects();
      testClients[0].injectSessionExpiration();

      // run for some more time in the 2 node cluster
      latch5[idx].await();

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
}
