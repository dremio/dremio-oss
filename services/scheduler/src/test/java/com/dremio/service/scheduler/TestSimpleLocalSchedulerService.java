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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Unit tests for {@code SimpleLocalSchedulerService}
 */
public class TestSimpleLocalSchedulerService {

  /**
   * Verify that threads created by the thread factory are daemon threads
   */
  @Test
  public void testThreadNameMatchesDefaultGroup() throws Exception {
    try (SimpleLocalSchedulerService service = new SimpleLocalSchedulerService(1)) {
      service.start();
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Thread> runnableThreadRef = new AtomicReference<>(null);
      service.schedule(Schedule.SingleShotBuilder.now().build(), () -> {
        Thread thread = Thread.currentThread();
        runnableThreadRef.set(thread);
        latch.countDown();
      });
      latch.await();
      final Thread thread = runnableThreadRef.get();
      assertNotNull(thread);
      assertTrue("thread should be a daemon thread", thread.isDaemon());
      assertTrue("thread name should start with scheduler-", thread.getName().startsWith("scheduler-"));
    }
  }

  @Test
  public void testBasicSchedule() throws Exception {
    try (SimpleLocalSchedulerService service = new SimpleLocalSchedulerService(1)) {
      service.start();
      final CountDownLatch firstLatch = new CountDownLatch(10);
      final CountDownLatch secondLatch = new CountDownLatch(1);
      final CountDownLatch thirdLatch = new CountDownLatch(20);
      final AtomicInteger counter = new AtomicInteger(0);
      service.schedule(Schedule.Builder.everyMillis(50).build(), () -> {
        final int last = counter.incrementAndGet();
        firstLatch.countDown();
        thirdLatch.countDown();
        if (last == 10) {
          try {
            secondLatch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      firstLatch.await();
      Assertions.assertThat(counter.get()).isEqualTo(10);
      secondLatch.countDown();
      thirdLatch.await();
      Assertions.assertThat(counter.get()).isGreaterThanOrEqualTo(20);
    }
  }

  @Test
  public void testMultipleSchedulesOnSingleThread() throws Exception {
    runMultipleSchedules(10, 1);
  }

  @Test
  public void testMultipleSchedulesOnThreadPool() throws Exception {
    runMultipleSchedules(100, 10);
  }

  @Test
  public void testScheduleCancelledBeforeRun() throws Exception {
    try (SimpleLocalSchedulerService service = new SimpleLocalSchedulerService(1)) {
      service.start();
      final AtomicInteger counter = new AtomicInteger(0);
      final Instant firstInstant = Instant.now().plusMillis(10 * 3600 * 1000);
      Cancellable task = service.schedule(Schedule.Builder.everyHours(10).startingAt(firstInstant).build(),
        counter::incrementAndGet);
      task.cancel(false);
      assertTrue(task.isCancelled());
    }
  }

  @Test
  public void testScheduleCancelledDuringRun() throws Exception {
    try (SimpleLocalSchedulerService service = new SimpleLocalSchedulerService(1)) {
      service.start();
      final CountDownLatch firstLatch = new CountDownLatch(1);
      final CountDownLatch secondLatch = new CountDownLatch(1);
      final CountDownLatch thirdLatch = new CountDownLatch(1);
      final AtomicInteger counter = new AtomicInteger(0);
      final AtomicBoolean interrupted = new AtomicBoolean(false);
      final Cancellable task = service.schedule(Schedule.Builder.everyMillis(50).build(), () -> {
        counter.incrementAndGet();
        firstLatch.countDown();
        try {
          secondLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          interrupted.set(true);
        }
        thirdLatch.countDown();
      });
      firstLatch.await();
      task.cancel(true);
      thirdLatch.await();
      assertTrue(interrupted.get());
      Assertions.assertThat(counter.get()).isEqualTo(1);
    }
  }

  private void runMultipleSchedules(int loopCount, int capacity) throws Exception {
    try (SimpleLocalSchedulerService service = new SimpleLocalSchedulerService(capacity)) {
      service.start();
      final CountDownLatch[] latches = new CountDownLatch[loopCount];
      final AtomicInteger[] counters = new AtomicInteger[loopCount];
      for (int i = 0; i < loopCount; i++) {
        latches[i] = new CountDownLatch(5);
        counters[i] = new AtomicInteger(0);
      }
      for (int i = 0; i < loopCount; i++) {
        final int currentIdx = i;
        service.schedule(Schedule.Builder.everyMillis(50).build(), () -> {
          counters[currentIdx].incrementAndGet();
          latches[currentIdx].countDown();
        });
      }
      for (int i = 0; i < loopCount; i++) {
        latches[i].await();
        Assertions.assertThat(counters[i].get()).isGreaterThanOrEqualTo(5);
      }
    }
  }
}
