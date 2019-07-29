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
package com.dremio.service.execselector;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.service.Pointer;

/**
 * Unit test for QueueProcessor
 */
public class TestQueueProcessor {
  @Test
  public void testQueueProcessor() throws Exception {
    Pointer<Long> counter = new Pointer<>(0L);
    final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    Lock readLock = rwlock.readLock();
    Lock writeLock = rwlock.writeLock();

    QueueProcessor<Long> qp = new QueueProcessor<>("queue-processor",
      () -> new AutoCloseableLock(writeLock).open(),
      (i) -> counter.value += i);
    qp.start();
    final long totalCount = 100_000;
    final long numBatches = 10;
    final long batchCount = totalCount / numBatches;
    for (long i = 0; i < totalCount; i++) {
      qp.enqueue(new Long(i));
      if (i % batchCount == (batchCount - 1)) {
        Thread.sleep(1);
      }
    }
    final long timeout = 5_000; // 5s
    long loopCount = 0;
    long expectedValue = totalCount * (totalCount - 1) / 2;
    while (getValue(counter, readLock) != expectedValue) {
      Thread.sleep(1);
      assertTrue(String.format("Timed out after %d ms", timeout), loopCount++ < timeout);
    }
    qp.close();
  }

  private static long getValue(Pointer<Long> counter, Lock readLock) {
    try (AutoCloseableLock ignored = new AutoCloseableLock(readLock).open()) {
      return counter.value;
    }
  }
}
