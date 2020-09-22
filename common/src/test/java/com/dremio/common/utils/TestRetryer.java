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

package com.dremio.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.dremio.common.util.Retryer;
import com.google.common.base.Stopwatch;


/**
 * Tests for Retryer
 */
public class TestRetryer {
  private static final int MAX_RETRIES = 4;

  @Test
  public void testMaxRetries() {
    Retryer<Boolean> retryer = new Retryer.Builder<Boolean>()
      .setWaitStrategy(Retryer.WaitStrategy.FLAT, 1, 1)
      .retryIfExceptionOfType(RuntimeException.class)
      .setMaxRetries(MAX_RETRIES).build();

    // Succeed only in last attempt. Throw exceptions before that.
    AtomicInteger counter = new AtomicInteger(0);
    boolean result = retryer.call(() -> {
      if (counter.incrementAndGet() < MAX_RETRIES) {
        throw new RuntimeException("Failure");
      } else if (counter.get() == MAX_RETRIES) {
        return true;
      } else {
        // Retry triggered even after success
        return false;
      }
    });
    assertTrue("Retry happened even without exception", result);
  }

  @Test
  public void testNoRetryAfterSuccess() {
    Retryer<Boolean> retryer = new Retryer.Builder<Boolean>()
      .setWaitStrategy(Retryer.WaitStrategy.FLAT, 1, 1)
      .retryIfExceptionOfType(RuntimeException.class)
      .setMaxRetries(MAX_RETRIES).build();

    final int succeedAfter = MAX_RETRIES / 2;

    // Succeed only in mid attempt. Throw exceptions before that.
    AtomicInteger counter = new AtomicInteger(0);
    boolean result = retryer.call(() -> {
      if (counter.incrementAndGet() < succeedAfter) {
        throw new RuntimeException("Failure");
      } else if (counter.get() == succeedAfter) {
        return true;
      } else {
        // Retry triggered even after success
        return false;
      }
    });
    assertTrue("Retry happened even without exception", result);
    assertEquals(counter.get(), succeedAfter);
  }

  @Test
  public void testFlatWaitStrategy() {
    final int expectedWait = 100;
    Retryer<Boolean> retryer = new Retryer.Builder<Boolean>()
      .setWaitStrategy(Retryer.WaitStrategy.FLAT, expectedWait, expectedWait)
      .retryIfExceptionOfType(RuntimeException.class)
      .setMaxRetries(MAX_RETRIES).build();

    AtomicInteger counter = new AtomicInteger(0);
    final Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      retryer.call(() -> {
        if (counter.incrementAndGet() == 1) {
          stopwatch.start();
        } else if (counter.get() > 1) {
          long elapsedTimeDiff = Math.abs(stopwatch.elapsed(TimeUnit.MILLISECONDS) - expectedWait);
          if (elapsedTimeDiff > 10) { //allow 10% variation
            fail("Elapsed time difference is more than expected: " + elapsedTimeDiff);
          }
          stopwatch.stop();
          stopwatch.reset();
          stopwatch.start();
        }
        throw new RuntimeException("Failure");
      });
    } catch (RuntimeException e) {
      // Nothing to do
    }
  }

  @Test(expected = RuntimeException.class)
  public void testRetryIfException() {
    Retryer<Boolean> retryer = new Retryer.Builder<Boolean>()
      .setWaitStrategy(Retryer.WaitStrategy.FLAT, 1, 1)
      .retryIfExceptionOfType(IOException.class)
      .retryIfExceptionOfType(SQLException.class)
      .setMaxRetries(MAX_RETRIES).build();

    // Throw IOException first. That should fall under retry. Other exceptions shouldn't
    AtomicInteger counter = new AtomicInteger(0);
    boolean result = retryer.call(() -> {
      if (counter.incrementAndGet() < (MAX_RETRIES - 2)) {
        throw new IOException("Should retry");
      } else if (counter.get() == (MAX_RETRIES - 2)) {
        throw new SQLException("Should retry");
      } else if (counter.get() == (MAX_RETRIES - 1)) {
        throw new RuntimeException("Should fail");
      } else {
        // Retry triggered even after success
        return false;
      }
    });

    // fail if didn't come out of call() with runtime exception.
    assertTrue(result);
  }
}
