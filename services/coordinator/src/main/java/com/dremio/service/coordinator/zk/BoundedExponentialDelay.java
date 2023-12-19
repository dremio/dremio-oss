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
package com.dremio.service.coordinator.zk;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

import com.google.common.base.Preconditions;

/**
 * Custom implementation of {@link BoundedExponentialBackoffRetry} with unlimited retries.<br>
 *
 * Retry policy that retries indefinitely with an increasing (up to a maximum bound) sleep time between retries<br>
 * assuming base sleep time is 1s:<br>
 * first retry sleeps 1s,<br>
 * 2nd retry sleeps "up to" 2s,<br>
 * 3rd retry sleeps up to 4s,<br>
 * ...
 */
public class BoundedExponentialDelay implements RetryPolicy {

  private final int baseSleepTimeMs;
  private final int maxSleepMs;
  private final long maxRetry;

  public BoundedExponentialDelay(int baseSleepTimeMs, int maxSleepMs, boolean unlimited, long maxRetry) {
    Preconditions.checkArgument(baseSleepTimeMs > 0, "baseSleepTimeMs must be positive");
    Preconditions.checkArgument(maxSleepMs > baseSleepTimeMs, "maxSleepMs > baseSleepTimeMs");
    Preconditions.checkArgument(unlimited || maxRetry > 0, "maxRetry must be positive");
    this.baseSleepTimeMs = baseSleepTimeMs;
    this.maxSleepMs = maxSleepMs;
    this.maxRetry = unlimited ? -1 : maxRetry;
  }

  @Override
  public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
    if (maxRetry != -1 && maxRetry < retryCount) {
      return false;
    }

    try {
      sleeper.sleepFor(getSleepTimeMs(retryCount), TimeUnit.MILLISECONDS);
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  private int getSleepTimeMs(int retryCount) {
    // copied from Hadoop's public class ExponentialBackoffRetry, but slightly modified
    final int exponent = Math.min(29, retryCount) + 1; // make sure we don't overflow
    // pick a random number between 1 and 2^exponent
    final int multiplier = Math.max(1, ThreadLocalRandom.current().nextInt(1 << exponent));
    // compute next sleep time
    return Math.min(maxSleepMs, baseSleepTimeMs * multiplier);
  }
}
