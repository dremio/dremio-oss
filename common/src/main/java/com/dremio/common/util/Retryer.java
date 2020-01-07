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

package com.dremio.common.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import com.dremio.io.ExponentialBackoff;

/**
 * Simple retrying utility
 *
 * @param <T>
 */
public final class Retryer<T> extends ExponentialBackoff {
  public enum WaitStrategy {EXPONENTIAL, FLAT}  //Can be extended

  private Set<Class<? extends Exception>> retryableExceptionClasses = new HashSet<>();
  private WaitStrategy waitStrategy = WaitStrategy.EXPONENTIAL;
  private int maxRetries = 4; // default
  private int baseMillis = 250;
  private int maxMillis = 2_500;

  private Retryer() {
  }

  public T call(Callable<T> callable) {
    for (int attemptNo = 1; attemptNo <= maxRetries; attemptNo++) {
      try {
        return callable.call();
      } catch (Exception e) {
        boolean retryable = retryableExceptionClasses.stream().anyMatch(clz -> clz.isInstance(e));
        if (!retryable || attemptNo == maxRetries) {
          throw new OperationFailedAfterRetriesException(e);
        }

        switch (waitStrategy) {
          case EXPONENTIAL:
            backoffWait(attemptNo);
            break;
          case FLAT:
            flatWait();
            break;
          default:
            throw new UnsupportedOperationException("Strategy not implemented: " + waitStrategy.name());
        }
      }
    }

    // will ever reach here
    throw new OperationFailedAfterRetriesException();
  }

  @Override
  protected int getBaseMillis() {
    return baseMillis;
  }

  @Override
  protected int getMaxMillis() {
    return maxMillis;
  }

  private void flatWait() {
    try {
      Thread.sleep((long)baseMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static class Builder {
    private Retryer retryer = new Retryer();

    public Builder retryIfExceptionOfType(Class<? extends Exception> clazz) {
      retryer.retryableExceptionClasses.add(clazz);
      return this;
    }

    public Builder setWaitStrategy(WaitStrategy waitStrategy, int baseMillis, int maxMillis) {
      retryer.waitStrategy = waitStrategy;
      retryer.baseMillis = baseMillis;
      retryer.maxMillis = maxMillis;
      return this;
    }

    public Builder setMaxRetries(int maxRetries) {
      retryer.maxRetries = maxRetries;
      return this;
    }

    public Retryer build() {
      return retryer;
    }
  }

  public static class OperationFailedAfterRetriesException extends RuntimeException {
    OperationFailedAfterRetriesException() {
      super();
    }

    OperationFailedAfterRetriesException(Exception e) {
      super(e);
    }
  }
}
