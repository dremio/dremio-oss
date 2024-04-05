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

import com.dremio.io.ExponentialBackoff;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CheckReturnValue;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple retrying utility */
@SuppressWarnings("checkstyle:FinalClass")
public class Retryer implements ExponentialBackoff {
  private static final Logger logger = LoggerFactory.getLogger(Retryer.class);

  public enum WaitStrategy {
    EXPONENTIAL,
    FLAT
  } // Can be extended

  private final Set<Class<? extends Exception>> retryableExceptionClasses = new HashSet<>();
  private WaitStrategy waitStrategy = WaitStrategy.EXPONENTIAL;
  private int maxRetries = 4; // default
  private int baseMillis = 250;
  private int maxMillis = 2_500;
  private boolean infiniteRetries;
  private final Function<Exception, Boolean> isExceptionClassRetriable =
      (ex) -> retryableExceptionClasses.stream().anyMatch(clz -> clz.isInstance(ex));
  private Function<Exception, Boolean> isRetriable = isExceptionClassRetriable;

  private Retryer() {}

  public <T> T call(Callable<T> callable) {
    for (int attemptNo = 1; infiniteRetries || (attemptNo <= maxRetries); attemptNo++) {
      try {
        return callable.call();
      } catch (Exception e) {
        checkRetriableException(attemptNo, e);
      }
    }

    // will ever reach here
    throw new OperationFailedAfterRetriesException();
  }

  public void run(Runnable runnable) {
    for (int attemptNo = 1; infiniteRetries || (attemptNo <= maxRetries); attemptNo++) {
      try {
        runnable.run();
        return;
      } catch (Exception e) {
        checkRetriableException(attemptNo, e);
      }
    }

    // will ever reach here
    throw new OperationFailedAfterRetriesException();
  }

  private void checkRetriableException(int attemptNo, Exception e) {
    boolean retryable = isRetriable.apply(e);
    if (!retryable || (!infiniteRetries && attemptNo == maxRetries)) {
      throw new OperationFailedAfterRetriesException(e);
    }
    final StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
    logger.warn(
        "Retry attempt {} for the failure at {}:{}:{}, Error - {}",
        attemptNo,
        caller.getClassName(),
        caller.getMethodName(),
        caller.getLineNumber(),
        e.getMessage());
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

  @Override
  public int getBaseMillis() {
    return baseMillis;
  }

  @Override
  public int getMaxMillis() {
    return maxMillis;
  }

  private void flatWait() {
    sleep(baseMillis);
  }

  @VisibleForTesting
  void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  public int getMaxRetries() {
    return maxRetries;
  }

  @CheckReturnValue
  public static Builder newBuilder() {
    return new Retryer.Builder();
  }

  public static final class Builder {
    private final Retryer retryer = new Retryer();

    private Builder() {
      // use static factory method newBuilder instead
    }

    public Builder retryIfExceptionOfType(Class<? extends Exception> clazz) {
      Preconditions.checkState(
          retryer.isRetriable == retryer.isExceptionClassRetriable,
          "Retryer does not support mix of exception class and exception function");
      retryer.retryableExceptionClasses.add(clazz);
      return this;
    }

    public Builder retryOnExceptionFunc(Function<Exception, Boolean> function) {
      Preconditions.checkState(
          retryer.retryableExceptionClasses.isEmpty(),
          "Retryer does not support mix of exception class and exception function");
      retryer.isRetriable = function;
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

    public Builder setInfiniteRetries(boolean infiniteRetries) {
      retryer.infiniteRetries = infiniteRetries;
      return this;
    }

    public Retryer build() {
      return retryer;
    }
  }

  public Retryer copy() {
    Retryer copy = new Retryer();
    copy.waitStrategy = waitStrategy;
    copy.baseMillis = baseMillis;
    copy.maxMillis = maxMillis;
    copy.maxRetries = maxRetries;
    copy.retryableExceptionClasses.addAll(retryableExceptionClasses);
    copy.isRetriable = isRetriable;
    return copy;
  }

  public static class OperationFailedAfterRetriesException extends RuntimeException {
    OperationFailedAfterRetriesException() {
      super();
    }

    OperationFailedAfterRetriesException(Exception e) {
      super(e);
    }

    public <T extends Exception> T getWrappedCause(
        Class<T> clazz, Function<Throwable, T> conversionFunc) {
      Throwable cause = getCause();
      return clazz.isInstance(cause) ? clazz.cast(cause) : conversionFunc.apply(cause);
    }
  }
}
