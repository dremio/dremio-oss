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
package com.dremio.io;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.dremio.common.exceptions.ErrorHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.buffer.ByteBuf;

/**
 * Decorator over AsyncByteReader with timeout.
 */
public class AsyncByteReaderWithTimeout extends ReusableAsyncByteReader {
  static private ScheduledThreadPoolExecutor delayer;
  private AsyncByteReader inner;
  private long timeoutInMillis;

  static {
    delayer = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("timeoutAfter-%d")
      .build());
    // remove cancelled tasks from the queue to reduce heap usage.
    delayer.setRemoveOnCancelPolicy(true);
  }

  public AsyncByteReaderWithTimeout(AsyncByteReader inner, long timeoutInMillis) {
    this.inner = inner;
    this.timeoutInMillis = timeoutInMillis;
  }

  /**
   * Wrapper over TimeoutException, just to distinguish from the case that S3/azure clients also
   * generate a TimeoutException. In that case, the buffer can be safely released.
   */
  private static class AsyncTimeoutException extends TimeoutException {
    AsyncTimeoutException() {
      super();
    }
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    CompletableFuture<Void> future = within(inner.readFully(offset, dst, dstOffset, len),
      timeoutInMillis);
    future = future.whenComplete((result, throwable) -> {
      if (ErrorHelper.findWrappedCause(throwable, AsyncTimeoutException.class) != null) {
        // grab an extra ref on behalf of the read that may complete at any point
        // in the future and access the buf. This buf is essentially leaked.
        dst.retain();
      }
    });
    return future;
  }

  /**
   * if the future cannot complete within 'millis', fail with TimeoutException.
   */
  private static <T> CompletableFuture<T> within(CompletableFuture<T> future, long millis) {
    // schedule a task to generate a timeout after 'millis'
    final CompletableFuture<T> timeout = new CompletableFuture<>();
    ScheduledFuture timeoutTask = delayer.schedule(
      () -> timeout.completeExceptionally(new AsyncTimeoutException()), millis, TimeUnit.MILLISECONDS);

    // accept either the origin future or the timeout, which ever happens first. cancel the timeout
    // task in either case.
    return future.applyToEither(timeout, Function.identity())
      .whenComplete((x, y) -> timeoutTask.cancel(true));
  }

  @Override
  protected void onClose() throws Exception {
    inner.close();
  }

  @Override
  public List<ReaderStat> getStats() {
    return inner.getStats();
  }
}
