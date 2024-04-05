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
package com.dremio.common.concurrent;

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** AutoCloseable implementation of {@link ScheduledThreadPoolExecutor} */
public class CloseableSchedulerThreadPool extends ScheduledThreadPoolExecutor
    implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CloseableSchedulerThreadPool.class);
  private final String name;

  public CloseableSchedulerThreadPool(String name, int corePoolSize) {
    super(corePoolSize, new NamedThreadFactory(name));
    this.name = name;
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    if (t != null) {
      logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
    }
    super.afterExecute(r, t);
  }

  @Override
  public void close() throws Exception {
    close(this, logger);
  }

  /**
   * Close a thread pool executor
   *
   * @param executor
   * @param logger
   */
  public static void close(ExecutorService executor, Logger logger) {
    executor.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
          logger.error("Pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      logger.warn("Executor interrupted while awaiting termination");

      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Wrap an {@code ExecutorService instance} into a {@code AutoCloseable}
   *
   * @param executor
   * @param logger
   * @return a {@code AutoCloseable} instance
   * @throws NullPointerException if executor or logger are {@code null}
   */
  public static AutoCloseable of(final ExecutorService executor, final Logger logger) {
    Preconditions.checkNotNull(executor);
    Preconditions.checkNotNull(logger);

    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        CloseableSchedulerThreadPool.close(executor, logger);
      }
    };
  }
}
