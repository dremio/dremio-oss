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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Closeable thread pool. This is an ThreadPoolExecutor/ExecutorService that is using a
 * NamedThreadFactory and calling awaitTermination on close.
 */
public class CloseableThreadPool extends ThreadPoolExecutor implements CloseableExecutorService {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CloseableThreadPool.class);

  /**
   * @see java.util.concurrent.Executors#newSingleThreadExecutor()
   */
  public static CloseableThreadPool newSingleThreadExecutor(String name) {
    return newFixedThreadPool(name, 1);
  }

  /**
   * @see java.util.concurrent.Executors#newFixedThreadPool(int)
   */
  public static CloseableThreadPool newFixedThreadPool(String name, int fixedPoolSize) {
    // same params as java.util.concurrent.Executors.newFixedThreadPool
    return new CloseableThreadPool(
        name, fixedPoolSize, fixedPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  /**
   * @see java.util.concurrent.Executors#newCachedThreadPool()
   */
  public static CloseableThreadPool newCachedThreadPool(String name) {
    return new CloseableThreadPool(name);
  }

  /**
   * @see java.util.concurrent.Executors#newCachedThreadPool()
   * @deprecated use {@link #newCachedThreadPool(String)} instead for clarity
   */
  @Deprecated
  public CloseableThreadPool(String name) {
    // same params as java.util.concurrent.Executors.newCachedThreadPool
    this(name, 0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
  }

  public CloseableThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(
        corePoolSize,
        maximumPoolSize,
        keepAliveTime,
        unit,
        workQueue,
        new NamedThreadFactory(name));
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    if (t != null) {
      logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
    }
    super.afterExecute(r, t);
  }

  @Override
  public void close() {
    CloseableSchedulerThreadPool.close(this, logger);
  }
}
