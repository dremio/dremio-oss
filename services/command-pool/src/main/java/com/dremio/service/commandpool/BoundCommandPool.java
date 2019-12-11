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
package com.dremio.service.commandpool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.telemetry.api.metrics.Metrics;

/**
 * Bound implementation of {@link CommandPool}.<br>
 *
 * Underlying thread pool uses a priority queue and relies on the {@link Command} comparator to define the priority.
 */
class BoundCommandPool implements CommandPool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundCommandPool.class);

  private final ThreadPoolExecutor executorService;


  BoundCommandPool(final int poolSize) {
    this.executorService = new ThreadPoolExecutor(
      poolSize, poolSize, // limited pool of threads
      0, TimeUnit.SECONDS, // doesn't matter as number of threads never exceeds core size
      new PriorityBlockingQueue<>(),
      new NamedThreadFactory("bound-command")
    );
  }

  @Override
  public <V> CompletableFuture<V> submit(Priority priority, String descriptor, Command<V> command, boolean runInSameThread) {
    final long time = System.currentTimeMillis();
    final CommandWrapper<V> wrapper = new CommandWrapper<>(priority, descriptor, time, command);
    logger.debug("command {} created", descriptor);
    if (runInSameThread) {
      logger.debug("running command {} in the same calling thread", descriptor);
      wrapper.run();
    } else {
      executorService.execute(wrapper);
    }
    return wrapper.getFuture();
  }

  @Override
  public void start() throws Exception {
    Metrics.newGauge(Metrics.join("jobs","command_pool", "active_threads"), () -> executorService.getActiveCount());
    Metrics.newGauge(Metrics.join("jobs","command_pool", "queue_size"), () -> executorService.getQueue().size());
  }

  @Override
  public void close() throws Exception {
    CloseableSchedulerThreadPool.close(executorService, logger);
  }

}
