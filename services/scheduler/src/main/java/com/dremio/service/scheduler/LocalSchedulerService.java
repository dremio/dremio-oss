/*
 * Copyright (C) 2017 Dremio Corporation
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

import static java.lang.String.format;

import java.util.Iterator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;

import com.google.common.annotations.VisibleForTesting;

/**
 * Simple implementation of {@link SchedulerService}
 *
 */
public class LocalSchedulerService implements SchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalSchedulerService.class);
  private static final String THREAD_NAME_PREFIX = "scheduler-";

  private final CloseableSchedulerThreadPool executorService;

  /**
   * Creates a new scheduler service.
   *
   * The underlying executor uses a {@link ThreadPoolExecutor}, with a given pool size.
   *
   * @param corePoolSize -- the <b>maximum</b> number of threads used by the underlying {@link ThreadPoolExecutor}
   */
  public LocalSchedulerService(int corePoolSize) {
    this(new CloseableSchedulerThreadPool(THREAD_NAME_PREFIX, corePoolSize));
  }

  @VisibleForTesting
  LocalSchedulerService(CloseableSchedulerThreadPool executorService) {
    this.executorService = executorService;
  }

  @VisibleForTesting
  public CloseableSchedulerThreadPool getExecutorService() {
    return executorService;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Stopping SchedulerService");
    executorService.close();
    LOGGER.info("Stopped SchedulerService");
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("SchedulerService is up");
  }

  private class CancellableTask implements Cancellable, Runnable {
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final Iterator<Instant> instants;
    private final Runnable task;
    private Instant lastRun = Instant.MIN;

    private final AtomicReference<ScheduledFuture<?>> currentTask = new AtomicReference<>(null);

    public CancellableTask(Schedule schedule, Runnable task) {
      this.instants = schedule.iterator();
      this.task = task;
    }

    @Override
    public void run() {
      if (cancelled.get()) {
        return;
      }

      try {
        task.run();
      } catch(Exception e) {
        LOGGER.warn(format("Execution of task %s failed", task.toString()), e);
      }
      lastRun = Instant.now();
      scheduleNext();
    }

    private void scheduleNext() {
      if (cancelled.get()) {
        return;
      }

      Instant instant = nextInstant();
      ScheduledFuture<?> future = executorService.schedule(this, ChronoUnit.MILLIS.between(Instant.now(), instant), TimeUnit.MILLISECONDS);
      currentTask.set(future);
    }

    private Instant nextInstant() {
      Instant result = null;
      while(instants.hasNext()) {
        result = instants.next();
        if (!result.isBefore(lastRun)) {
          break;
        }
      }
      return result;
    }

    @Override
    public void cancel() {
      if (cancelled.getAndSet(true)) {
        // Already cancelled
        return;
      }

      LOGGER.info(format("Cancelling task %s", task.toString()));
      ScheduledFuture<?> future = currentTask.getAndSet(null);
      if (future != null) {
        future.cancel(true);
      }
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }
  }
  @Override
  public Cancellable schedule(Schedule schedule, final Runnable task) {
    CancellableTask cancellableTask = new CancellableTask(schedule, task);
    cancellableTask.scheduleNext();

    return cancellableTask;
  }

}
