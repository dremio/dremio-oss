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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;
import org.threeten.bp.temporal.TemporalAmount;

import com.google.common.annotations.VisibleForTesting;

/**
 * Simple implementation of {@link SchedulerService}
 *
 */
public class LocalSchedulerService implements SchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalSchedulerService.class);

  private final ScheduledExecutorService executorService;
  static final Schedule ONLY_ONCE_SCHEDULE = new Schedule() {
    @Override
    public TemporalAmount getPeriod() {
      return null;
    }

    @Override
    public Iterator<Instant> iterator() {
      return Collections.singleton(Instant.now()).iterator();
    }
  };

  /**
   * Thread factory for scheduler threads:
   * - threads are named scheduler-&lt;number&gt;
   * - threads are configured as daemon thread
   */
  @VisibleForTesting
  static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
    private final ThreadGroup threadGroup = new ThreadGroup("scheduler");
    private final AtomicInteger count = new AtomicInteger();
    @Override
    public Thread newThread(Runnable r) {
      Thread result = new Thread(threadGroup, r, format("scheduler-%d", count.getAndIncrement()));

      // Scheduler threads should not prevent JVM to exit
      result.setDaemon(true);

      return result;
    }
  };

  /**
   * Creates a new scheduler service.
   *
   * The underlying executor uses a {@link ThreadPoolExecutor}, with a core of 1.
   */
  public LocalSchedulerService() {
    this(Executors.newScheduledThreadPool(1, THREAD_FACTORY));
  }

  @VisibleForTesting
  LocalSchedulerService(ScheduledExecutorService executorService) {
    this.executorService = executorService;
  }

  @VisibleForTesting
  public ScheduledExecutorService getExecutorService() {
    return executorService;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Stopping SchedulerService");
    executorService.shutdown();
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
      executorService.schedule(this, ChronoUnit.MILLIS.between(Instant.now(), instant), TimeUnit.MILLISECONDS);
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

  @Override
  public Cancellable scheduleOnce(final Runnable task) {
    return schedule(ONLY_ONCE_SCHEDULE, task);
  }

}
