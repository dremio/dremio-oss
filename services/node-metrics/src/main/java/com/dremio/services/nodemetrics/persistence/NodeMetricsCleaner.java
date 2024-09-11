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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds and deletes all files that were created more than the configured number of retention days
 * ago. This operates based on the write timestamp denoted through the filenames. Note that
 * historical data will become eligible for deletion after the retention period elapses, but
 * depending on the compaction and recompaction schedule, they may not be immediately deleted since
 * the freshness check is done against the date the file was written and not the date represented by
 * the data.
 */
class NodeMetricsCleaner implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(NodeMetricsCleaner.class);

  private final Supplier<Boolean> isEnabled;
  private final Function<Instant, Schedule> getSchedule;
  private final long maxAgeDays;
  private final NodeMetricsStorage nodeMetricsStorage;
  private final SchedulerService schedulerService;
  private final Object syncObject;
  private final Clock clock;

  NodeMetricsCleaner(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long maxAgeDays,
      NodeMetricsStorage nodeMetricsStorage,
      SchedulerService schedulerService,
      Object syncObject) {
    this(
        isEnabled,
        getSchedule,
        maxAgeDays,
        nodeMetricsStorage,
        schedulerService,
        syncObject,
        Clock.systemUTC());
  }

  @VisibleForTesting
  NodeMetricsCleaner(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long maxAgeDays,
      NodeMetricsStorage nodeMetricsStorage,
      SchedulerService schedulerService,
      Object syncObject,
      Clock clock) {
    this.isEnabled = isEnabled;
    this.getSchedule = getSchedule;
    this.maxAgeDays = maxAgeDays;
    this.nodeMetricsStorage = nodeMetricsStorage;
    this.schedulerService = schedulerService;
    this.syncObject = syncObject;
    this.clock = clock;
  }

  @Override
  public void run() {
    if (!isEnabled.get()) {
      // The support key value may have changed since startup
      logger.info("Terminating node metrics scheduled cleaner due to support key being disabled");
      return;
    }
    // Synchronize to avoid race condition with the compacter
    synchronized (syncObject) {
      logger.info("Running node metrics cleaner scheduled task");
      try {
        runCleanup();
      } catch (Exception e) {
        logger.error("Unhandled error when cleaning up node metrics history", e);
      } finally {
        schedule(false);
      }
    }
  }

  public void start() {
    schedule(true);
  }

  private void schedule(boolean isStartup) {
    schedulerService.schedule(getSchedule.apply(getScheduleTime(isStartup)), this);
  }

  private void runCleanup() {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    try (DirectoryStream<NodeMetricsFile> directoryStream = nodeMetricsStorage.list()) {
      int numDeleted = 0;
      for (NodeMetricsFile file : directoryStream) {
        ZonedDateTime fileDateTime = file.getWriteTimestamp();
        if (fileDateTime.plusDays(maxAgeDays).isBefore(now)) {
          logger.debug(
              "Removing node history file '{}' with date time {}", file.getName(), fileDateTime);
          nodeMetricsStorage.delete(file);
          numDeleted++;
        }
      }
      logger.info(
          "Deleted {} node metrics file(s) that are older than the configured {} day retention period",
          numDeleted,
          maxAgeDays);
    } catch (IOException e) {
      logger.error("Could not clean up old node metrics files", e);
    }
  }

  private Instant getScheduleTime(boolean isStartup) {
    if (isStartup) {
      // Always run on startup to delete any metric files out of the retention period, no-op if
      // there's nothing old enough
      return Instant.now(clock);
    }
    // Otherwise schedule to run a day from now. This is independent of the specified retention
    // period (which is minimum 1 day)
    return Instant.now(clock).plus(Duration.ofDays(1));
  }
}
