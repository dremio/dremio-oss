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
import com.dremio.services.nodemetrics.NodeMetrics;
import com.dremio.services.nodemetrics.NodeMetricsService;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collects node metrics for this current point in time and writes to dist */
class NodeMetricsWriter implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(NodeMetricsWriter.class);

  private final Supplier<Boolean> isEnabled;
  private final Function<Instant, Schedule> getSchedule;
  private final long scheduleIntervalMins;
  private final boolean executorsOnly;
  private final NodeMetricsCsvFormatter csvFormatter;
  private final NodeMetricsStorage nodeMetricsStorage;
  private final NodeMetricsService nodeMetricsService;
  private final SchedulerService schedulerService;
  private final Object syncObject;
  private final Clock clock;

  NodeMetricsWriter(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long scheduleIntervalMins,
      boolean executorsOnly,
      NodeMetricsCsvFormatter csvFormatter,
      NodeMetricsStorage nodeMetricsStorage,
      NodeMetricsService nodeMetricsService,
      SchedulerService schedulerService,
      Object syncObject) {
    this(
        isEnabled,
        getSchedule,
        scheduleIntervalMins,
        executorsOnly,
        csvFormatter,
        nodeMetricsStorage,
        nodeMetricsService,
        schedulerService,
        syncObject,
        Clock.systemUTC());
  }

  @VisibleForTesting
  NodeMetricsWriter(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long scheduleIntervalMins,
      boolean executorsOnly,
      NodeMetricsCsvFormatter csvFormatter,
      NodeMetricsStorage nodeMetricsStorage,
      NodeMetricsService nodeMetricsService,
      SchedulerService schedulerService,
      Object syncObject,
      Clock clock) {
    this.isEnabled = isEnabled;
    this.getSchedule = getSchedule;
    this.scheduleIntervalMins = scheduleIntervalMins;
    this.executorsOnly = executorsOnly;
    this.csvFormatter = csvFormatter;
    this.nodeMetricsStorage = nodeMetricsStorage;
    this.nodeMetricsService = nodeMetricsService;
    this.schedulerService = schedulerService;
    this.syncObject = syncObject;
    this.clock = clock;
  }

  @Override
  public void run() {
    if (!isEnabled.get()) {
      // The support key value may have changed since startup
      logger.info("Terminating node metrics scheduled writer due to support key being disabled");
      return;
    }

    // Synchronize to avoid race condition with the compacter
    synchronized (syncObject) {
      logger.debug("Running node metrics writer scheduled task"); // debug level due to frequency
      try {
        List<NodeMetrics> nodeMetricsList = nodeMetricsService.getClusterMetrics(!executorsOnly);
        logger.debug("Collected node metrics for {} nodes", nodeMetricsList.size());
        writeMetrics(nodeMetricsList);
      } catch (Exception e) {
        logger.error("Error when collecting or writing node metrics", e);
      } finally {
        schedule();
      }
    }
  }

  public void start() {
    schedule();
  }

  private void schedule() {
    schedulerService.schedule(getSchedule.apply(getScheduleTime()), this);
  }

  private Instant getScheduleTime() {
    return Instant.now(clock).plus(Duration.ofMinutes(scheduleIntervalMins));
  }

  private void writeMetrics(List<NodeMetrics> nodeMetricsList) throws IOException {
    if (nodeMetricsList.isEmpty()) {
      logger.warn("No node metrics were written because no data was received for cluster nodes");
      return;
    }

    NodeMetricsFile nodeMetricsFile = NodeMetricsPointFile.newInstance();

    String csv = csvFormatter.toCsv(nodeMetricsList);
    try (final InputStream csvContents = IOUtils.toInputStream(csv, StandardCharsets.UTF_8)) {
      nodeMetricsStorage.write(csvContents, nodeMetricsFile);
    }
    logger.trace(
        "Wrote node metrics file {} with data for {} node(s)",
        nodeMetricsFile.getName(),
        nodeMetricsList.size());
  }
}
