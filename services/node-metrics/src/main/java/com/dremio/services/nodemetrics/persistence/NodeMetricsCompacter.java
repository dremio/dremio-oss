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

import com.dremio.exec.ExecConstants;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combines all point-in-time node metrics files into a single aggregate file, or all
 * singly-compacted files into a single aggregate file, depending on the specified compaction type
 */
class NodeMetricsCompacter implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(NodeMetricsCompacter.class);

  private final Supplier<Boolean> isEnabled;
  private final Function<Instant, Schedule> getSchedule;
  private final long scheduleIntervalMins;
  private final CompactionType compactionType;
  private final NodeMetricsStorage nodeMetricsStorage;
  private final SchedulerService schedulerService;
  private final Object syncObject;
  private final Clock clock;

  NodeMetricsCompacter(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long scheduleIntervalMins,
      CompactionType compactionType,
      NodeMetricsStorage nodeMetricsStorage,
      SchedulerService schedulerService,
      Object syncObject) {
    this(
        isEnabled,
        getSchedule,
        scheduleIntervalMins,
        compactionType,
        nodeMetricsStorage,
        schedulerService,
        syncObject,
        Clock.systemUTC());
  }

  @VisibleForTesting
  NodeMetricsCompacter(
      Supplier<Boolean> isEnabled,
      Function<Instant, Schedule> getSchedule,
      long scheduleIntervalMins,
      CompactionType compactionType,
      NodeMetricsStorage nodeMetricsStorage,
      SchedulerService schedulerService,
      Object syncObject,
      Clock clock) {
    this.isEnabled = isEnabled;
    this.getSchedule = getSchedule;
    this.scheduleIntervalMins = scheduleIntervalMins;
    this.compactionType = compactionType;
    this.nodeMetricsStorage = nodeMetricsStorage;
    this.schedulerService = schedulerService;
    this.syncObject = syncObject;
    this.clock = clock;
  }

  @Override
  public void run() {
    if (!isEnabled.get()) {
      logger.info(
          "Terminating node metrics scheduled compaction runner due to option {} being disabled",
          ExecConstants.NODE_HISTORY_ENABLED.getOptionName());
      return;
    }
    if (compactionType != CompactionType.Single && compactionType != CompactionType.Double) {
      throw new IllegalStateException("Unsupported compaction type: " + compactionType);
    }
    // Synchronize to avoid race condition with the writer/cleaner/other compaction schedule
    // runner
    synchronized (syncObject) {
      logger.info("Running node metrics compacter scheduled task");
      try {
        runCompaction();
      } catch (Exception e) {
        logger.error("Unhandled error when compacting node metrics history", e);
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

  private Instant getScheduleTime(boolean isStartup) {
    if (isStartup && shouldRunOnStartup()) {
      return Instant.now(clock);
    } else {
      return Instant.now(clock).plus(Duration.ofMinutes(scheduleIntervalMins));
    }
  }

  private boolean shouldRunOnStartup() {
    Optional<NodeMetricsFile> oldestEligible = getOldestEligibleFile();
    if (oldestEligible.isEmpty()) {
      // No files to compact: don't run
      logger.debug(
          "Scheduling compaction for later as there are not enough eligible files to compact immediately");
      return false;
    }

    // If it has been more than the designated period since we wrote the oldest point file (&
    // we're doing single compaction) or ran single compaction (& we're doing double compaction),
    // it is due to run.
    // This may be the case if the coordinator was shut down for enough time.
    ZonedDateTime writtenTime = oldestEligible.get().getWriteTimestamp();
    ZonedDateTime currentDateTime = ZonedDateTime.now(writtenTime.getZone());
    return writtenTime.plusMinutes(scheduleIntervalMins).isBefore(currentDateTime);
  }

  /**
   * Runs compaction. Note: If query planning completes, and compaction runs (deleting some files
   * listed as part of the nodes history dataset metadata) in the period between then and when query
   * execution begins, this is a benign race condition as query planning will automatically be
   * re-triggered when execution finds that some dataset CSV files are missing. So no special
   * handling is done to prevent this race condition.
   */
  private void runCompaction() {
    try (DirectoryStream<NodeMetricsFile> directoryStream = listEligibleFiles()) {
      Iterator<NodeMetricsFile> it = directoryStream.iterator();
      if (!it.hasNext()) {
        logger.debug(
            "Skipping compaction of node metrics files as there are not enough to compact");
        return;
      }

      // First copy all files to compact to an in-memory buffer. We don't sequentially read/write
      // to the file output stream since the blob stores like S3 don't support appending
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      List<NodeMetricsFile> filesCompacted = new ArrayList<>();
      while (it.hasNext()) {
        NodeMetricsFile file = it.next();
        try (final InputStream is = nodeMetricsStorage.open(file)) {
          IOUtils.copy(is, buffer);
        }
        filesCompacted.add(file);
      }

      NodeMetricsFile nodeMetricsFile = NodeMetricsCompactedFile.newInstance(compactionType);
      nodeMetricsStorage.write(buffer, nodeMetricsFile);

      // There is a slim but non-zero chance that query planning and execution could run in the
      // window between writing the compacted file and deleting the raw files, in which case the
      // dataset would include the individual history files as well as the compacted file. This is
      // handled during execution by filtering out duplicate rows.
      for (NodeMetricsFile file : filesCompacted) {
        nodeMetricsStorage.delete(file);
      }
      logger.info("Compacted {} node metrics files", filesCompacted.size());
    } catch (IOException e) {
      logger.error("Error when running compaction", e);
    }
  }

  private DirectoryStream<NodeMetricsFile> listEligibleFiles() throws IOException {
    CompactionType eligibleToCompact =
        compactionType == CompactionType.Single
            ? CompactionType.Uncompacted
            : CompactionType.Single;
    return nodeMetricsStorage.list(eligibleToCompact);
  }

  private boolean hasEnough(DirectoryStream<NodeMetricsFile> directoryStream) {
    // We don't need a precise count, only whether there are 2 or more uncompacted files
    return Iterables.size(Iterables.limit(directoryStream, 2)) >= 2;
  }

  private Optional<NodeMetricsFile> getOldestEligibleFile() {
    try (DirectoryStream<NodeMetricsFile> directoryStream = listEligibleFiles()) {
      if (!hasEnough(directoryStream)) {
        // Nothing eligible for compaction if there are not enough files to compact
        return Optional.empty();
      }
      NodeMetricsFile oldestFile = null;
      ZonedDateTime oldestWriteTime = null;
      for (NodeMetricsFile file : directoryStream) {
        ZonedDateTime writeTimestamp = file.getWriteTimestamp();
        if (oldestFile == null || writeTimestamp.isBefore(oldestWriteTime)) {
          oldestFile = file;
          oldestWriteTime = writeTimestamp;
        }
      }
      if (oldestFile == null) {
        return Optional.empty();
      }
      return Optional.of(oldestFile);
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
