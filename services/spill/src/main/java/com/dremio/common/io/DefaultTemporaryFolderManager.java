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
package com.dremio.common.io;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.google.common.annotations.VisibleForTesting;

/**
 * Monitors temporary data created by dremio engine(s) and cleans it up when it is no longer
 * necessary.
 */
public class DefaultTemporaryFolderManager implements TemporaryFolderManager {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTemporaryFolderManager.class);
  private static final String CLEANUP_THREAD_PREFIX = "folder-cleanup-";

  @VisibleForTesting
  interface CleanupConfig {
    int getStalenessOnRestartSeconds();

    int getStalenessLimitSeconds();

    int getCleanupDelaySeconds();

    int getCleanupDelayMaxVariationSeconds();

    int getOneShotCleanupDelaySeconds();

    int getMinUnhealthyCyclesBeforeDelete();
  }

  // Currently these config values are not externally configurable except for unit testing purposes.
  private static final int DEFAULT_STALENESS_LIMIT_SECONDS = 30 * 60;
  private static final int DEFAULT_STALENESS_ON_RESTART_LIMIT_SECONDS = 90;
  private static final int DEFAULT_CLEANUP_DELAY_SECONDS = 3 * 60;
  private static final int DEFAULT_CLEANUP_DELAY_MAX_GAP = 60;
  private static final int DEFAULT_ONE_SHOT_DELAY_SECONDS = DEFAULT_STALENESS_ON_RESTART_LIMIT_SECONDS + 1;
  private static final int DEFAULT_MIN_UNHEALTHY_CYCLES = 5;

  private static final CleanupConfig CLEANUP_CONFIG = new DefaultCleanupConfig();

  private final CloseableSchedulerThreadPool executorService;
  private final CleanupConfig cleanupConfig;
  private final TemporaryFolderMonitor folderMonitor;
  private final FileSystemHelper fsWrapper;
  private final Supplier<ExecutorId> thisExecutor;
  private final String purpose;

  private volatile ScheduledFuture<?> monitorTask;
  private volatile ScheduledFuture<?> oneShotTask;
  private volatile boolean closed;

  public DefaultTemporaryFolderManager(Supplier<ExecutorId> thisExecutor, Configuration conf,
                                       Supplier<Set<ExecutorId>> availableExecutors,
                                       String purpose) {
    this(thisExecutor, conf, availableExecutors, purpose, CLEANUP_CONFIG);
  }

  @VisibleForTesting
  DefaultTemporaryFolderManager(Supplier<ExecutorId> thisExecutor, Configuration conf,
                                Supplier<Set<ExecutorId>> availableExecutors,
                                String purpose, CleanupConfig cleanupConfig) {
    this.executorService = new CloseableSchedulerThreadPool(CLEANUP_THREAD_PREFIX, 1);
    this.executorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
    this.executorService.setRemoveOnCancelPolicy(true);
    this.fsWrapper = new FileSystemHelper(conf);
    this.cleanupConfig = cleanupConfig;
    this.purpose = purpose;
    this.thisExecutor = thisExecutor;
    if (availableExecutors != null) {
      // if available executors can be monitored, start a background task to clean up on dead executors.
      this.folderMonitor = new TemporaryFolderMonitor(thisExecutor, availableExecutors, purpose,
        cleanupConfig.getStalenessLimitSeconds(), cleanupConfig.getMinUnhealthyCyclesBeforeDelete(), fsWrapper);
    } else {
      this.folderMonitor = null;
    }
    this.oneShotTask = null;
    this.monitorTask = null;
  }

  @Override
  public void startMonitoring() {
    if (folderMonitor != null) {
      final int increment = (cleanupConfig.getCleanupDelayMaxVariationSeconds() > 0) ?
        (int) (Instant.now().toEpochMilli() * Thread.currentThread().getId())
          % cleanupConfig.getCleanupDelayMaxVariationSeconds() : 0;
      final int delay = cleanupConfig.getCleanupDelaySeconds() + increment;
      logger.debug("Starting folder monitoring for cleanup with {} seconds as interval", delay);
      this.monitorTask = executorService.scheduleAtFixedRate(folderMonitor::doCleanupOther, delay, delay,
        TimeUnit.SECONDS);
    }
  }

  @Override
  public Path createTmpDirectory(Path rootPath) throws IOException {
    if (closed) {
      throw new IOException("Temporary Folder Manager already closed");
    }
    if (thisExecutor == null || thisExecutor.get() == null) {
      // retain old behaviour
      return rootPath;
    }
    final String prefix = thisExecutor.get().toPrefix(purpose);
    logger.info("Registering path '{}' for temporary file monitoring and cleanup", rootPath.toString());
    List<Path> pathsToDelete = new ArrayList<>();
    fsWrapper.visitDirectory(rootPath, prefix, fileStatus -> {
      if (fileStatus.isDirectory()) {
        try {
          final long incarnation = Long.parseLong(fileStatus.getPath().getName());
          if (incarnation > Instant.EPOCH.toEpochMilli() && incarnation < Instant.now().toEpochMilli()) {
            pathsToDelete.add(fileStatus.getPath());
          } else {
            logger.warn("Not deleting old incarnation {} as it is too recent or invalid", incarnation);
          }
        } catch (NumberFormatException ignored) {
          logger.debug("Ignoring directory {} as it is not a valid incarnation", fileStatus.getPath().getName());
        }
      }
    });
    if (!pathsToDelete.isEmpty()) {
      this.oneShotTask = executorService.schedule(() -> doCleanupOneShot(pathsToDelete),
        cleanupConfig.getOneShotCleanupDelaySeconds(), TimeUnit.SECONDS);
    }
    final Path newIncarnation = fsWrapper.createTmpDirectory(rootPath, prefix);
    if (folderMonitor != null) {
      folderMonitor.startMonitoring(rootPath);
    }
    return newIncarnation;
  }

  @Override
  public void close() throws Exception {
    try {
      if (oneShotTask != null && !oneShotTask.isDone()) {
        try {
          oneShotTask.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          oneShotTask.cancel(false);
        }
      }
      if (monitorTask != null) {
        monitorTask.cancel(false);
      }
      executorService.shutdown();
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          logger.warn("Unable to stop temporary folder monitor task");
        }
      }
      this.closed = true;
    } catch (Exception e) {
      // just log.. mask the exception from layers above as this mostly in the shutdown path
      logger.warn("Unexpected exception while stopping temporary folder service {}",
        e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
    }
  }

  private void doCleanupOneShot(List<Path> pathsToDelete) {
    for (final Path incarnationDir : pathsToDelete) {
      fsWrapper.safeCleanOldIncarnation(incarnationDir, cleanupConfig.getStalenessOnRestartSeconds(), true);
    }
  }

  private static final class DefaultCleanupConfig implements CleanupConfig {
    @Override
    public int getStalenessOnRestartSeconds() {
      return DEFAULT_STALENESS_ON_RESTART_LIMIT_SECONDS;
    }

    @Override
    public int getStalenessLimitSeconds() {
      return DEFAULT_STALENESS_LIMIT_SECONDS;
    }

    @Override
    public int getCleanupDelaySeconds() {
      return DEFAULT_CLEANUP_DELAY_SECONDS;
    }

    @Override
    public int getCleanupDelayMaxVariationSeconds() {
      return DEFAULT_CLEANUP_DELAY_MAX_GAP;
    }

    @Override
    public int getOneShotCleanupDelaySeconds() {
      return DEFAULT_ONE_SHOT_DELAY_SECONDS;
    }

    @Override
    public int getMinUnhealthyCyclesBeforeDelete() {
      return DEFAULT_MIN_UNHEALTHY_CYCLES;
    }
  }
}
