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
package com.dremio.exec.server;

import java.nio.file.DirectoryStream;
import java.nio.file.attribute.FileTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;

public class ResultsCleanupService implements Service {
  private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final int DISABLE_CLEANUP_VALUE = -1;
  private static final Logger logger = LoggerFactory.getLogger(ResultsCleanupService.class);
  private final Provider<SchedulerService> schedulerService;
  private final Provider<JobResultsStoreConfig> jobResultsStoreConfigProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private Cancellable jobResultsCleanupTask;

  public ResultsCleanupService(Provider<SchedulerService> schedulerService,
                               Provider<JobResultsStoreConfig> jobResultsStoreConfigProvider,
                               Provider<OptionManager> optionManagerProvider) {
    this.schedulerService = schedulerService;
    this.jobResultsStoreConfigProvider = jobResultsStoreConfigProvider;
    this.optionManagerProvider = optionManagerProvider;
  }

  @Override
  public void start() throws Exception {
    try {
      jobResultsStoreConfigProvider.get();
    } catch (Exception e) {
      logger.info("JobResultsStoreConfig is not available exiting...");
      return;
    }
    logger.info("Starting ResultsCleanupService..");

    final OptionManager optionManager = optionManagerProvider.get();
    final long maxJobResultsAgeInDays = optionManager.getOption(ExecConstants.RESULTS_MAX_AGE_IN_DAYS);
    if (maxJobResultsAgeInDays != DISABLE_CLEANUP_VALUE) {
      final long jobResultsCleanupStartHour = optionManager.getOption(ExecConstants.JOB_RESULTS_CLEANUP_START_HOUR);
      final LocalTime startTime = LocalTime.of((int) jobResultsCleanupStartHour, 0);
      final Schedule resultSchedule = Schedule.Builder.everyDays(1, startTime)
        .withTimeZone(ZoneId.systemDefault())
        .build();
      jobResultsCleanupTask = schedulerService.get().schedule(resultSchedule, new ResultsCleanup());
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Closing ResultsCleanupService...");
    if (jobResultsCleanupTask != null) {
      jobResultsCleanupTask.cancel(false);
      jobResultsCleanupTask = null;
    }
  }

  public class ResultsCleanup implements Runnable {
    @Override
    public void run() {
      cleanup();
    }

    public void cleanup() {
      FileSystem dfs = jobResultsStoreConfigProvider.get().getFileSystem();
      Path resultsFolder = jobResultsStoreConfigProvider.get().getStoragePath();
      final OptionManager optionManager = optionManagerProvider.get();
      long maxAgeInMillis = optionManager.getOption(ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS);
      long maxAgeInDays = optionManager.getOption(ExecConstants.RESULTS_MAX_AGE_IN_DAYS);
      long jobResultsMaxAgeInMillis = (maxAgeInDays * ONE_DAY_IN_MILLIS) + maxAgeInMillis;

      try {
        long cutOffTime = System.currentTimeMillis() - jobResultsMaxAgeInMillis;
        if (!(dfs.exists(resultsFolder))) {
          //directory does not exist so nothing to clean up
          return;
        }
        DirectoryStream<FileAttributes> listOfAttributes = jobResultsStoreConfigProvider.get().getFileSystem().list(resultsFolder);
        //iterate through the directory and cleanup files created before the cutoff time
        for (FileAttributes attr : listOfAttributes) {
          FileTime creationTime = attr.creationTime();
          if (creationTime.toMillis() < cutOffTime) {
            //cleanup
            if (!jobResultsStoreConfigProvider.get().getFileSystem().delete(attr.getPath(), true)) {
              logger.info("Failed to delete directory, {}", attr.getPath());
            }
          }
        }

      } catch (Exception e) {
        logger.error("An exception occured while running ResultsCleanupService", e);
      }
    }
  }
}
