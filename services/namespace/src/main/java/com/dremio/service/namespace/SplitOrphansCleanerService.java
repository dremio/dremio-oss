/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.namespace;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Provider;

import com.dremio.service.Service;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;

/**
 * A companion service for {@code NamespaceService} to clean orphan splits in the background
 */
public class SplitOrphansCleanerService implements Service {
  /**
   * System property to set period of time between two split orphans clean.
   */
  public static final String SPLIT_ORPHANS_CLEAN_PERIOD_HOUR_PROPERTY = "dremio.datasets.split-orphans-clean-period-in-hour";

  private static final String  SPLIT_ORPHANS_RELEASE_LEADERSHIP_MS_PROPERTY = "orphanscleaner.release.leadership.ms";

  private static final String LOCAL_TASK_LEADER_NAME = "orphanscleaner";

  private static final int DEFAULT_SPLIT_ORPHANS_CLEAN_PERIOD_HOUR = Math.toIntExact(TimeUnit.DAYS.toHours(1));

  private static final int DEFAULT_SPLIT_ORPHANS_RELEASE_LEADERSHIP_HOUR_IN_MS =
    Math.toIntExact(TimeUnit.HOURS.toMillis(36));

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitOrphansCleanerService.class);

  private final Provider<SchedulerService> scheduler;
  private final Provider<NamespaceService.Factory> namespaceServiceFactory;

  private volatile Cancellable cleanerTask;

  @Inject
  public SplitOrphansCleanerService(Provider<SchedulerService> schedulerService,
      Provider<NamespaceService.Factory> namespaceServiceFactory) {
    this.scheduler = schedulerService;
    this.namespaceServiceFactory = namespaceServiceFactory;
  }

  @Override
  public void start() throws Exception {
    final int splitOrphansCleanPeriodHour = Integer.getInteger(SPLIT_ORPHANS_CLEAN_PERIOD_HOUR_PROPERTY,
        DEFAULT_SPLIT_ORPHANS_CLEAN_PERIOD_HOUR);

    final int splitOrphansReleaseLeadershipHour = Integer.getInteger(SPLIT_ORPHANS_RELEASE_LEADERSHIP_MS_PROPERTY,
      DEFAULT_SPLIT_ORPHANS_RELEASE_LEADERSHIP_HOUR_IN_MS);

    final NamespaceService namespaceService = namespaceServiceFactory.get().get(SystemUser.SYSTEM_USERNAME);

    cleanerTask = scheduler.get().schedule(Schedule.Builder.everyHours(splitOrphansCleanPeriodHour)
      .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
      .releaseOwnershipAfter(splitOrphansReleaseLeadershipHour, TimeUnit.MILLISECONDS)
      .build(), () -> {
          logger.info("Search for expired dataset splits");
          final int expired = namespaceService.deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_VALID_SPLITS);
          logger.info("Deleted {} expired/orphan dataset splits", expired);
        });
  }

  @Override
  public void close() throws Exception {
    if (cleanerTask != null) {
      cleanerTask.cancel(false);
    }
  }
}
