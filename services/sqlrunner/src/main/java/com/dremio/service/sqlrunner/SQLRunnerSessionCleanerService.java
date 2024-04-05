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
package com.dremio.service.sqlrunner;

import com.dremio.service.Service;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * A companion service for {@code SQLRunnerSessionService} to clean expired SQL Runner sessions in
 * the background
 */
public class SQLRunnerSessionCleanerService implements Service {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SQLRunnerSessionCleanerService.class);
  private static final int DEFAULT_SQLRUNNER_SESSION_CLEAN_PERIOD_HOUR =
      Math.toIntExact(TimeUnit.DAYS.toHours(1));
  private static final int DEFAULT_SQLRUNNER_SESSION_CLEAN_RELEASE_LEADERSHIP_HOUR_IN_MS =
      Math.toIntExact(TimeUnit.HOURS.toMillis(36));
  private static final String LOCAL_TASK_LEADER_NAME = "sqlrunner_session_cleaner";
  private final Provider<SchedulerService> scheduler;
  private final Provider<SQLRunnerSessionService> sqlRunnerSessionService;
  private volatile Cancellable cleanerTask;

  @Inject
  public SQLRunnerSessionCleanerService(
      Provider<SchedulerService> schedulerService,
      Provider<SQLRunnerSessionService> sqlRunnerSessionService) {
    this.scheduler = schedulerService;
    this.sqlRunnerSessionService = sqlRunnerSessionService;
  }

  @Override
  public void start() throws Exception {
    cleanerTask =
        scheduler
            .get()
            .schedule(
                Schedule.Builder.everyHours(DEFAULT_SQLRUNNER_SESSION_CLEAN_PERIOD_HOUR)
                    .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                    .releaseOwnershipAfter(
                        DEFAULT_SQLRUNNER_SESSION_CLEAN_RELEASE_LEADERSHIP_HOUR_IN_MS,
                        TimeUnit.MILLISECONDS)
                    .startingAt(
                        Instant.ofEpochMilli(
                            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15)))
                    .build(),
                () -> {
                  logger.info("Clean expired SQL Runner sessions");
                  final int expired = sqlRunnerSessionService.get().deleteExpired();
                  logger.info("Deleted {} expired SQL Runner sessions", expired);
                });
  }

  @Override
  public void close() throws Exception {
    if (cleanerTask != null) {
      cleanerTask.cancel(false);
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static SQLRunnerSessionCleanerService NOOP =
      new SQLRunnerSessionCleanerService(null, null) {
        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {}
      };
}
