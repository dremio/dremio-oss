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
package com.dremio.catalog;

import com.dremio.common.WakeupHandler;
import com.dremio.service.Service;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs catalog periodic maintenance tasks. */
public class CatalogMaintenanceService implements Service {
  private static final Logger logger = LoggerFactory.getLogger(CatalogMaintenanceService.class);

  private final Provider<SchedulerService> schedulerServiceProvider;
  private final Provider<ExecutorService> executorServiceProvider;
  private final ImmutableList<CatalogMaintenanceRunnable> maintenanceRunnables;

  public CatalogMaintenanceService(
      Provider<SchedulerService> schedulerServiceProvider,
      Provider<ExecutorService> executorServiceProvider,
      ImmutableList<CatalogMaintenanceRunnable> maintenanceRunnables) {
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.executorServiceProvider = executorServiceProvider;
    this.maintenanceRunnables = maintenanceRunnables;
  }

  /** Schedules periodic maintenance tasks to run on an executor service. */
  @Override
  public void start() throws Exception {
    SchedulerService schedulerService = schedulerServiceProvider.get();
    ExecutorService executorService = executorServiceProvider.get();
    for (CatalogMaintenanceRunnable runnable : maintenanceRunnables) {
      logger.info(
          "Scheduling catalog maintenance {} with schedule {}",
          runnable.name(),
          runnable.schedule());
      WakeupHandler wakeupHandler = new WakeupHandler(executorService, runnable.runnable());
      schedulerService.schedule(
          runnable.schedule(),
          () -> {
            logger.info(
                "Scheduling run of catalog maintenance {} with schedule {}",
                runnable.name(),
                runnable.schedule());
            wakeupHandler.handle(String.format("Scheduled %s", runnable.name()));
          });
    }
  }

  @Override
  public void close() throws Exception {}
}
