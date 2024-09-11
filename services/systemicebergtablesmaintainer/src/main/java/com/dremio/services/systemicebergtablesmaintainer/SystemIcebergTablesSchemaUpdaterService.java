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
package com.dremio.services.systemicebergtablesmaintainer;

import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig;
import com.dremio.service.Service;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.Schedule.SingleShotBuilder;
import com.dremio.service.scheduler.Schedule.SingleShotType;
import com.dremio.service.scheduler.SchedulerService;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for updating the schema of system iceberg tables. The service's task is
 * executed once and only once in the cluster or at every upgrade.
 */
public class SystemIcebergTablesSchemaUpdaterService implements Service, Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SystemIcebergTablesSchemaUpdaterService.class);
  private static final String LOCAL_TASK_LEADER_NAME = "systemicebergtablesschemaupdater";
  private final Provider<SchedulerService> schedulerService;
  private final Provider<CatalogService> catalogServiceProvider;

  public SystemIcebergTablesSchemaUpdaterService(
      final Provider<SchedulerService> schedulerServiceProvider,
      final Provider<CatalogService> catalogServiceProvider) {
    this.schedulerService = schedulerServiceProvider;
    this.catalogServiceProvider = catalogServiceProvider;
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("Starting system iceberg tables schema updater service");
    Schedule schedule =
        SingleShotBuilder.now()
            .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
            .setSingleShotType(SingleShotType.RUN_ONCE_EVERY_UPGRADE)
            .build();
    schedulerService.get().schedule(schedule, this);
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing system iceberg tables schema updater service");
  }

  @Override
  public void run() {
    LOGGER.info("Running system iceberg tables schema updater service task");

    SystemIcebergTablesStoragePlugin plugin =
        catalogServiceProvider
            .get()
            .getSource(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);
    plugin.updateSystemIcebergTables();
  }
}
