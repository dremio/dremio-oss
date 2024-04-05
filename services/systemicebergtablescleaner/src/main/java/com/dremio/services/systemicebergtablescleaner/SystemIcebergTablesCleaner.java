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
package com.dremio.services.systemicebergtablescleaner;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.dremio.service.job.QueryLabel;
import com.dremio.service.job.QueryType;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemIcebergTablesCleaner implements Service, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SystemIcebergTablesCleaner.class);
  private static final String LOCAL_TASK_LEADER_NAME = "copyintoerrorstablecleanup";
  private static final String DELETE_QUERY_TEMPLATE = "DELETE FROM %s.%s WHERE %s < '%s'";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  private final Provider<SchedulerService> schedulerService;
  private final Provider<OptionManager> optionManager;
  private final Provider<SabotContext> sabotContext;
  private final long schemaVersion;

  public SystemIcebergTablesCleaner(
      final Provider<SchedulerService> schedulerService,
      final Provider<OptionManager> optionManagerProvider,
      final Provider<SabotContext> sabotContextProvider) {
    this.schedulerService = schedulerService;
    this.optionManager = optionManagerProvider;
    this.sabotContext = sabotContextProvider;
    this.schemaVersion =
        optionManagerProvider.get().getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION);
  }

  @Override
  public void start() throws Exception {
    if (optionManager.get().getOption(ExecConstants.ENABLE_SYSTEM_ICEBERG_TABLES_STORAGE)) {
      LOGGER.info("Starting system iceberg tables cleaner Service");
      schedulerService
          .get()
          .schedule(
              Schedule.Builder.singleShotChain()
                  .startingAt(getScheduleTime())
                  .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                  .build(),
              this);
    }
  }

  @Override
  public void close() throws Exception {
    if (optionManager.get().getOption(ExecConstants.ENABLE_SYSTEM_ICEBERG_TABLES_STORAGE)) {
      LOGGER.info("Closing system iceberg tables cleaner Service");
    }
  }

  @Override
  public void run() {
    try {
      SystemIcebergTablesStoragePlugin plugin =
          sabotContext
              .get()
              .getCatalogService()
              .getSource(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);
      SystemIcebergTableMetadata jobHistoryMetadata =
          plugin.getTableMetadata(
              ImmutableList.of(SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME));
      SystemIcebergTableMetadata fileHistoryMetadata =
          plugin.getTableMetadata(
              ImmutableList.of(SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME));
      String timestamp =
          DATE_TIME_FORMATTER.format(
              ZonedDateTime.ofInstant(
                  Instant.ofEpochMilli(System.currentTimeMillis() - getRecordLifespan()),
                  ZoneId.of("UTC")));
      performClean(
          plugin,
          jobHistoryMetadata.getTableLocation(),
          String.format(
              DELETE_QUERY_TEMPLATE,
              jobHistoryMetadata.getNamespaceKey().getRoot(),
              jobHistoryMetadata.getTableName(),
              CopyJobHistoryTableSchemaProvider.getExecutedAtColName(schemaVersion),
              timestamp));

      performClean(
          plugin,
          fileHistoryMetadata.getTableLocation(),
          String.format(
              DELETE_QUERY_TEMPLATE,
              fileHistoryMetadata.getNamespaceKey().getRoot(),
              fileHistoryMetadata.getTableName(),
              CopyFileHistoryTableSchemaProvider.getEventTimestampColName(schemaVersion),
              timestamp));
    } catch (Exception e) {
      LOGGER.warn("Unable to cleanup system iceberg tables", e);
    } finally {
      schedulerService
          .get()
          .schedule(
              Schedule.Builder.singleShotChain()
                  .startingAt(getScheduleTime())
                  .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                  .build(),
              this);
    }
  }

  private void performClean(
      SystemIcebergTablesStoragePlugin plugin, String tableLocation, String deleteQuery)
      throws Exception {
    if (plugin.isTableExists(tableLocation)) {
      SimpleJobRunner simpleJobRunner = sabotContext.get().getJobsRunner().get();
      simpleJobRunner.runQueryAsJob(
          deleteQuery,
          SystemUser.SYSTEM_USERNAME,
          QueryType.SYSTEM_ICEBERG_TABLES_CLEANER.name(),
          QueryLabel.DML.name());
    }
  }

  private Instant getScheduleTime() {
    return Instant.ofEpochMilli(System.currentTimeMillis() + getCleanupPeriodInMillis());
  }

  private long getCleanupPeriodInMillis() {
    return optionManager
        .get()
        .getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_HOUSEKEEPING_THREAD_FREQUENCY_IN_MILLIS);
  }

  private long getRecordLifespan() {
    return optionManager
        .get()
        .getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_RECORD_LIFESPAN_IN_MILLIS);
  }
}
