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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
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
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for maintaining system Iceberg tables by performing scheduled cleanup tasks
 * such as deleting old records, optimizing tables, and vacuuming snapshots.
 */
public class SystemIcebergTablesMaintainerService implements Service, Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SystemIcebergTablesMaintainerService.class);
  private static final String LOCAL_TASK_LEADER_NAME = "systemicebergtablesmaintainer";
  private static final String DELETE_QUERY_TEMPLATE = "DELETE FROM %s.%s WHERE %s < '%s'";
  private static final String OPTIMIZE_QUERY_TEMPLATE = "OPTIMIZE TABLE %s.%s";
  private static final String VACUUM_QUERY_TEMPLATE =
      "VACUUM TABLE %s.%s EXPIRE SNAPSHOTS OLDER_THAN '%s'";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  private final Provider<SchedulerService> schedulerService;
  private final Provider<OptionManager> optionManager;
  private final Provider<SabotContext> sabotContext;
  private final List<SystemIcebergTableMetadata> tableMetadataList = new ArrayList<>();
  private final CopyJobHistoryTableSchemaProvider copyJobHistoryTableSchemaProvider;
  private final CopyFileHistoryTableSchemaProvider copyFileHistoryTableSchemaProvider;
  private SimpleJobRunner jobRunner;

  public SystemIcebergTablesMaintainerService(
      final Provider<SchedulerService> schedulerService,
      final Provider<OptionManager> optionManagerProvider,
      final Provider<SabotContext> sabotContextProvider) {
    this.schedulerService = schedulerService;
    this.optionManager = optionManagerProvider;
    this.sabotContext = sabotContextProvider;
    long schemaVersion =
        optionManager.get().getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION);
    this.copyJobHistoryTableSchemaProvider = new CopyJobHistoryTableSchemaProvider(schemaVersion);
    this.copyFileHistoryTableSchemaProvider = new CopyFileHistoryTableSchemaProvider(schemaVersion);
  }

  protected void setJobRunner(SimpleJobRunner jobRunner) {
    this.jobRunner = jobRunner;
  }

  @Override
  public void start() throws Exception {
    if (optionManager.get().getOption(ExecConstants.ENABLE_SYSTEM_ICEBERG_TABLES_STORAGE)) {
      LOGGER.info("Starting system iceberg tables maintainer service");
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
      LOGGER.info("Closing system iceberg tables maintainer service");
    }
  }

  @Override
  public void run() {
    try {
      LOGGER.info("Running system iceberg tables maintainer service task");
      SimpleJobRunner simpleJobRunner =
          jobRunner != null ? jobRunner : sabotContext.get().getJobsRunner().get();
      long currentTimeMillis = System.currentTimeMillis();
      performDeletes(simpleJobRunner, getTimestamp(currentTimeMillis, getRecordLifespan()));
      performOptimize(simpleJobRunner);
      performVacuum(simpleJobRunner, getTimestamp(currentTimeMillis, getSnapshotLifespan()));
    } catch (Exception e) {
      LOGGER.error("Error raised while running system iceberg tables maintainer task", e);
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

  /**
   * Performs deletion of old records from system iceberg tables.
   *
   * @param simpleJobRunner The SimpleJobRunner instance.
   * @param deleteTimestamp The timestamp indicating records older than which will be deleted.
   */
  protected void performDeletes(SimpleJobRunner simpleJobRunner, String deleteTimestamp) {
    try {
      SystemIcebergTablesStoragePlugin plugin = getStoragePlugin();
      SystemIcebergTableMetadata jobHistoryMetadata =
          plugin.getTableMetadata(
              ImmutableList.of(SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName()));
      tableMetadataList.add(jobHistoryMetadata);
      SystemIcebergTableMetadata fileHistoryMetadata =
          plugin.getTableMetadata(
              ImmutableList.of(SupportedSystemIcebergTable.COPY_FILE_HISTORY.getTableName()));
      tableMetadataList.add(fileHistoryMetadata);
      runQuery(
          simpleJobRunner,
          String.format(
              DELETE_QUERY_TEMPLATE,
              jobHistoryMetadata.getNamespaceKey().getRoot(),
              jobHistoryMetadata.getTableName(),
              copyJobHistoryTableSchemaProvider.getExecutedAtColName(),
              deleteTimestamp),
          jobHistoryMetadata.getTableLocation());
      LOGGER.info(
          "Ran scheduled delete on system iceberg table {}. Deleted records older than {}",
          jobHistoryMetadata.getTableName(),
          deleteTimestamp);

      runQuery(
          simpleJobRunner,
          String.format(
              DELETE_QUERY_TEMPLATE,
              fileHistoryMetadata.getNamespaceKey().getRoot(),
              fileHistoryMetadata.getTableName(),
              copyFileHistoryTableSchemaProvider.getEventTimestampColName(),
              deleteTimestamp),
          fileHistoryMetadata.getTableLocation());
      LOGGER.info(
          "Ran scheduled delete on system iceberg table {}. Deleted records older than {}",
          fileHistoryMetadata.getTableName(),
          deleteTimestamp);
    } catch (Exception e) {
      LOGGER.error("Error raised while running scheduled delete on system iceberg table", e);
    }
  }

  /**
   * Runs optimization query on system iceberg tables.
   *
   * @param simpleJobRunner The SimpleJobRunner instance.
   */
  protected void performOptimize(SimpleJobRunner simpleJobRunner) {
    try {
      for (SystemIcebergTableMetadata tableMetadata : tableMetadataList) {
        String optimizeQuery =
            String.format(
                OPTIMIZE_QUERY_TEMPLATE,
                tableMetadata.getNamespaceKey().getRoot(),
                tableMetadata.getTableName());
        runQuery(simpleJobRunner, optimizeQuery, tableMetadata.getTableLocation());
        LOGGER.info(
            "Ran scheduled optimize on system iceberg table {}", tableMetadata.getTableName());
      }
    } catch (Exception e) {
      LOGGER.error("Error raised while running scheduled optimize on system iceberg table", e);
    }
  }

  /**
   * Runs vacuum query on system iceberg tables.
   *
   * @param simpleJobRunner The SimpleJobRunner instance.
   * @param vacuumTimestamp The timestamp indicating snapshots older than which will be expired.
   */
  protected void performVacuum(SimpleJobRunner simpleJobRunner, String vacuumTimestamp) {
    try {
      for (SystemIcebergTableMetadata tableMetadata : tableMetadataList) {
        String vacuumQuery =
            String.format(
                VACUUM_QUERY_TEMPLATE,
                tableMetadata.getNamespaceKey().getRoot(),
                tableMetadata.getTableName(),
                vacuumTimestamp);
        runQuery(simpleJobRunner, vacuumQuery, tableMetadata.getTableLocation());
        LOGGER.info(
            "Ran scheduled vacuum on system iceberg table {}. Expired snapshots older than {}",
            tableMetadata.getTableName(),
            vacuumTimestamp);
      }
    } catch (Exception e) {
      LOGGER.error("Error raised while running scheduled vacuum on system iceberg table", e);
    }
  }

  /**
   * Generates a timestamp string based on the current time and the specified lifespan.
   *
   * @param currentTimeMillis The current time in milliseconds.
   * @param SnapshotLifespan The lifespan of the snapshot in milliseconds.
   * @return The timestamp string.
   */
  private String getTimestamp(long currentTimeMillis, long SnapshotLifespan) {
    return DATE_TIME_FORMATTER.format(
        ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(currentTimeMillis - SnapshotLifespan), ZoneId.of("UTC")));
  }

  /**
   * Runs the specified query as a job.
   *
   * @param simpleJobRunner The SimpleJobRunner instance.
   * @param query The query to run.
   * @param tableLocation The location of the table.
   * @throws Exception If an error occurs during query execution.
   */
  private void runQuery(SimpleJobRunner simpleJobRunner, String query, String tableLocation)
      throws Exception {
    if (getStoragePlugin().isTableExists(tableLocation)) {
      simpleJobRunner.runQueryAsJob(
          query,
          SystemUser.SYSTEM_USERNAME,
          QueryType.SYSTEM_ICEBERG_TABLES_MAINTAINER.name(),
          QueryLabel.OPTIMIZATION.name());
    }
  }

  private SystemIcebergTablesStoragePlugin getStoragePlugin() {
    return sabotContext
        .get()
        .getCatalogService()
        .getSource(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);
  }

  /**
   * Gets the schedule time for the next task execution.
   *
   * @return The schedule time.
   */
  private Instant getScheduleTime() {
    return Instant.ofEpochMilli(System.currentTimeMillis() + getCleanupPeriodInMillis());
  }

  /**
   * Gets the cleanup period in milliseconds.
   *
   * @return The cleanup period in milliseconds.
   */
  private long getCleanupPeriodInMillis() {
    return optionManager
        .get()
        .getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_HOUSEKEEPING_THREAD_FREQUENCY_IN_MILLIS);
  }

  /**
   * Gets the record lifespan in milliseconds.
   *
   * @return The record lifespan in milliseconds.
   */
  private long getRecordLifespan() {
    return optionManager
        .get()
        .getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_RECORD_LIFESPAN_IN_MILLIS);
  }

  /**
   * Gets the snapshot lifespan in milliseconds.
   *
   * @return The snapshot lifespan in milliseconds.
   */
  private long getSnapshotLifespan() {
    return optionManager
        .get()
        .getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_SNAPSHOT_LIFESPAN_IN_MILLIS);
  }
}
