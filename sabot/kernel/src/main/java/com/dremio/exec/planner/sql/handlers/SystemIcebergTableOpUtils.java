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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.util.Retryer;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for System Iceberg tables in Dremio. */
public final class SystemIcebergTableOpUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemIcebergTableOpUtils.class);
  private static final Retryer RETRYER =
      Retryer.newBuilder().retryIfExceptionOfType(RuntimeException.class).setMaxRetries(10).build();

  private SystemIcebergTableOpUtils() {}

  /**
   * Checks the consistency of the target table with the copy history tables. If tables are not
   * consistent, tries to revert changes within copy history tables to achieve consistency. Reverts
   * are attempted by trying to remove files added by snapshots with specified jobId. If this fails,
   * a DELETE query is run against the history tables to remove entries with given jobId in them.
   *
   * @param context Query context.
   * @param targetTablePath The path of the target table with version context.
   * @param jobId JobId to check consistency.
   * @return Returns only if tables are consistent. Returns true if the target table has been
   *     committed to, false otherwise.
   * @throws Exception If consistency cannot be validated or achieved.
   */
  public static boolean corroborateConsistencyWithCopyHistoryTables(
      QueryContext context, CatalogEntityKey targetTablePath, String jobId) throws Exception {
    Catalog catalog = context.getCatalog();
    SystemIcebergTablesStoragePlugin systemPlugin =
        catalog.getSource(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);

    List<String> jobHistoryTableSchemaPath = ImmutableList.of(COPY_JOB_HISTORY_TABLE_NAME);
    List<String> fileHistoryTableSchemaPath = ImmutableList.of(COPY_FILE_HISTORY_TABLE_NAME);

    Table jobHistoryTable =
        Preconditions.checkNotNull(
            systemPlugin.getTable(jobHistoryTableSchemaPath), "COPY JOB HISTORY table not found.");
    Table fileHistoryTable =
        Preconditions.checkNotNull(
            systemPlugin.getTable(fileHistoryTableSchemaPath),
            "COPY FILE HISTORY table not found.");

    Table targetTable =
        IcebergUtils.getIcebergTable(
            IcebergUtils.getIcebergCreateTableEntry(
                context,
                catalog,
                catalog.getTable(targetTablePath),
                SqlCopyIntoTable.OPERATOR,
                null,
                CatalogUtil.resolveVersionContext(
                    catalog,
                    targetTablePath.getRootEntity(),
                    targetTablePath.getTableVersionContext().asVersionContext())));

    // Short circuit if target table has commits
    if (targetTableHasCommits(targetTable, jobId)) {
      return true;
    }

    boolean deleteFromJobHistoryTable = false;
    boolean deleteFromFileHistoryTable = false;
    try {
      SystemIcebergTableOpUtils.corroborateConsistencyWithTable(
          targetTable, jobHistoryTable, jobId);
      systemPlugin.refreshDataset(jobHistoryTableSchemaPath);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to corroborate consistency with copy job history table.", e);
      deleteFromJobHistoryTable = true;
    }
    try {
      SystemIcebergTableOpUtils.corroborateConsistencyWithTable(
          targetTable, fileHistoryTable, jobId);
      systemPlugin.refreshDataset(fileHistoryTableSchemaPath);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to corroborate consistency with copy file history table.", e);
      deleteFromFileHistoryTable = true;
    }

    if (deleteFromJobHistoryTable) {
      systemPlugin.refreshDataset(jobHistoryTableSchemaPath);

      final SystemIcebergTableMetadata jobHistoryTableMetadata =
          systemPlugin.getTableMetadata(jobHistoryTableSchemaPath);

      runDeleteQuery(
          context,
          jobHistoryTableMetadata,
          CopyJobHistoryTableSchemaProvider.getJobIdColName(),
          jobId);
    }
    if (deleteFromFileHistoryTable) {
      systemPlugin.refreshDataset(fileHistoryTableSchemaPath);

      final SystemIcebergTableMetadata fileHistoryTableMetadata =
          systemPlugin.getTableMetadata(fileHistoryTableSchemaPath);

      runDeleteQuery(
          context,
          fileHistoryTableMetadata,
          CopyFileHistoryTableSchemaProvider.getJobIdColName(),
          jobId);
    }
    return false;
  }

  /**
   * Retrieves append snapshots from table if they are tagged with the specified jobId. If
   * delete/overwrite snapshots are found with the specified job ID, the appends are assumed to have
   * been reverted and an empty optional is returned.
   *
   * @param table The table to retrieve the snapshot from.
   * @param jobId The job ID to match against the snapshot's summary job ID.
   * @return An Optional containing the append snapshot if found, or an empty Optional if not found
   *     or reverted by a delete/overwrite snapshot with the same jobId.
   */
  public static Optional<Snapshot> getStampedAppendSnapshot(Table table, String jobId) {
    Snapshot appendSnapshot = null;
    for (Snapshot snapshot : table.snapshots()) {
      final String summaryJobId =
          snapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY);
      if (summaryJobId != null && summaryJobId.equals(jobId)) {
        final String operation = snapshot.operation();
        if (operation.equals(DataOperations.DELETE) || operation.equals(DataOperations.OVERWRITE)) {
          LOGGER.info(
              "DELETE or OVERWRITE snapshot found for JobId {}. Data added by Job has already been reverted.",
              jobId);
          // If we find a DELETE snapshot with the same queryId,
          // we can assume the append was reverted
          return Optional.empty();
        } else if (operation.equals(DataOperations.APPEND)) {
          appendSnapshot = snapshot;
        }
      }
    }
    return Optional.ofNullable(appendSnapshot);
  }

  /**
   * Checks the consistency of the target table with the provided tables. If tables are not
   * consistent, tries to revert changes within them to achieve consistency.
   *
   * @param targetTable The target table.
   * @param table The table to check consistency against target table.
   * @param jobId JobId to check consistency.
   * @return Returns only if tables are consistent. Returns true if the target table has been
   *     committed to, false otherwise.
   * @throws Exception If consistency cannot be validated or achieved.
   */
  public static boolean corroborateConsistencyWithTable(
      Table targetTable, Table table, String jobId) {
    if (targetTableHasCommits(targetTable, jobId)) {
      return true;
    }

    getStampedAppendSnapshot(table, jobId)
        .ifPresent(
            snapshot -> {
              LOGGER.warn(
                  "Reverting snapshot [{}] with jobId {} for table {}.",
                  snapshot.snapshotId(),
                  jobId,
                  table.name());
              if (safelyRetry(() -> IcebergUtils.revertSnapshotFiles(table, snapshot, jobId))
                  .isEmpty()) {
                throw new RuntimeException(
                    String.format("Failed to revert snapshot for table %s", table.name()));
              }
            });

    return false;
  }

  private static boolean targetTableHasCommits(Table targetTable, String jobId) {
    return getStampedAppendSnapshot(targetTable, jobId).isPresent();
  }

  private static <T> Optional<T> safelyRetry(Callable<T> action) {
    try {
      return Optional.of(RETRYER.call(action));
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      LOGGER.warn("Failed to revert snapshots.", e);
      return Optional.empty();
    }
  }

  private static void runDeleteQuery(
      QueryContext context,
      SystemIcebergTableMetadata tableMetadata,
      String colName,
      String queryId)
      throws Exception {
    LOGGER.info("Running DELETE query for table {}.", tableMetadata.getTableName());
    context
        .getSabotQueryContext()
        .getJobsRunner()
        .get()
        .runQueryAsJob(
            String.format(
                "DELETE FROM %s.%s WHERE %s = '%s'",
                tableMetadata.getNamespaceKey().getRoot(),
                tableMetadata.getTableName(),
                colName,
                queryId),
            SystemUser.SYSTEM_USERNAME,
            "SYSTEM_ICEBERG_TABLES_MAINTAINER",
            null);
  }
}
