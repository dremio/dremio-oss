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
package com.dremio.exec.store.dfs.system;

import static com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED;
import static com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState.IN_PROGRESS;
import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME;

import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.service.namespace.NamespaceKey;

/**
 * A builder class for generating SQL queries for system Iceberg views based on the schema version
 * and view name.
 */
public class SystemIcebergViewQueryBuilder {

  private final long schemaVersion;
  private final String viewName;
  private final NamespaceKey namespaceKey;
  private final String userName;
  private final boolean canReadAllRecords;

  /**
   * Constructs a new SystemIcebergViewQueryBuilder.
   *
   * @param schemaVersion The schema version to consider.
   * @param viewName The name of the view for which the query will be generated.
   * @param namespaceKey The namespace key for the dataset.
   * @param userName The user name to filter the results.
   * @param canReadAllRecords Flag to indicate whether the provided userName can access all the
   *     records from the view or an addition filtering has to be applied.
   */
  public SystemIcebergViewQueryBuilder(
      long schemaVersion,
      String viewName,
      NamespaceKey namespaceKey,
      String userName,
      boolean canReadAllRecords) {
    this.schemaVersion = schemaVersion;
    this.viewName = viewName;
    this.namespaceKey = namespaceKey;
    this.userName = userName;
    this.canReadAllRecords = canReadAllRecords;
  }

  /**
   * Generate an SQL query for the specified view name and schema version.
   *
   * @return The SQL query for the specified view name, schema version, and user name filter.
   * @throws UnsupportedOperationException If the view name or schema version is not supported.
   */
  public String getViewQuery() {
    if (viewName.equalsIgnoreCase(COPY_JOB_HISTORY_TABLE_NAME)
        || viewName.equalsIgnoreCase(COPY_FILE_HISTORY_TABLE_NAME)) {
      return "SELECT * FROM " + namespaceKey;
    } else if (viewName.equalsIgnoreCase(
        SystemIcebergViewMetadataFactory.COPY_ERRORS_HISTORY_VIEW_NAME)) {
      if (schemaVersion <= 2) {
        String query =
            "SELECT"
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getExecutedAtColName()
                + "\","
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getJobIdColName()
                + "\","
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getTableNameColName()
                + "\","
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getUserNameColName()
                + "\","
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName()
                + "\","
                + " jh.\""
                + CopyJobHistoryTableSchemaProvider.getStorageLocationColName()
                + "\","
                + " fh.\""
                + CopyFileHistoryTableSchemaProvider.getFilePathColName()
                + "\","
                + " fh.\""
                + CopyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\","
                + " fh.\""
                + CopyFileHistoryTableSchemaProvider.getRecordsLoadedColName()
                + "\","
                + " fh.\""
                + CopyFileHistoryTableSchemaProvider.getRecordsRejectedColName()
                + "\""
                + " FROM sys.\""
                + COPY_JOB_HISTORY_TABLE_NAME
                + "\" AS jh"
                + " INNER JOIN sys.\""
                + COPY_FILE_HISTORY_TABLE_NAME
                + "\" AS fh"
                + " ON jh.\""
                + CopyJobHistoryTableSchemaProvider.getJobIdColName()
                + "\" = fh.\""
                + CopyFileHistoryTableSchemaProvider.getJobIdColName()
                + "\""
                + " WHERE fh.\""
                + CopyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != \'"
                + FULLY_LOADED.name()
                + "\' AND fh.\""
                + CopyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != \'"
                + IN_PROGRESS.name()
                + "\'";
        if (!canReadAllRecords) {
          query +=
              " AND jh.\""
                  + CopyJobHistoryTableSchemaProvider.getUserNameColName()
                  + "\" = '"
                  + userName
                  + "'";
        }
        return query;
      }
    }
    throw new UnsupportedOperationException(
        String.format("Cannot provide view query for schema version %s", schemaVersion));
  }
}
