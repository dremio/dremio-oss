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

  /**
   * Constructs a new SystemIcebergViewQueryBuilder.
   *
   * @param schemaVersion The schema version to consider.
   * @param viewName The name of the view for which the query will be generated.
   * @param namespaceKey The namespace key for the dataset.
   * @param userName The user name to filter the results.
   */
  public SystemIcebergViewQueryBuilder(
      long schemaVersion, String viewName, NamespaceKey namespaceKey, String userName) {
    this.schemaVersion = schemaVersion;
    this.viewName = viewName;
    this.namespaceKey = namespaceKey;
    this.userName = userName;
  }

  /**
   * Generate an SQL query for the specified view name and schema version.
   *
   * @return The SQL query for the specified view name, schema version, and user name filter.
   * @throws UnsupportedOperationException If the view name or schema version is not supported.
   */
  public String getViewQuery() {
    if (schemaVersion == 1) {
      if (viewName.equalsIgnoreCase(
          SystemIcebergViewMetadataFactory.COPY_ERRORS_HISTORY_VIEW_NAME)) {
        return "SELECT"
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getExecutedAtColName(schemaVersion)
            + "\","
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getJobIdColName(schemaVersion)
            + "\","
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getTableNameColName(schemaVersion)
            + "\","
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getUserNameColName(schemaVersion)
            + "\","
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName(schemaVersion)
            + "\","
            + " jh.\""
            + CopyJobHistoryTableSchemaProvider.getStorageLocationColName(schemaVersion)
            + "\","
            + " fh.\""
            + CopyFileHistoryTableSchemaProvider.getFilePathColName(schemaVersion)
            + "\","
            + " fh.\""
            + CopyFileHistoryTableSchemaProvider.getFileStateColName(schemaVersion)
            + "\","
            + " fh.\""
            + CopyFileHistoryTableSchemaProvider.getRecordsLoadedColName(schemaVersion)
            + "\","
            + " fh.\""
            + CopyFileHistoryTableSchemaProvider.getRecordsRejectedColName(schemaVersion)
            + "\""
            + " FROM sys.\""
            + COPY_JOB_HISTORY_TABLE_NAME
            + "\" AS jh"
            + " INNER JOIN sys.\""
            + COPY_FILE_HISTORY_TABLE_NAME
            + "\" AS fh"
            + " ON jh.\""
            + CopyJobHistoryTableSchemaProvider.getJobIdColName(schemaVersion)
            + "\" = fh.\""
            + CopyFileHistoryTableSchemaProvider.getJobIdColName(schemaVersion)
            + "\""
            + " WHERE jh.\""
            + CopyJobHistoryTableSchemaProvider.getUserNameColName(schemaVersion)
            + "\" = '"
            + userName
            + "'";
      } else if (viewName.equalsIgnoreCase(COPY_JOB_HISTORY_TABLE_NAME)
          || viewName.equalsIgnoreCase(COPY_FILE_HISTORY_TABLE_NAME)) {
        return "SELECT * FROM " + namespaceKey;
      }

      throw new UnsupportedOperationException(
          String.format("Cannot provide view query for view name %s", viewName));
    }

    throw new UnsupportedOperationException(
        String.format("Cannot provide view query for schema version %s", schemaVersion));
  }
}
