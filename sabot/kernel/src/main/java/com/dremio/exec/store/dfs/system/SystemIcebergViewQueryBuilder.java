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

import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.ColumnDefinition;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory.SupportedSystemIcebergView;
import com.dremio.service.namespace.NamespaceKey;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A builder class for generating SQL queries for system Iceberg views based on the schema version
 * and view name.
 */
public class SystemIcebergViewQueryBuilder {

  private static final String SELECT_QUERY_TEMPLATE = "select %s from %s";

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
    if (viewName.equalsIgnoreCase(SupportedSystemIcebergTable.COPY_FILE_HISTORY.getTableName())) {
      CopyFileHistoryTableSchemaProvider schemaProvider =
          new CopyFileHistoryTableSchemaProvider(schemaVersion);
      return String.format(
          SELECT_QUERY_TEMPLATE, getSelectClause(schemaProvider.getColumns()), namespaceKey);
    }

    if (viewName.equalsIgnoreCase(SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName())) {
      CopyJobHistoryTableSchemaProvider schemaProvider =
          new CopyJobHistoryTableSchemaProvider(schemaVersion);
      return String.format(
          SELECT_QUERY_TEMPLATE, getSelectClause(schemaProvider.getColumns()), namespaceKey);
    }

    if (viewName.equalsIgnoreCase(SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName())) {
      if (schemaVersion <= 3) {
        CopyJobHistoryTableSchemaProvider copyJobHistoryTableSchemaProvider =
            new CopyJobHistoryTableSchemaProvider(schemaVersion);
        CopyFileHistoryTableSchemaProvider copyFileHistoryTableSchemaProvider =
            new CopyFileHistoryTableSchemaProvider(schemaVersion);
        String query =
            "SELECT"
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getExecutedAtColName()
                + "\","
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getJobIdColName()
                + "\","
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getTableNameColName()
                + "\","
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getUserNameColName()
                + "\","
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName()
                + "\","
                + " jh.\""
                + copyJobHistoryTableSchemaProvider.getStorageLocationColName()
                + "\","
                + " fh.\""
                + copyFileHistoryTableSchemaProvider.getFilePathColName()
                + "\","
                + " fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\","
                + " fh.\""
                + copyFileHistoryTableSchemaProvider.getRecordsLoadedColName()
                + "\","
                + " fh.\""
                + copyFileHistoryTableSchemaProvider.getRecordsRejectedColName()
                + "\""
                + " FROM sys.\""
                + SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName()
                + "\" AS jh"
                + " INNER JOIN sys.\""
                + SupportedSystemIcebergTable.COPY_FILE_HISTORY.getTableName()
                + "\" AS fh"
                + " ON jh.\""
                + copyJobHistoryTableSchemaProvider.getJobIdColName()
                + "\" = fh.\""
                + copyFileHistoryTableSchemaProvider.getJobIdColName()
                + "\""
                + " WHERE fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + FULLY_LOADED.name()
                + "' AND fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + IN_PROGRESS.name()
                + "'";
        if (!canReadAllRecords) {
          query +=
              " AND jh.\""
                  + copyJobHistoryTableSchemaProvider.getUserNameColName()
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

  private String getSelectClause(List<ColumnDefinition> columns) {
    return columns.stream()
        .filter(c -> !c.isHidden())
        .map(ColumnDefinition::getName)
        .map(name -> "\"" + name + "\"")
        .collect(Collectors.joining(", "));
  }
}
