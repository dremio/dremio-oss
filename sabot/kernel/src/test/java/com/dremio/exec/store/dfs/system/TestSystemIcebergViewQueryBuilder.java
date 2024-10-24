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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory.SupportedSystemIcebergView;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestSystemIcebergViewQueryBuilder {

  @Test
  public void testGetViewQueryForCopyErrorsHistoryView() {
    String userName = "sampleUserName";
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    CopyJobHistoryTableSchemaProvider copyJobHistoryTableSchemaProvider =
        new CopyJobHistoryTableSchemaProvider(schemaVersion);
    CopyFileHistoryTableSchemaProvider copyFileHistoryTableSchemaProvider =
        new CopyFileHistoryTableSchemaProvider(schemaVersion);
    SystemIcebergViewQueryBuilder queryBuilder =
        new SystemIcebergViewQueryBuilder(
            schemaVersion,
            SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName(),
            new NamespaceKey(
                ImmutableList.of(SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName())),
            userName,
            false);

    String query = queryBuilder.getViewQuery();

    assertThat(query).contains("SELECT");
    assertThat(query)
        .contains(
            "FROM sys.\""
                + SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName()
                + "\" AS jh");
    assertThat(query)
        .contains(
            "INNER JOIN sys.\""
                + SupportedSystemIcebergTable.COPY_FILE_HISTORY.getTableName()
                + "\" AS fh");
    assertThat(query)
        .contains(
            "WHERE fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + FULLY_LOADED.name()
                + "' AND fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + IN_PROGRESS.name()
                + "'");
    assertThat(query)
        .contains(
            "AND jh.\""
                + copyJobHistoryTableSchemaProvider.getUserNameColName()
                + "\" = '"
                + userName
                + "'");

    queryBuilder =
        new SystemIcebergViewQueryBuilder(
            schemaVersion,
            SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName(),
            new NamespaceKey(
                ImmutableList.of(SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName())),
            userName,
            true);
    query = queryBuilder.getViewQuery();
    assertThat(query)
        .contains(
            "WHERE fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + FULLY_LOADED.name()
                + "' AND fh.\""
                + copyFileHistoryTableSchemaProvider.getFileStateColName()
                + "\" != '"
                + IN_PROGRESS.name()
                + "'");
    assertThat(
            query.contains(
                "AND jh.\""
                    + copyJobHistoryTableSchemaProvider.getUserNameColName()
                    + "\" = '"
                    + userName
                    + "'"))
        .isFalse();
  }

  @Test
  public void testGetViewQueryForCopyJobHistoryTable() {
    NamespaceKey namespaceKey = new NamespaceKey(ImmutableList.of("path", "to", "namespace"));
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    SystemIcebergViewQueryBuilder queryBuilder =
        new SystemIcebergViewQueryBuilder(
            schemaVersion,
            SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName(),
            namespaceKey,
            "sampleUserName",
            true);

    String query = queryBuilder.getViewQuery();
    CopyJobHistoryTableSchemaProvider schemaProvider =
        new CopyJobHistoryTableSchemaProvider(schemaVersion);
    schemaProvider.getColumns().stream()
        .filter(c -> !c.isHidden())
        .forEach(c -> assertThat(query).contains(c.getName()));
  }

  @Test
  public void testGetViewQueryForCopyFileHistoryTable() {
    NamespaceKey namespaceKey = new NamespaceKey(ImmutableList.of("path", "to", "namespace"));
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    SystemIcebergViewQueryBuilder queryBuilder =
        new SystemIcebergViewQueryBuilder(
            schemaVersion,
            SupportedSystemIcebergTable.COPY_FILE_HISTORY.getTableName(),
            namespaceKey,
            "sampleUserName",
            true);

    String query = queryBuilder.getViewQuery();
    CopyFileHistoryTableSchemaProvider schemaProvider =
        new CopyFileHistoryTableSchemaProvider(schemaVersion);
    schemaProvider.getColumns().stream()
        .filter(c -> !c.isHidden())
        .forEach(c -> assertThat(query).contains(c.getName()));
  }

  @Test
  public void testGetViewQueryForUnsupportedViewName() {
    String unsupportedViewName = "unsupported_view";
    SystemIcebergViewQueryBuilder queryBuilder =
        new SystemIcebergViewQueryBuilder(
            1L, unsupportedViewName, new NamespaceKey(ImmutableList.of()), "sampleUserName", false);

    assertThrows(
        "Cannot provide a view query for view name",
        UnsupportedOperationException.class,
        queryBuilder::getViewQuery);
  }

  @Test
  public void testGetViewQueryForUnsupportedSchemaVersion() {
    long unsupportedSchemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal() + 1;
    SystemIcebergViewQueryBuilder queryBuilder =
        new SystemIcebergViewQueryBuilder(
            unsupportedSchemaVersion,
            "sampleViewName",
            new NamespaceKey(ImmutableList.of()),
            "sampleUserName",
            false);

    assertThrows(
        "Cannot provide a view query for schema version",
        UnsupportedOperationException.class,
        queryBuilder::getViewQuery);
  }
}
