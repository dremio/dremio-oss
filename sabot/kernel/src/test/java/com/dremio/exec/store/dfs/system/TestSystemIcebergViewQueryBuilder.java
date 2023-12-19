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
import static com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory.COPY_ERRORS_HISTORY_VIEW_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;


public class TestSystemIcebergViewQueryBuilder {


  @Test
  public void testGetViewQueryForCopyErrorsHistoryView() {
    String userName = "sampleUserName";
    long schemaVersion = 1L;
    SystemIcebergViewQueryBuilder queryBuilder = new SystemIcebergViewQueryBuilder(
      schemaVersion, COPY_ERRORS_HISTORY_VIEW_NAME, new NamespaceKey(ImmutableList.of(COPY_ERRORS_HISTORY_VIEW_NAME)),
      userName);

    String query = queryBuilder.getViewQuery();

    assertThat(query.contains("SELECT")).isTrue();
    assertThat(query.contains("FROM sys.\"" + COPY_JOB_HISTORY_TABLE_NAME + "\" AS jh")).isTrue();
    assertThat(query.contains("INNER JOIN sys.\"" + COPY_FILE_HISTORY_TABLE_NAME + "\" AS fh")).isTrue();
    assertThat(query.contains("WHERE jh.\"" + CopyJobHistoryTableSchemaProvider.getUserNameColName(schemaVersion) +
      "\" = '" + userName + "'")).isTrue();
  }

  @Test
  public void testGetViewQueryForCopyJobHistoryTable() {
    NamespaceKey namespaceKey = new NamespaceKey(ImmutableList.of("path", "to", "namespace"));
    SystemIcebergViewQueryBuilder queryBuilder = new SystemIcebergViewQueryBuilder(
      1L, COPY_JOB_HISTORY_TABLE_NAME, namespaceKey, "sampleUserName");

    String query = queryBuilder.getViewQuery();
    assertThat("SELECT * FROM path.\"to\".namespace".equals(query)).isTrue();
  }

  @Test
  public void testGetViewQueryForUnsupportedViewName() {
    String unsupportedViewName = "unsupported_view";
    SystemIcebergViewQueryBuilder queryBuilder = new SystemIcebergViewQueryBuilder(
      1L, unsupportedViewName, new NamespaceKey(ImmutableList.of()), "sampleUserName");

    assertThrows("Cannot provide a view query for view name", UnsupportedOperationException.class,
      queryBuilder::getViewQuery);
  }

  @Test
  public void testGetViewQueryForUnsupportedSchemaVersion() {
    long unsupportedSchemaVersion = 2;
    SystemIcebergViewQueryBuilder queryBuilder = new SystemIcebergViewQueryBuilder(
      unsupportedSchemaVersion, "sampleViewName", new NamespaceKey(ImmutableList.of()), "sampleUserName");

    assertThrows("Cannot provide a view query for schema version", UnsupportedOperationException.class, queryBuilder::getViewQuery);
  }
}
