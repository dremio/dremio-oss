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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

public class TestSystemIcebergTableOpUtils {

  private MockedStatic<IcebergUtils> icebergUtils;

  @Before
  public void setup() {
    icebergUtils = mockStatic(IcebergUtils.class);
  }

  @After
  public void cleanup() {
    icebergUtils.close();
  }

  @Test
  public void testGetAppendSnapshot() {
    Table table = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class, RETURNS_DEEP_STUBS);

    when(table.snapshots()).thenReturn(ImmutableList.of(snapshot));
    when(snapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY)).thenReturn("1");
    when(snapshot.operation()).thenReturn(DataOperations.APPEND);

    assertEquals(
        Optional.of(snapshot), SystemIcebergTableOpUtils.getStampedAppendSnapshot(table, "1"));
  }

  @Test
  public void testGetRevertedAppendSnapshot() {
    Table table = mock(Table.class);
    Snapshot appendSnapshot = mock(Snapshot.class, RETURNS_DEEP_STUBS);
    Snapshot deleteSnapshot = mock(Snapshot.class, RETURNS_DEEP_STUBS);

    when(table.snapshots()).thenReturn(ImmutableList.of(appendSnapshot, deleteSnapshot));
    when(appendSnapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY))
        .thenReturn("1");
    when(deleteSnapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY))
        .thenReturn("1");
    when(appendSnapshot.operation()).thenReturn(DataOperations.APPEND);
    when(deleteSnapshot.operation()).thenReturn(DataOperations.DELETE);

    assertEquals(Optional.empty(), SystemIcebergTableOpUtils.getStampedAppendSnapshot(table, "1"));
  }

  @Test
  public void testCorroborateConsistency() {
    Table targetTable = mock(Table.class);
    Table fileTable = mock(Table.class);
    Table jobTable = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class, RETURNS_DEEP_STUBS);

    when(fileTable.snapshots()).thenReturn(ImmutableList.of(snapshot));
    when(jobTable.snapshots()).thenReturn(ImmutableList.of(snapshot));
    when(snapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY)).thenReturn("1");
    when(snapshot.operation()).thenReturn(DataOperations.APPEND);
    icebergUtils
        .when(() -> IcebergUtils.revertSnapshotFiles(any(), eq(snapshot), eq("1")))
        .thenReturn(true);

    SystemIcebergTableOpUtils.corroborateConsistencyWithTable(targetTable, fileTable, "1");
    SystemIcebergTableOpUtils.corroborateConsistencyWithTable(targetTable, jobTable, "1");
    icebergUtils.verify(
        () -> IcebergUtils.revertSnapshotFiles(any(), eq(snapshot), eq("1")), times(2));
  }

  @Test
  public void testCorroborateConsistencyRevertFailure() {
    Table targetTable = mock(Table.class);
    Table fileTable = mock(Table.class);
    Table jobTable = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class, RETURNS_DEEP_STUBS);

    when(fileTable.snapshots()).thenReturn(ImmutableList.of(snapshot));
    when(jobTable.snapshots()).thenReturn(ImmutableList.of(snapshot));
    when(snapshot.summary().get(IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY)).thenReturn("1");
    when(snapshot.operation()).thenReturn(DataOperations.APPEND);
    icebergUtils
        .when(() -> IcebergUtils.revertSnapshotFiles(eq(fileTable), eq(snapshot), eq("1")))
        .thenThrow(new RuntimeException());
    icebergUtils
        .when(() -> IcebergUtils.revertSnapshotFiles(eq(jobTable), eq(snapshot), eq("1")))
        .thenReturn(true);

    assertThatThrownBy(
            () -> {
              SystemIcebergTableOpUtils.corroborateConsistencyWithTable(
                  targetTable, fileTable, "1");
            })
        .hasMessageContaining("Failed to revert snapshot for table");
    SystemIcebergTableOpUtils.corroborateConsistencyWithTable(targetTable, jobTable, "1");
    icebergUtils.verify(
        () -> IcebergUtils.revertSnapshotFiles(fileTable, snapshot, "1"), times(10));
    icebergUtils.verify(() -> IcebergUtils.revertSnapshotFiles(jobTable, snapshot, "1"), times(1));
  }

  @Test
  public void testCorroborateConsistencyWithHistoryTables() throws Exception {
    QueryContext context = mock(QueryContext.class, RETURNS_DEEP_STUBS);
    SystemIcebergTablesStoragePlugin plugin = mock(SystemIcebergTablesStoragePlugin.class);
    Table table = mock(Table.class);
    SystemIcebergTableMetadata tableMetadata =
        mock(SystemIcebergTableMetadata.class, RETURNS_DEEP_STUBS);
    try (MockedStatic<SystemIcebergTableOpUtils> systemUtils =
        mockStatic(SystemIcebergTableOpUtils.class, CALLS_REAL_METHODS)) {
      when(context.getCatalog().getSource(anyString())).thenReturn(plugin);
      when(plugin.getTable(anyList())).thenReturn(table);
      when(plugin.getTableMetadata(anyList())).thenReturn(tableMetadata);
      icebergUtils.when(() -> IcebergUtils.getIcebergTable(any())).thenReturn(table);
      systemUtils
          .when(
              () ->
                  SystemIcebergTableOpUtils.corroborateConsistencyWithTable(
                      any(), any(), anyString()))
          .thenThrow(new RuntimeException());

      SystemIcebergTableOpUtils.corroborateConsistencyWithCopyHistoryTables(
          context, mock(CatalogEntityKey.class, RETURNS_DEEP_STUBS), "1");

      verify(context.getSabotQueryContext().getJobsRunner().get(), times(2))
          .runQueryAsJob(anyString(), anyString(), anyString(), eq(null));
    }
  }
}
