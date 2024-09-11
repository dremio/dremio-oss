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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.test.AllocatorRule;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class TestSystemIcebergTableManager {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  @Mock private SystemIcebergTablesStoragePlugin plugin;
  @Mock private SystemIcebergTableMetadata tableMetadata;
  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = allocatorRule.newAllocator("test-system-iceberg-table-manager", 0, Long.MAX_VALUE);
    MockitoAnnotations.openMocks(this);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testCommitStampsSnapshots() {
    Table table = mock(Table.class);
    AppendFiles appendFiles = mock(AppendFiles.class);
    try (MockedStatic<IcebergUtils> icebergUtils = mockStatic(IcebergUtils.class)) {
      OperatorContext context = mock(OperatorContext.class, RETURNS_DEEP_STUBS);

      when(plugin.getTable((String) null)).thenReturn(table);
      when(table.newAppend()).thenReturn(appendFiles);
      when(appendFiles.appendFile(any())).thenReturn(appendFiles);
      when(context.getFragmentHandle().getQueryId())
          .thenReturn(QueryId.newBuilder().setPart1(1).setPart2(2).build());

      SystemIcebergTableManager systemIcebergTableManager =
          new SystemIcebergTableManager(context, plugin, tableMetadata);
      systemIcebergTableManager.setAppendFiles(appendFiles);

      systemIcebergTableManager.commit();

      icebergUtils.verify(
          () -> IcebergUtils.stampSnapshotUpdateWithDremioJobId(any(), anyString()), times(1));
      verify(appendFiles, times(1)).commit();
    }
  }
}
