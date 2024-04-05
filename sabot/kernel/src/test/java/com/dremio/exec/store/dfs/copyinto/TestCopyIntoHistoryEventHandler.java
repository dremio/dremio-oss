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
package com.dremio.exec.store.dfs.copyinto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.system.SystemIcebergTableManager;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestCopyIntoHistoryEventHandler {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  @Mock private SystemIcebergTableManager copyJobHistoryTableManager;
  @Mock private SystemIcebergTableManager copyFileJobHistoryTableManager;
  @Mock private SystemIcebergTablesStoragePlugin plugin;
  @Mock private OperatorContext context;
  @Mock OptionManager optionManager;
  @Captor private ArgumentCaptor<Long> numSuccessCaptor;
  @Captor private ArgumentCaptor<Long> numErrorsCaptor;
  @Captor private ArgumentCaptor<List<String>> filePathsCaptor;
  @Captor private ArgumentCaptor<VectorContainer> containerCaptor;
  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = allocatorRule.newAllocator("test-copy-into-history-handler", 0, Long.MAX_VALUE);

    MockitoAnnotations.openMocks(this);
    SabotContext sabotContext = mock(SabotContext.class);
    when(plugin.getContext()).thenReturn(sabotContext);
    CatalogService catalogService = mock(CatalogService.class);
    when((sabotContext.getCatalogService())).thenReturn(catalogService);
    Catalog catalog = mock(Catalog.class);
    when(catalogService.getCatalog(any())).thenReturn(catalog);
    SystemIcebergTableMetadata tableMetadata = mock(SystemIcebergTableMetadata.class);
    when(tableMetadata.getSchemaVersion()).thenReturn(1L);
    when(plugin.getTableMetadata(anyList())).thenReturn(tableMetadata);
    when(context.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE))
        .thenReturn(1000L);
    when(context.getAllocator()).thenReturn(allocator);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testProcessAndCommit() throws Exception {
    // Create a CopyIntoErrorsHandler instance
    CopyIntoHistoryEventHandler historyEventsHandler =
        new CopyIntoHistoryEventHandler(context, plugin);
    historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
    historyEventsHandler.setCopyFileHistoryTableManager(copyFileJobHistoryTableManager);

    // Create a CopyIntoErrorInfo instance
    CopyIntoFileLoadInfo errorInfo =
        new CopyIntoFileLoadInfo.Builder(
                "queryId",
                "queryUser",
                "tableName",
                "storageLocation",
                "filePath1",
                new ExtendedFormatOptions(),
                FileType.JSON.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(3)
            .setRecordsRejectedCount(2)
            .setRecordDelimiter("\n")
            .setFieldDelimiter(",")
            .setSnapshotId(12345L)
            .build();

    // Process the errorInfo
    historyEventsHandler.process(errorInfo);

    // Verify the state after processing
    assertThat(historyEventsHandler.getNumberOfRejectedRecords()).isEqualTo(2);
    assertThat(historyEventsHandler.getNumberOfLoadedRecords()).isEqualTo(3);
    assertThat(
            historyEventsHandler.getFileLoadInfos().stream()
                .map(CopyIntoFileLoadInfo::getFilePath)
                .collect(Collectors.toList()))
        .isEqualTo(ImmutableList.of("filePath1"));

    // Create a CopyIntoErrorInfo instance
    errorInfo =
        new CopyIntoFileLoadInfo.Builder(
                "queryId",
                "queryUser",
                "tableName",
                "storageLocation",
                "filePath2",
                new ExtendedFormatOptions(),
                FileType.JSON.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(4)
            .setRecordsRejectedCount(12)
            .setRecordDelimiter("\n")
            .setFieldDelimiter(",")
            .setSnapshotId(23456L)
            .build();

    // Process the errorInfo
    historyEventsHandler.process(errorInfo);

    // Verify the updated state after processing
    assertThat(historyEventsHandler.getNumberOfRejectedRecords()).isEqualTo(14);
    assertThat(historyEventsHandler.getNumberOfLoadedRecords()).isEqualTo(7);
    ImmutableList<String> expectedFilePaths = ImmutableList.of("filePath1", "filePath2");
    assertThat(
            historyEventsHandler.getFileLoadInfos().stream()
                .map(CopyIntoFileLoadInfo::getFilePath)
                .collect(Collectors.toList()))
        .isEqualTo(expectedFilePaths);

    // Commit the errorInfo
    historyEventsHandler.commit();

    // Verify that tableManager.writeRecord() was called with the correct parameters
    verify(copyJobHistoryTableManager, times(1)).writeRecords(containerCaptor.capture());
    verify(copyJobHistoryTableManager, times(1)).commit();
    assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(1);

    verify(copyFileJobHistoryTableManager, times(1)).writeRecords(containerCaptor.capture());
    verify(copyFileJobHistoryTableManager, times(1)).commit();
    assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(2);
  }

  @Test
  public void testProcessAndCommitReachBatchThreshold() throws Exception {
    // update the threshold
    when(optionManager.getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE))
        .thenReturn(0L);

    // Create a CopyIntoErrorsHandler instance
    CopyIntoHistoryEventHandler historyEventsHandler =
        new CopyIntoHistoryEventHandler(context, plugin);
    historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
    historyEventsHandler.setCopyFileHistoryTableManager(copyFileJobHistoryTableManager);

    // Create a CopyIntoErrorInfo instance
    CopyIntoFileLoadInfo errorInfo =
        new CopyIntoFileLoadInfo.Builder(
                "queryId",
                "queryUser",
                "tableName",
                "storageLocation",
                "filePath1",
                new ExtendedFormatOptions(),
                FileType.JSON.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(3)
            .setRecordsRejectedCount(2)
            .setRecordDelimiter("\n")
            .setFieldDelimiter(",")
            .setSnapshotId(12345L)
            .build();

    // Process the errorInfo
    historyEventsHandler.process(errorInfo);

    // Verify that tableManager.writeRecord() was called with the correct parameters
    verify(copyFileJobHistoryTableManager, times(1)).writeRecords(containerCaptor.capture());
    assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(1);
    // Verify that tableManager.commit() was not yet called
    verify(copyFileJobHistoryTableManager, times(0)).commit();

    // Commit the errorInfo
    historyEventsHandler.commit();

    // Verify that tableManager.writeRecord() was called with the correct parameters
    verify(copyJobHistoryTableManager, times(1)).writeRecords(containerCaptor.capture());
    assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(1);

    verify(copyFileJobHistoryTableManager, times(1)).writeRecords(containerCaptor.capture());

    // Verify that tableManager.commit() was called
    verify(copyJobHistoryTableManager, times(1)).commit();
    verify(copyFileJobHistoryTableManager, times(1)).commit();
  }
}
