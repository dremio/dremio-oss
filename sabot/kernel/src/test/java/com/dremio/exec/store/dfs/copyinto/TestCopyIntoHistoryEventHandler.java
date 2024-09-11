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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.planner.sql.handlers.SystemIcebergTableOpUtils;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.system.SystemIcebergTableManager;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class TestCopyIntoHistoryEventHandler {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  @Mock private SystemIcebergTableManager copyJobHistoryTableManager;
  @Mock private SystemIcebergTableManager copyFileHistoryTableManager;
  @Mock private SystemIcebergTablesStoragePlugin plugin;
  @Mock private OperatorContext context;
  @Mock OptionManager optionManager;
  @Captor private ArgumentCaptor<VectorContainer> containerCaptor;
  @Captor private ArgumentCaptor<List<ValueVector>> vectorsCaptor;
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
    SystemIcebergTableMetadata jobHistoryMetadata = mock(SystemIcebergTableMetadata.class);
    long schemaVersion = 2L;
    when(jobHistoryMetadata.getIcebergSchema())
        .thenReturn(CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion));
    when(jobHistoryMetadata.getPartitionSpec())
        .thenReturn(CopyJobHistoryTableSchemaProvider.getPartitionSpec(schemaVersion));
    SystemIcebergTableMetadata fileHistoryMetadata = mock(SystemIcebergTableMetadata.class);
    when(fileHistoryMetadata.getIcebergSchema())
        .thenReturn(CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion));
    when(fileHistoryMetadata.getPartitionSpec())
        .thenReturn(CopyFileHistoryTableSchemaProvider.getPartitionSpec(schemaVersion));
    when(plugin.getTableMetadata(
            ImmutableList.of(SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME)))
        .thenReturn(jobHistoryMetadata);
    when(plugin.getTableMetadata(
            ImmutableList.of(SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME)))
        .thenReturn(fileHistoryMetadata);
    when(plugin.getTable((String) null)).thenReturn(mock(Table.class));
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
    try (CopyIntoHistoryEventHandler historyEventsHandler =
        new CopyIntoHistoryEventHandler(context, plugin)) {
      historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
      historyEventsHandler.setCopyFileHistoryTableManager(copyFileHistoryTableManager);

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

      // Commit the errorInfo
      historyEventsHandler.commit();

      // Verify that tableManager.writeRecord() was called with the correct parameters
      verify(copyJobHistoryTableManager, times(1))
          .writeRecords(containerCaptor.capture(), vectorsCaptor.capture());
      verify(copyJobHistoryTableManager, times(1)).commit();

      verify(copyFileHistoryTableManager, times(1))
          .writeRecords(containerCaptor.capture(), vectorsCaptor.capture());
      verify(copyFileHistoryTableManager, times(1)).commit();

      assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(2);
      assertThat(vectorsCaptor.getValue().get(0).getValueCount())
          .isEqualTo(containerCaptor.getValue().getRecordCount());
      assertThat(containerCaptor.getValue().getSchema()).isNotNull();
    }
  }

  @Test
  public void testProcessAndCommitReachBatchThreshold() throws Exception {
    // update the threshold
    when(optionManager.getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE))
        .thenReturn(0L);

    // Create a CopyIntoErrorsHandler instance
    try (CopyIntoHistoryEventHandler historyEventsHandler =
        new CopyIntoHistoryEventHandler(context, plugin)) {
      historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
      historyEventsHandler.setCopyFileHistoryTableManager(copyFileHistoryTableManager);

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
      verify(copyFileHistoryTableManager, times(1))
          .writeRecords(containerCaptor.capture(), vectorsCaptor.capture());
      assertThat(vectorsCaptor.getValue().size()).isEqualTo(1L);
      // Verify that tableManager.commit() was not yet called
      verify(copyFileHistoryTableManager, times(0)).commit();

      // Commit the errorInfo
      historyEventsHandler.commit();

      // Verify that tableManager.writeRecord() was called with the correct parameters
      verify(copyJobHistoryTableManager, times(1))
          .writeRecords(containerCaptor.capture(), vectorsCaptor.capture());

      verify(copyFileHistoryTableManager, times(1))
          .writeRecords(containerCaptor.capture(), vectorsCaptor.capture());
      assertThat(containerCaptor.getValue().getRecordCount()).isEqualTo(0);
      assertThat(vectorsCaptor.getValue().get(0).getValueCount()).isEqualTo(0);
      assertThat(containerCaptor.getValue().getSchema()).isNotNull();

      // Verify that tableManager.commit() was called
      verify(copyJobHistoryTableManager, times(1)).commit();
      verify(copyFileHistoryTableManager, times(1)).commit();
    }
  }

  @Test
  public void testRevertCalledOnCommitException() throws Exception {
    try (CopyIntoHistoryEventHandler historyEventsHandler =
            new CopyIntoHistoryEventHandler(context, plugin);
        MockedStatic<IcebergUtils> icebergUtils = mockStatic(IcebergUtils.class);
        MockedStatic<SystemIcebergTableOpUtils> systemTableUtils =
            mockStatic(SystemIcebergTableOpUtils.class); ) {
      historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
      historyEventsHandler.setCopyFileHistoryTableManager(copyFileHistoryTableManager);

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

      QueryId queryId = QueryId.newBuilder().setPart1(123).setPart2(456).build();
      String queryString = QueryIdHelper.getQueryId(queryId);
      Snapshot snapshot = mock(Snapshot.class);
      FragmentHandle fragmentHandle = mock(FragmentHandle.class);

      when(context.getFragmentHandle()).thenReturn(fragmentHandle);
      when(fragmentHandle.getQueryId()).thenReturn(queryId);
      doThrow(new RuntimeException()).when(copyFileHistoryTableManager).commit();
      systemTableUtils
          .when(() -> SystemIcebergTableOpUtils.getStampedAppendSnapshot(any(), eq(queryString)))
          .thenReturn(Optional.of(snapshot));

      // Commit the errorInfo
      historyEventsHandler.commit();

      systemTableUtils.verify(
          () -> SystemIcebergTableOpUtils.getStampedAppendSnapshot(any(), eq(queryString)),
          times(1));
      icebergUtils.verify(
          () -> IcebergUtils.revertSnapshotFiles(any(), eq(snapshot), eq(queryString)), times(1));
    }
  }

  @Test
  public void testRevert() throws Exception {
    when(plugin.getTable((String) null)).thenReturn(mock(Table.class));
    try (CopyIntoHistoryEventHandler historyEventsHandler =
            new CopyIntoHistoryEventHandler(context, plugin);
        MockedStatic<IcebergUtils> icebergUtils = mockStatic(IcebergUtils.class);
        MockedStatic<SystemIcebergTableOpUtils> systemTableUtils =
            mockStatic(SystemIcebergTableOpUtils.class); ) {
      historyEventsHandler.setCopyJobHistoryTableManager(copyJobHistoryTableManager);
      historyEventsHandler.setCopyFileHistoryTableManager(copyFileHistoryTableManager);
      Snapshot snapshot = mock(Snapshot.class);

      systemTableUtils
          .when(() -> SystemIcebergTableOpUtils.getStampedAppendSnapshot(any(), eq("1")))
          .thenReturn(Optional.of(snapshot));

      historyEventsHandler.revert("1", new RuntimeException());

      systemTableUtils.verify(
          () -> SystemIcebergTableOpUtils.getStampedAppendSnapshot(any(), eq("1")), times(2));
      icebergUtils.verify(
          () -> IcebergUtils.revertSnapshotFiles(any(), eq(snapshot), eq("1")), times(2));
    }
  }
}
