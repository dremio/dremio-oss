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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableMetadata;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableVectorContainerBuilder;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestSystemIcebergTableRecordWriter extends BaseTestQuery {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = allocatorRule.newAllocator("test-copy-into-parquet-writer", 0, Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  private SystemIcebergTableRecordWriter getRecordWriter(Path targetDirPath) {
    OperatorStats operatorStats = mock(OperatorStats.class);

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR))
        .thenReturn("none"); // compression shouldn't matter
    when(optionManager.getOption(ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR)).thenReturn(256L);
    when(optionManager.getOption(ExecConstants.PARQUET_MAXIMUM_PARTITIONS_VALIDATOR))
        .thenReturn(1L);
    when(optionManager.getOption(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR)).thenReturn(4096L);
    when(optionManager.getOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR))
        .thenReturn(256 * 1024 * 1024L);

    OperatorContext opContext = mock(OperatorContext.class);
    when(opContext.getFragmentHandle())
        .thenReturn(
            ExecProtos.FragmentHandle.newBuilder()
                .setMajorFragmentId(2323)
                .setMinorFragmentId(234234)
                .build());
    when(opContext.getAllocator()).thenReturn(this.allocator);
    when(opContext.getOptions()).thenReturn(optionManager);
    when(opContext.getStats()).thenReturn(operatorStats);

    SystemIcebergTablesStoragePlugin plugin = getMockedSystemIcebergTablesStoragePlugin();
    return new SystemIcebergTableRecordWriter(
        opContext,
        plugin,
        plugin.getTableMetadata(
            ImmutableList.of(SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName())),
        com.dremio.io.file.Path.of(targetDirPath.toString()));
  }

  private static SystemIcebergTablesStoragePlugin getMockedSystemIcebergTablesStoragePlugin() {
    try (SystemIcebergTablesStoragePlugin plugin = mock(SystemIcebergTablesStoragePlugin.class)) {
      FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
      when(plugin.getSystemUserFS()).thenReturn(fs);
      when(plugin.getFsConfCopy()).thenReturn(new Configuration());
      when(plugin.createIcebergFileIO(any(), any(), any(), any(), any()))
          .thenReturn(
              new DremioFileIO(
                  fs,
                  null,
                  null,
                  null,
                  null,
                  new HadoopFileSystemConfigurationAdapter(new Configuration())));
      when(plugin.createFS(notNull(), notNull(), notNull()))
          .thenReturn(HadoopFileSystem.getLocal(new Configuration()));
      SystemIcebergTableMetadata tableMetadata =
          new CopyJobHistoryTableMetadata(
              2L,
              4,
              "fake_plugin_name",
              "fake_plugin_path",
              SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName());
      when(plugin.getTableMetadata(
              ImmutableList.of(SupportedSystemIcebergTable.COPY_JOB_HISTORY.getTableName())))
          .thenReturn(tableMetadata);
      return plugin;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testWrite() throws Exception {
    Path targetDirPath = new Path(new Path(getDfsTestTmpSchemaLocation()), "testWrite");
    org.apache.hadoop.fs.FileSystem newFs = targetDirPath.getFileSystem(new Configuration());
    assertThat(newFs.mkdirs(targetDirPath)).isTrue();
    SystemIcebergTableRecordWriter recordWriter = getRecordWriter(targetDirPath);
    Path targetFilePath = new Path(targetDirPath, "testFile.parquet");

    try (VectorContainer container = getContainer(allocator)) {
      recordWriter.write(container, com.dremio.io.file.Path.of(targetFilePath.toString()));
      recordWriter.close();
    }

    for (FileStatus file : newFs.listStatus(targetDirPath)) {
      assertThat(file.getPath().getName().equals("testFile.parquet")).isTrue();
      assertThat(file.getLen()).isGreaterThan(0);
    }
  }

  private VectorContainer getContainer(BufferAllocator allocator) {
    CopyIntoFileLoadInfo info =
        new CopyIntoFileLoadInfo.Builder(
                "queryId",
                "queryUser",
                "tableName",
                "storageLocation",
                "filePath",
                new ExtendedFormatOptions(),
                FileType.CSV.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(2)
            .setRecordsRejectedCount(1)
            .setSnapshotId(12345L)
            .setBranch("someBranch")
            .setPipeName("dev-pipe")
            .setPipeId("d2981263-fb78-4f83-a4e1-27ba02074bae")
            .setProcessingStartTime(System.currentTimeMillis())
            .build();
    VectorContainer container =
        CopyJobHistoryTableVectorContainerBuilder.initializeContainer(
            allocator, new CopyJobHistoryTableSchemaProvider(2L).getSchema());
    CopyJobHistoryTableVectorContainerBuilder.writeRecordToContainer(
        container, info, 100, 13, 0L, 0L);
    return container;
  }
}
