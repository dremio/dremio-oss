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
package com.dremio.exec.store.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestParquetRecordWriter extends BaseTestQuery {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private ParquetRecordWriter mockParquetRecordWriter(
      Configuration hadoopConf,
      Path targetPath,
      int minorFragmentId,
      BufferAllocator ALLOCATOR,
      Long targetBlockSize)
      throws Exception {
    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR))
        .thenReturn("none"); // compression shouldn't matter
    when(optionManager.getOption(ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR)).thenReturn(256L);
    when(optionManager.getOption(ExecConstants.PARQUET_MAXIMUM_PARTITIONS_VALIDATOR))
        .thenReturn(1L);
    when(optionManager.getOption(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR)).thenReturn(4096L);
    when(optionManager.getOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR))
        .thenReturn(256 * 1024 * 1024L);

    OperatorStats operatorStats = mock(OperatorStats.class);

    OperatorContext opContext = mock(OperatorContext.class);
    when(opContext.getFragmentHandle())
        .thenReturn(
            ExecProtos.FragmentHandle.newBuilder()
                .setMajorFragmentId(2323)
                .setMinorFragmentId(minorFragmentId)
                .build());
    when(opContext.getAllocator()).thenReturn(ALLOCATOR);
    when(opContext.getOptions()).thenReturn(optionManager);
    when(opContext.getStats()).thenReturn(operatorStats);

    ParquetWriter writerConf = mock(ParquetWriter.class, Mockito.RETURNS_DEEP_STUBS);
    when(writerConf.getLocation()).thenReturn(targetPath.toUri().toString());
    OpProps props = mock(OpProps.class);
    when(writerConf.getProps()).thenReturn(props);
    when(writerConf.getProps().getUserName()).thenReturn("testuser");
    when(writerConf.getOptions().getTableFormatOptions().getTargetFileSize())
        .thenReturn(targetBlockSize);

    FileSystemPlugin fsPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    when(fsPlugin.createFS((String) notNull(), (String) notNull(), (OperatorContext) notNull()))
        .thenReturn(HadoopFileSystem.getLocal(hadoopConf));
    when(fsPlugin.getFsConfCopy()).thenReturn(hadoopConf);
    when(writerConf.getPlugin()).thenReturn(fsPlugin);

    return new ParquetRecordWriter(opContext, writerConf, new ParquetFormatConfig());
  }

  @Test
  public void testFileSize() throws Exception {
    final Path tmpSchemaPath = new Path(getDfsTestTmpSchemaLocation());
    final Path targetPath = new Path(tmpSchemaPath, "testFileSize");
    final Configuration hadoopConf = new Configuration();
    final FileSystem newFs = targetPath.getFileSystem(hadoopConf);
    assertThat(newFs.mkdirs(targetPath)).isTrue();

    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final BufferAllocator ALLOCATOR =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        mockParquetRecordWriter(hadoopConf, targetPath, 234234, ALLOCATOR, null);

    RecordWriter.OutputEntryListener outputEntryListener =
        mock(RecordWriter.OutputEntryListener.class);
    RecordWriter.WriteStatsListener writeStatsListener =
        mock(RecordWriter.WriteStatsListener.class);
    ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Long> recordRejectedCaptor = ArgumentCaptor.forClass(long.class);

    BigIntVector bigIntVector = new BigIntVector("key", ALLOCATOR);
    bigIntVector.allocateNew(2);
    bigIntVector.set(0, 52459253098448904L);
    bigIntVector.set(1, 1116675951L);

    VectorContainer container = new VectorContainer();
    container.add(bigIntVector);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    writer.setup(container, outputEntryListener, writeStatsListener);
    writer.startPartition(WritePartition.NONE);
    writer.writeBatch(0, container.getRecordCount());

    container.clear();
    writer.close();

    verify(outputEntryListener, times(1))
        .recordsWritten(
            recordWrittenCaptor.capture(),
            fileSizeCaptor.capture(),
            pathCaptor.capture(),
            metadataCaptor.capture(),
            partitionCaptor.capture(),
            icebergMetadataCaptor.capture(),
            any(),
            any(),
            any(),
            any(),
            recordRejectedCaptor.capture(),
            any());

    for (FileStatus file : newFs.listStatus(targetPath)) {
      if (file.getPath()
          .toString()
          .endsWith(".parquet")) { // complex243_json is in here for some reason?
        assertEquals(Long.valueOf(file.getLen()), fileSizeCaptor.getValue());
        break;
      }
    }

    container.close();
    ALLOCATOR.close();
  }

  @Test
  public void testBlockSizeWithTarget() throws Exception {
    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final BufferAllocator ALLOCATOR =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        prepareRecordWriter("testFileSizeWithTarget", 234236, ALLOCATOR, 100 * 1024 * 1024L);

    assertTrue(writer.getBlockSize() == 100 * 1024 * 1024L);

    writer.close();
    ALLOCATOR.close();
  }

  @Test
  public void testBlockSizeWithNullTarget() throws Exception {
    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final BufferAllocator ALLOCATOR =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        prepareRecordWriter("testFileSizeWithTarget", 234236, ALLOCATOR, null);

    assertTrue(writer.getBlockSize() == 256 * 1024 * 1024L);

    writer.close();
    ALLOCATOR.close();
  }

  @Test
  public void testBlockSizeWithInvalidTarget() throws Exception {
    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final BufferAllocator ALLOCATOR =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        prepareRecordWriter("testFileSizeWithInvalidTarget", 234236, ALLOCATOR, 0L);

    assertTrue(writer.getBlockSize() == 256 * 1024 * 1024L);

    writer.close();
    ALLOCATOR.close();
  }

  @Test
  public void testOutOfMemory() throws Exception {
    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final BufferAllocator ALLOCATOR = allocatorRule.newAllocator("test-parquet-writer", 0, 128);

    ParquetRecordWriter writer = prepareRecordWriter("testOutOfMemory", 234235, ALLOCATOR, null);

    RecordWriter.OutputEntryListener outputEntryListener =
        mock(RecordWriter.OutputEntryListener.class);
    RecordWriter.WriteStatsListener writeStatsListener =
        mock(RecordWriter.WriteStatsListener.class);

    BigIntVector bigIntVector = new BigIntVector("key", ALLOCATOR);
    bigIntVector.allocateNew(2);
    bigIntVector.set(0, 52459253098448904L);
    bigIntVector.set(1, 1116675951L);

    VectorContainer container = new VectorContainer();
    container.add(bigIntVector);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    writer.setup(container, outputEntryListener, writeStatsListener);
    writer.startPartition(WritePartition.NONE);
    writer.writeBatch(0, container.getRecordCount());

    container.clear();
    try {
      writer.close();
    } catch (Exception e) {
      // ignore any exception in close(), but all the buffers should be released.
    }

    container.close();
    ALLOCATOR.close();
  }

  @Test
  public void testFilterCopyIntoErrorColumn() throws Exception {
    BufferAllocator allocator =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        prepareRecordWriter("filterCopyIntoErrorColumn", 234236, allocator, null);

    VarCharVector historyVector = getCopyHistoryVector(10, 2, 1, allocator);
    VectorContainer container = getContainerWithErrorColumn(3, historyVector, allocator);

    RecordWriter.OutputEntryListener mockOutputEntryListener =
        mock(RecordWriter.OutputEntryListener.class);
    RecordWriter.WriteStatsListener mockWriteStatsListener =
        mock(RecordWriter.WriteStatsListener.class);
    writer.setup(container, mockOutputEntryListener, mockWriteStatsListener);
    writer.startPartition(WritePartition.NONE);
    int recordWritten = writer.writeBatch(0, container.getRecordCount());
    assertThat(5).isEqualTo(recordWritten);

    container.clear();
    writer.close();
    container.close();
    allocator.close();
  }

  @Test
  public void testErrorRecordPassThrough() throws Exception {
    BufferAllocator allocator =
        allocatorRule.newAllocator("test-parquet-writer", 0, Long.MAX_VALUE);

    ParquetRecordWriter writer =
        prepareRecordWriter("errorRecordPassThrough", 234236, allocator, null);

    VarCharVector copyHistoryVector = getCopyHistoryVector(10, 1, 2, allocator);
    String fileLoadInfoExpectedJson = new String(copyHistoryVector.get(0));
    VectorContainer container = getContainerWithErrorColumn(2, copyHistoryVector, allocator);

    RecordWriter.OutputEntryListener mockOutputEntryListener =
        mock(RecordWriter.OutputEntryListener.class);
    RecordWriter.WriteStatsListener mockWriteStatsListener =
        mock(RecordWriter.WriteStatsListener.class);
    ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> operationTypeCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> recordRejectedCaptor = ArgumentCaptor.forClass(long.class);

    writer.setup(container, mockOutputEntryListener, mockWriteStatsListener);
    writer.startPartition(WritePartition.NONE);
    writer.writeBatch(0, container.getRecordCount());
    writer.close();

    verify(mockOutputEntryListener, times(2))
        .recordsWritten(
            recordWrittenCaptor.capture(),
            fileSizeCaptor.capture(),
            pathCaptor.capture(),
            metadataCaptor.capture(),
            partitionCaptor.capture(),
            icebergMetadataCaptor.capture(),
            any(),
            any(),
            operationTypeCaptor.capture(),
            any(),
            recordRejectedCaptor.capture(),
            any());

    assertThat(recordWrittenCaptor.getAllValues()).isEqualTo(ImmutableList.of(0L, 0L));
    assertThat(pathCaptor.getAllValues()).isEqualTo(ImmutableList.of("file_0", "file_1"));
    assertThat(
            operationTypeCaptor.getAllValues().stream()
                .allMatch(OperationType.COPY_HISTORY_EVENT.value::equals))
        .isTrue();
    assertThat(new String(metadataCaptor.getAllValues().get(0)))
        .isEqualTo(fileLoadInfoExpectedJson);
    assertThat(recordRejectedCaptor.getAllValues()).isEqualTo(ImmutableList.of(5L, 5L));

    container.clear();
    container.close();
    allocator.close();
  }

  private ParquetRecordWriter prepareRecordWriter(
      String directoryName, int minorFragmentId, BufferAllocator allocator, Long targetBolockSize)
      throws Exception {
    final Path tmpSchemaPath = new Path(getDfsTestTmpSchemaLocation());
    final Path targetPath = new Path(tmpSchemaPath, directoryName);
    final Configuration hadoopConf = new Configuration();
    final FileSystem newFs = targetPath.getFileSystem(hadoopConf);
    assertThat(newFs.mkdirs(targetPath)).isTrue();
    return mockParquetRecordWriter(
        hadoopConf, targetPath, minorFragmentId, allocator, targetBolockSize);
  }

  private VectorContainer getContainerWithErrorColumn(
      int numCols, VarCharVector copyHistoryVector, BufferAllocator allocator) {
    int numRecords = copyHistoryVector.getValueCount();
    VectorContainer container = new VectorContainer();
    for (int i = 0; i < numCols - 1; i++) {
      BigIntVector colVector = new BigIntVector("col_" + (i + 1), allocator);
      colVector.allocateNew(numRecords);
      colVector.setValueCount(numRecords);
      IntStream.range(0, numRecords).forEach(j -> colVector.set(j, new Random().nextLong()));
      container.add(colVector);
    }
    container.add(copyHistoryVector);
    container.setRecordCount(numRecords);
    container.buildSchema();
    return container;
  }

  private VarCharVector getCopyHistoryVector(
      int numRecords, int errorRecordFreq, int numFiles, BufferAllocator allocator) {
    VarCharVector copyHistoryVector =
        new VarCharVector(ColumnUtils.COPY_HISTORY_COLUMN_NAME, allocator);
    copyHistoryVector.allocateNew(numRecords);
    copyHistoryVector.setValueCount(numRecords);
    int fileCounter = 0;
    for (int i = 0; i < numRecords; i++) {
      if (i % errorRecordFreq != 0) {
        continue;
      }
      CopyIntoFileLoadInfo copyIntoFileLoadInfo =
          new CopyIntoFileLoadInfo.Builder(
                  "a161ab64-cbcc-42e8-ace4-7e30530fcc00",
                  "testUser",
                  "testTable",
                  "@S3",
                  "file_" + (fileCounter % numFiles),
                  new ExtendedFormatOptions(),
                  FileType.CSV.name(),
                  CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
              .setRecordsLoadedCount(1)
              .setRecordsRejectedCount(1)
              .build();
      fileCounter++;
      copyHistoryVector.set(i, new Text(FileLoadInfo.Util.getJson(copyIntoFileLoadInfo)));
    }
    return copyHistoryVector;
  }
}
