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
package com.dremio.exec.store.iceberg.manifestwriter;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPOP;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.util.TestUtilities;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestManifestRecordWriter extends BaseTestQuery {
  private static final long MANIFEST_TARGET_SIZE = 16384;

  @Test
  public void testErrorRecordPassThrough() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc, true);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVectorWithError(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> operationType = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<Long> rejectedRecordsCaptor = ArgumentCaptor.forClass(long.class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);

      verify(outputEntryListener, times(2))
          .recordsWritten(
              recordWrittenCaptor.capture(),
              fileSizeCaptor.capture(),
              pathCaptor.capture(),
              metadataCaptor.capture(),
              partitionCaptor.capture(),
              icebergMetadataCaptor.capture(),
              any(),
              any(),
              operationType.capture(),
              any(),
              rejectedRecordsCaptor.capture(),
              any());

      assertThat(
              operationType.getAllValues().stream()
                  .allMatch(OperationType.COPY_HISTORY_EVENT.value::equals))
          .isTrue();
      VarBinaryVector errorVector =
          (VarBinaryVector)
              VectorUtil.getVectorFromSchemaPath(incomingVector, RecordWriter.METADATA_COLUMN);
      assertThat(metadataCaptor.getAllValues().get(0)).isEqualTo(errorVector.get(0));
      assertThat(metadataCaptor.getAllValues().get(1)).isEqualTo(errorVector.get(1));
      assertThat(recordWrittenCaptor.getAllValues()).isEqualTo(ImmutableList.of(0L, 0L));
      assertThat(pathCaptor.getAllValues()).isEqualTo(ImmutableList.of("file_1", "file_2"));
      assertThat(fileSizeCaptor.getAllValues()).isEqualTo(ImmutableList.of(1L, 2L));
      assertThat(rejectedRecordsCaptor.getAllValues()).isEqualTo(ImmutableList.of(3L, 4L));

      manifestFileRecordWriter.close();
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithOneBatch() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
                        }
                      }))
              .length);
      // Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithMultipleBatch() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      refillIncomingVector(incomingVector, 3000, getTestPartitionSpec(), 1);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
                        }
                      }))
              .length);
      // Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithMultipleBatchExceedingLimit() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> operationType = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<Long> rejectedRecordsCaptor = ArgumentCaptor.forClass(long.class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      verify(outputEntryListener, times(0))
          .recordsWritten(
              recordWrittenCaptor.capture(),
              fileSizeCaptor.capture(),
              pathCaptor.capture(),
              metadataCaptor.capture(),
              partitionCaptor.capture(),
              icebergMetadataCaptor.capture(),
              any(),
              any(),
              operationType.capture(),
              any(),
              rejectedRecordsCaptor.capture(),
              any());

      refillIncomingVector(incomingVector, 1500, getTestPartitionSpec(), 0);
      for (int i = 0; i < 3; i++) {
        manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      }
      manifestFileRecordWriter.close();

      verify(outputEntryListener, times(2))
          .recordsWritten(
              recordWrittenCaptor.capture(),
              fileSizeCaptor.capture(),
              pathCaptor.capture(),
              metadataCaptor.capture(),
              partitionCaptor.capture(),
              icebergMetadataCaptor.capture(),
              any(),
              any(),
              operationType.capture(),
              any(),
              rejectedRecordsCaptor.capture(),
              any());

      ManifestFile manifestFile2 =
          getManifestFile(icebergMetadataCaptor.getValue(), operationType.getValue());
      assertTrue(manifestFile2.addedFilesCount() > 0);

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          2,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
                        }
                      }))
              .length);
      // Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)

      assertEquals(
          1,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return (new File(dir, name)).length() >= MANIFEST_TARGET_SIZE;
                        }
                      }))
              .length);
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithAbort() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Long> rejectedRecordsCaptor = ArgumentCaptor.forClass(long.class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      verify(outputEntryListener, times(0))
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
              rejectedRecordsCaptor.capture(),
              any());

      refillIncomingVector(incomingVector, 1500, getTestPartitionSpec(), 0);
      for (int i = 0; i < 3; i++) {
        manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      }
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      File[] files = metadataFolder.listFiles();
      List<File> maniFiles =
          Arrays.stream(files)
              .filter(e -> e.getName().endsWith(".avro") && !e.getName().startsWith("dremio-"))
              .collect(Collectors.toList());
      assertEquals(2, maniFiles.size());
      manifestFileRecordWriter.abort();
      files = metadataFolder.listFiles();
      maniFiles =
          Arrays.stream(files)
              .filter(e -> e.getName().endsWith(".avro") && !e.getName().startsWith("dremio-"))
              .collect(Collectors.toList());
      assertEquals(0, maniFiles.size());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testManifestFileOverwrite() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> operationType = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<Long> rejectedRecordsCaptor = ArgumentCaptor.forClass(long.class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();

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
              operationType.capture(),
              any(),
              rejectedRecordsCaptor.capture(),
              any());

      ManifestFile manifestFile =
          getManifestFile(icebergMetadataCaptor.getValue(), operationType.getValue());

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder

      File[] files = metadataFolder.listFiles();
      List<File> maniFiles =
          Arrays.stream(files)
              .filter(e -> e.getName().endsWith(".avro"))
              .collect(Collectors.toList());
      assertEquals(1, maniFiles.size());
      long createdTime = maniFiles.get(0).lastModified();
      Thread.sleep(2000);
      // Create IcebergTable
      PartitionSpec spec = getTestPartitionSpec();

      Map<String, String> map = new HashMap();
      map.put("compatibility.snapshot-id-inheritance.enabled", "true");
      Table table =
          new HadoopTables(new Configuration())
              .create(spec.schema(), spec, SortOrder.unsorted(), map, tempFolderLoc);
      table.newAppend().appendManifest(manifestFile).commit();
      files = metadataFolder.listFiles();
      maniFiles =
          Arrays.stream(files)
              .filter(e -> e.getName().endsWith(".avro") && !e.getName().startsWith("dremio-"))
              .collect(Collectors.toList());
      assertEquals(2, maniFiles.size());
      maniFiles =
          maniFiles.stream()
              .filter(e -> !e.getName().startsWith("snap"))
              .collect(Collectors.toList());
      assertEquals(1, maniFiles.size());
      File afterAppendManifestFile = maniFiles.get(0);
      assertEquals(createdTime, afterAppendManifestFile.lastModified());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithDeleteDataFile() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriter(tempFolderLoc, false);
      RecordWriter.OutputEntryListener outputEntryListener =
          mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVectorWithDeleteDataFile(allocator, getTestPartitionSpec());
      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Long> rejectedRecordsCaptor = ArgumentCaptor.forClass(long.class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 3);
      manifestFileRecordWriter.close();

      verify(outputEntryListener, times(2))
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
              rejectedRecordsCaptor.capture(),
              any());

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
                        }
                      }))
              .length);
      // Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  private ManifestFileRecordWriter getNewManifestWriter(String metadataLocation, boolean isInsertOp)
      throws IOException {
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(null);
    OptionManager options = mock(OptionManager.class);
    when(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX))
        .thenReturn(CatalogOptions.METADATA_LEAF_COLUMN_MAX.getDefault().getNumVal());
    when(operatorContext.getOptions()).thenReturn(options);
    IcebergManifestWriterPOP manifestWriterPOP =
        getManifestWriter(metadataLocation, false, isInsertOp);

    ManifestFileRecordWriter manifestFileRecordWriter =
        new ManifestFileRecordWriter(operatorContext, manifestWriterPOP) {
          @Override
          ManifestWritesHelper getManifestWritesHelper(
              IcebergManifestWriterPOP writer, OperatorContext context) {
            return new ManifestWritesHelper(writer) {
              @Override
              public PartitionSpec getPartitionSpec(WriterOptions writerOptions) {
                return getTestPartitionSpec();
              }

              @Override
              public boolean hasReachedMaxLen() {
                return length() >= MANIFEST_TARGET_SIZE;
              }
            };
          }
        };

    manifestFileRecordWriter = spy(manifestFileRecordWriter);
    doAnswer((i) -> null).when(manifestFileRecordWriter).updateStats(anyLong(), anyLong());

    manifestFileRecordWriter.updateStats(0, 0);
    return manifestFileRecordWriter;
  }

  private static PartitionSpec getTestPartitionSpec() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 16).build();
    return spec;
  }

  private VectorContainer getIncomingVectorWithError(BufferAllocator allocator, PartitionSpec spec)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    icebergMeta.allocateNew(2);
    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, "id_bucket=1");
    icebergMeta.set(0, getAddDataFileIcebergMetadata(dataFile1));
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, "id_bucket=2");
    icebergMeta.set(1, getAddDataFileIcebergMetadata(dataFile2));
    icebergMeta.setValueCount(2);

    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    operationType.allocateNew(2);
    operationType.set(0, OperationType.COPY_HISTORY_EVENT.value);
    operationType.set(1, OperationType.COPY_HISTORY_EVENT.value);
    operationType.setValueCount(2);

    VarBinaryVector metadataVec = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    metadataVec.allocateNew(2);
    CopyIntoFileLoadInfo info1 =
        new CopyIntoFileLoadInfo.Builder(
                "a161ab64-cbcc-42e8-ace4-7e30530fcc00",
                "testUser",
                "testTable",
                "@S3",
                "file_0",
                new ExtendedFormatOptions(),
                FileType.CSV.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(1)
            .setRecordsRejectedCount(3)
            .build();
    metadataVec.set(0, FileLoadInfo.Util.getJson(info1).getBytes());
    CopyIntoFileLoadInfo info2 =
        new CopyIntoFileLoadInfo.Builder(
                "a161ab64-cbcc-42e8-ace4-7e30530fcc00",
                "testUser",
                "testTable",
                "@S3",
                "file_1",
                new ExtendedFormatOptions(),
                FileType.JSON.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
            .setRecordsLoadedCount(2)
            .setRecordsRejectedCount(4)
            .build();
    metadataVec.set(1, FileLoadInfo.Util.getJson(info2).getBytes());
    metadataVec.setValueCount(2);

    BigIntVector recordCountVec = new BigIntVector(RecordWriter.RECORDS_COLUMN, allocator);
    recordCountVec.allocateNew(2);
    recordCountVec.set(0, 0L);
    recordCountVec.set(1, 0L);
    recordCountVec.setValueCount(2);

    VarCharVector pathVec = new VarCharVector(RecordWriter.PATH_COLUMN, allocator);
    pathVec.allocateNew(2);
    pathVec.set(0, new Text("file_1"));
    pathVec.set(1, new Text("file_2"));
    pathVec.setValueCount(2);

    BigIntVector fileSizeVec = new BigIntVector(RecordWriter.FILESIZE_COLUMN, allocator);
    fileSizeVec.allocateNew(2);
    fileSizeVec.set(0, 1L);
    fileSizeVec.set(1, 2L);
    fileSizeVec.setValueCount(2);

    BigIntVector rejectedRecordsVector =
        new BigIntVector(RecordWriter.REJECTED_RECORDS_COLUMN, allocator);
    rejectedRecordsVector.allocateNew(2);
    rejectedRecordsVector.set(0, 3L);
    rejectedRecordsVector.set(1, 4L);
    rejectedRecordsVector.setValueCount(2);

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(metadataVec);
    container.add(recordCountVec);
    container.add(pathVec);
    container.add(fileSizeVec);
    container.add(rejectedRecordsVector);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer getIncomingVector(BufferAllocator allocator, PartitionSpec spec)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    icebergMeta.allocateNew(2);
    operationType.allocateNew(2);
    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, "id_bucket=1");
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, "id_bucket=2");
    icebergMeta.set(0, getAddDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.ADD_DATAFILE.value);
    icebergMeta.set(1, getAddDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.ADD_DATAFILE.value);

    VarBinaryVector metadataVec = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    metadataVec.allocateNew(2);

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(metadataVec);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer getIncomingVectorWithDeleteDataFile(
      BufferAllocator allocator, PartitionSpec spec) throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    icebergMeta.allocateNew(3);
    operationType.allocateNew(3);
    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, "id_bucket=1");
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, "id_bucket=2");
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec, "id_bucket=3");
    icebergMeta.set(0, getAddDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.ADD_DATAFILE.value);
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.DELETE_DATAFILE.value);
    icebergMeta.set(2, getAddDataFileIcebergMetadata(dataFile3));
    operationType.set(2, OperationType.ADD_DATAFILE.value);

    VarBinaryVector metadataVec = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    metadataVec.allocateNew(3);

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(metadataVec);
    container.setRecordCount(3);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private DataFile getDatafile(String path, PartitionSpec spec, String partitionData) {
    DataFiles.Builder dataFile =
        DataFiles.builder(spec).withPath(path).withFileSizeInBytes(40).withRecordCount(9);
    if (partitionData != null) {
      dataFile.withPartitionPath(partitionData);
    }
    return dataFile.build();
  }

  private byte[] getAddDataFileIcebergMetadata(DataFile dataFile) throws IOException {
    byte[] dataFileBytes = IcebergSerDe.serializeDataFile(dataFile);
    IcebergMetadataInformation icebergMetadataInformation =
        new IcebergMetadataInformation(dataFileBytes);
    return IcebergSerDe.serializeToByteArray(icebergMetadataInformation);
  }

  private byte[] getDeleteDataFileIcebergMetadata(DataFile dataFile) throws IOException {
    byte[] dataFileBytes = IcebergSerDe.serializeDataFile(dataFile);
    IcebergMetadataInformation icebergMetadataInformation =
        new IcebergMetadataInformation(dataFileBytes);
    return IcebergSerDe.serializeToByteArray(icebergMetadataInformation);
  }

  private void refillIncomingVector(
      VectorContainer vectorContainer, int records, PartitionSpec spec, int batchId)
      throws IOException {
    vectorContainer.setRecordCount(records);
    VarBinaryVector icebergMeta = vectorContainer.addOrGet(RecordWriter.ICEBERG_METADATA);
    IntVector operationType = vectorContainer.addOrGet(RecordWriter.OPERATION_TYPE);
    icebergMeta.allocateNew(records);
    operationType.allocateNew(records);
    for (int i = 0; i < records; i++) {
      int partition = i % 16;
      DataFile dataFile =
          getDatafile(
              "/path/to/datafile-" + i + "-" + batchId + ".parquet",
              spec,
              "id_bucket=" + partition);
      icebergMeta.setSafe(i, getAddDataFileIcebergMetadata(dataFile));
      operationType.setSafe(i, OperationType.ADD_DATAFILE.value);
    }
    VarBinaryVector metadata = vectorContainer.addOrGet(RecordWriter.METADATA);
    metadata.allocateNew(records);
  }

  private ManifestFile getManifestFile(byte[] manifestFileBytes, Integer operationTypeValue)
      throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(manifestFileBytes);
        ObjectInput in = new ObjectInputStream(bis)) {
      IcebergMetadataInformation icebergMetadataInformation =
          (IcebergMetadataInformation) in.readObject();
      if (OperationType.valueOf(operationTypeValue) == OperationType.ADD_MANIFESTFILE) {
        return IcebergSerDe.deserializeManifestFile(
            icebergMetadataInformation.getIcebergMetadataFileByte());
      } else {
        throw new IOException("Wrong Type");
      }
    }
  }

  @Test
  public void testFSMetaRefreshAddedFileWrites() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriterWithSchemaDiscovery(tempFolderLoc, false);

      RecordWriter.OutputEntryListener outputEntryListener =
          (recordCount,
              fileSize,
              path,
              metadata,
              partitionNumber,
              icebergMetadataBytes,
              schemaBytes,
              partition,
              operationTypeValue,
              partitionValue,
              rejectedRecordCount,
              referencedDataFiles) -> {
            try {
              IcebergMetadataInformation icebergMetadata =
                  IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
              assertEquals(
                  OperationType.ADD_MANIFESTFILE, OperationType.valueOf(operationTypeValue));
              ManifestFile manifestFile =
                  IcebergSerDe.deserializeManifestFile(
                      icebergMetadata.getIcebergMetadataFileByte());
              assertTrue(manifestFile.hasAddedFiles());
              assertEquals(Integer.valueOf(2), manifestFile.addedFilesCount());
              assertEquals(Long.valueOf(18), manifestFile.addedRowsCount());
              assertTrue(manifestFile.partitions().isEmpty());
              assertEquals(0, manifestFile.partitions().size());

              BatchSchema schema = BatchSchema.deserialize(schemaBytes);
              assertEquals(2, schema.getFieldCount());
              assertNotNull(schema.findField("test_field0"));
              assertNotNull(schema.findField("test_field1"));
            } catch (Exception e) {
              fail(e.getMessage());
            }
          };
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedAddedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          metadataFolder.listFiles(
                  (dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5)
              .length);
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testFSMetaRefreshRemovedFileWrites() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriterWithSchemaDiscovery(tempFolderLoc, false);
      AtomicInteger matchedIdx = new AtomicInteger(0);
      List<String> expectedDataFiles =
          Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      RecordWriter.OutputEntryListener outputEntryListener =
          (recordCount,
              fileSize,
              path,
              metadata,
              partitionNumber,
              icebergMetadataBytes,
              schemaBytes,
              partition,
              operationTypeValue,
              partitionValue,
              rejectedRecordCount,
              referencedDataFiles) -> {
            try {
              assertNull(schemaBytes);
              IcebergMetadataInformation icebergMetadata =
                  IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
              assertEquals(
                  OperationType.DELETE_DATAFILE, OperationType.valueOf(operationTypeValue));
              DataFile dataFile =
                  IcebergSerDe.deserializeDataFile(icebergMetadata.getIcebergMetadataFileByte());
              assertEquals(
                  expectedDataFiles.get(matchedIdx.getAndIncrement()), dataFile.path().toString());
            } catch (Exception e) {
              fail(e.getMessage());
            }
          };
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedDeletedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          0,
          metadataFolder.listFiles(
                  (dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5)
              .length);
      assertEquals(2, matchedIdx.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testFSMetaRefreshAddedAndRemovedFileWrites() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriterWithSchemaDiscovery(tempFolderLoc, false);
      AtomicInteger deleteDataFileMatchIdx = new AtomicInteger(0);
      List<String> expectedDeleteDataFiles =
          Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      AtomicBoolean foundManifest = new AtomicBoolean(false);
      RecordWriter.OutputEntryListener outputEntryListener =
          (recordCount,
              fileSize,
              path,
              metadata,
              partitionNumber,
              icebergMetadataBytes,
              schemaBytes,
              partition,
              operationTypeValue,
              partitionValue,
              rejectedRecordCount,
              referencedDataFiles) -> {
            try {
              IcebergMetadataInformation icebergMetadata =
                  IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
              OperationType operationType = OperationType.valueOf(operationTypeValue);
              switch (operationType) {
                case DELETE_DATAFILE:
                  DataFile dataFile =
                      IcebergSerDe.deserializeDataFile(
                          icebergMetadata.getIcebergMetadataFileByte());
                  assertEquals(
                      expectedDeleteDataFiles.get(deleteDataFileMatchIdx.getAndIncrement()),
                      dataFile.path().toString());
                  break;
                case ADD_MANIFESTFILE:
                  ManifestFile manifestFile =
                      IcebergSerDe.deserializeManifestFile(
                          icebergMetadata.getIcebergMetadataFileByte());
                  assertTrue(manifestFile.hasAddedFiles());
                  assertEquals(0, manifestFile.partitions().size());
                  assertEquals(Integer.valueOf(2), manifestFile.addedFilesCount());
                  assertEquals(Long.valueOf(18), manifestFile.addedRowsCount());
                  assertTrue(manifestFile.partitions().isEmpty());

                  BatchSchema schema = BatchSchema.deserialize(schemaBytes);
                  assertEquals(2, schema.getFieldCount());
                  assertNotNull(schema.findField("test_field0"));
                  assertNotNull(schema.findField("test_field1"));
                  foundManifest.set(true);
                  break;
                default:
                  fail("Unexpected type " + operationType);
              }
            } catch (Exception e) {
              fail(e.getMessage());
            }
          };
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedMixedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 4);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          metadataFolder.listFiles(
                  (dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5)
              .length);
      assertEquals(2, deleteDataFileMatchIdx.get());
      assertTrue(foundManifest.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testFSPartitionedMetaRefreshAddedAndRemovedFileWrites() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter =
          getNewManifestWriterWithSchemaDiscovery(tempFolderLoc, false);
      AtomicInteger deleteDataFileMatchIdx = new AtomicInteger(0);
      List<String> expectedDeleteDataFiles =
          Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      AtomicBoolean foundManifest = new AtomicBoolean(false);
      RecordWriter.OutputEntryListener outputEntryListener =
          (recordCount,
              fileSize,
              path,
              metadata,
              partitionNumber,
              icebergMetadataBytes,
              schemaBytes,
              partition,
              operationTypeValue,
              partitionValue,
              rejectedRecordCount,
              referencedDataFiles) -> {
            try {
              IcebergMetadataInformation icebergMetadata =
                  IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
              OperationType operationType = OperationType.valueOf(operationTypeValue);
              switch (operationType) {
                case DELETE_DATAFILE:
                  DataFile dataFile =
                      IcebergSerDe.deserializeDataFile(
                          icebergMetadata.getIcebergMetadataFileByte());
                  assertEquals(
                      expectedDeleteDataFiles.get(deleteDataFileMatchIdx.getAndIncrement()),
                      dataFile.path().toString());
                  break;
                case ADD_MANIFESTFILE:
                  ManifestFile manifestFile =
                      IcebergSerDe.deserializeManifestFile(
                          icebergMetadata.getIcebergMetadataFileByte());
                  assertTrue(manifestFile.hasAddedFiles());
                  assertEquals(2, manifestFile.partitions().size());
                  assertEquals(Integer.valueOf(2), manifestFile.addedFilesCount());
                  assertEquals(Long.valueOf(18), manifestFile.addedRowsCount());

                  BatchSchema schema = BatchSchema.deserialize(schemaBytes);
                  assertEquals(4, schema.getFieldCount());
                  assertNotNull(schema.findField("test_field0"));
                  assertNotNull(schema.findField("test_field1"));
                  assertNotNull(schema.findField("dir0"));
                  assertNotNull(schema.findField("dir1"));
                  foundManifest.set(true);
                  break;
                default:
                  fail("Unexpected type " + operationType);
              }
            } catch (Exception e) {
              fail(e.getMessage());
            }
          };
      RecordWriter.WriteStatsListener writeStatsListener =
          mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingPartitionedMixedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 4);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          metadataFolder.listFiles(
                  (dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5)
              .length);
      assertEquals(2, deleteDataFileMatchIdx.get());
      assertTrue(foundManifest.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  // Creates a batch schema with "n" columns. Each column is prefixed with test_field and an
  // identification.
  private BatchSchema getBatchSchema(int colCount) {
    return new BatchSchema(
        IntStream.range(0, colCount)
            .mapToObj(i -> Field.nullable("test_field" + i, new ArrowType.Int(64, true)))
            .collect(Collectors.toList()));
  }

  private VectorContainer incomingUnpartitionedMixedDataFiles(BufferAllocator allocator)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(4);
    operationType.allocateNew(4);
    schema.allocateNew(4);

    DataFile dataFile1 =
        getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 =
        getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.DELETE_DATAFILE.value);
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.DELETE_DATAFILE.value);

    BatchSchema schemaVal = getBatchSchema(2);
    PartitionSpec spec =
        IcebergUtils.getIcebergPartitionSpec(schemaVal, Collections.EMPTY_LIST, null);
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec, null);
    DataFile dataFile4 = getDatafile("/path/to/datafile-4-pre.parquet", spec, null);
    icebergMeta.set(2, getAddDataFileIcebergMetadata(dataFile3));
    operationType.set(2, OperationType.ADD_DATAFILE.value);
    icebergMeta.set(3, getAddDataFileIcebergMetadata(dataFile4));
    operationType.set(3, OperationType.ADD_DATAFILE.value);
    schema.set(3, schemaVal.serialize());

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(schema);
    container.setRecordCount(4);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer incomingPartitionedMixedDataFiles(BufferAllocator allocator)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(4);
    operationType.allocateNew(4);
    schema.allocateNew(4);

    DataFile dataFile1 =
        getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 =
        getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.DELETE_DATAFILE.value);
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.DELETE_DATAFILE.value);

    BatchSchema schemaWithoutPartitions = getBatchSchema(2);
    List<Field> withPartitionFields = new ArrayList<>(schemaWithoutPartitions.getFields());
    withPartitionFields.add(Field.nullable("dir0", new ArrowType.Utf8()));
    withPartitionFields.add(Field.nullable("dir1", new ArrowType.Utf8()));
    BatchSchema schemaVal = new BatchSchema(withPartitionFields);
    PartitionSpec spec3 =
        IcebergUtils.getIcebergPartitionSpec(schemaVal, Arrays.asList("dir0"), null);
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec3, "dir0=1");

    PartitionSpec spec4 =
        IcebergUtils.getIcebergPartitionSpec(schemaVal, Arrays.asList("dir0", "dir1"), null);
    DataFile dataFile4 = getDatafile("/path/to/datafile-4-pre.parquet", spec4, "dir0=1/dir1=2");
    icebergMeta.set(2, getAddDataFileIcebergMetadata(dataFile3));
    operationType.set(2, OperationType.ADD_DATAFILE.value);
    icebergMeta.set(3, getAddDataFileIcebergMetadata(dataFile4));
    operationType.set(3, OperationType.ADD_DATAFILE.value);
    schema.set(3, schemaVal.serialize());

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(schema);
    container.setRecordCount(4);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer incomingUnpartitionedDeletedDataFiles(BufferAllocator allocator)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(2);
    operationType.allocateNew(2);
    schema.allocateNew(2);

    DataFile dataFile1 =
        getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 =
        getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.DELETE_DATAFILE.value);
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.DELETE_DATAFILE.value);

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(schema);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer incomingUnpartitionedAddedDataFiles(BufferAllocator allocator)
      throws IOException {
    VarBinaryVector icebergMeta =
        new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(2);
    operationType.allocateNew(2);
    schema.allocateNew(2);

    BatchSchema schemaVal = getBatchSchema(2);
    PartitionSpec spec =
        IcebergUtils.getIcebergPartitionSpec(schemaVal, Collections.EMPTY_LIST, null);

    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, null);
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, null);
    icebergMeta.set(0, getAddDataFileIcebergMetadata(dataFile1));
    operationType.set(0, OperationType.ADD_DATAFILE.value);
    icebergMeta.set(1, getAddDataFileIcebergMetadata(dataFile2));
    operationType.set(1, OperationType.ADD_DATAFILE.value);
    schema.set(0, schemaVal.toByteArray());
    // Intentionally skipping schema set for other indexes in the batch.

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(operationType);
    container.add(schema);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private ManifestFileRecordWriter getNewManifestWriterWithSchemaDiscovery(
      String metadataLocation, boolean isInsertOp) throws IOException {
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(null);
    OptionManager options = mock(OptionManager.class);
    when(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX))
        .thenReturn(CatalogOptions.METADATA_LEAF_COLUMN_MAX.getDefault().getNumVal());
    when(operatorContext.getOptions()).thenReturn(options);
    IcebergManifestWriterPOP manifestWriterPOP =
        getManifestWriter(metadataLocation, true, isInsertOp);

    ManifestFileRecordWriter manifestFileRecordWriter =
        new ManifestFileRecordWriter(operatorContext, manifestWriterPOP);
    manifestFileRecordWriter = spy(manifestFileRecordWriter);
    doAnswer((i) -> null).when(manifestFileRecordWriter).updateStats(anyLong(), anyLong());

    manifestFileRecordWriter.updateStats(0, 0);
    return manifestFileRecordWriter;
  }

  private IcebergManifestWriterPOP getManifestWriter(
      String metadataLocation, boolean detectSchema, boolean isInsertOp) throws IOException {
    IcebergManifestWriterPOP manifestWriterPOP = mock(IcebergManifestWriterPOP.class);
    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    Configuration configuration = new Configuration();
    when(fileSystemPlugin.getFsConfCopy()).thenReturn(configuration);
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    when(fileSystemPlugin.getSystemUserFS()).thenReturn(fs);
    when(fileSystemPlugin.createFS(any(), any(), any())).thenReturn(fs);
    when(manifestWriterPOP.getLocation()).thenReturn(metadataLocation + "/queryID");
    when(manifestWriterPOP.getPlugin()).thenReturn(fileSystemPlugin);
    WriterOptions writerOptions = mock(WriterOptions.class);
    when(manifestWriterPOP.getOptions()).thenReturn(writerOptions);
    IcebergTableProps icebergTableProps = mock(IcebergTableProps.class);
    when(icebergTableProps.getTableLocation()).thenReturn(metadataLocation);
    when(icebergTableProps.getFullSchema()).thenReturn(BatchSchema.EMPTY);
    when(icebergTableProps.isDetectSchema()).thenReturn(detectSchema);
    when(icebergTableProps.isMetadataRefresh()).thenReturn(true);
    when(icebergTableProps.getPartitionColumnNames()).thenReturn(Collections.singletonList("id"));
    when(icebergTableProps.getFullSchema())
        .thenReturn(BatchSchema.of(Field.nullable("id", new ArrowType.Int(32, false))));
    if (isInsertOp) {
      when(icebergTableProps.getIcebergOpType()).thenReturn(IcebergCommandType.INSERT);
    }
    IcebergWriterOptions icebergOptions =
        new ImmutableIcebergWriterOptions.Builder().setIcebergTableProps(icebergTableProps).build();
    TableFormatWriterOptions tableFormatOptions =
        new ImmutableTableFormatWriterOptions.Builder()
            .setIcebergSpecificOptions(icebergOptions)
            .build();
    when(writerOptions.getTableFormatOptions()).thenReturn(tableFormatOptions);
    when(writerOptions.getExtendedProperty()).thenReturn(null);
    return manifestWriterPOP;
  }

  @Test
  public void testLazyManifestWriter() throws Exception {
    FileIO mockIo = mock(FileIO.class);
    OutputFile mockOutputFile = mock(OutputFile.class);
    ManifestWriter<DataFile> mockManifestWriter = mock(ManifestWriter.class);

    when(mockIo.newOutputFile(anyString())).thenReturn(mockOutputFile);

    try (MockedStatic<ManifestFiles> manifestFiles = Mockito.mockStatic(ManifestFiles.class)) {
      manifestFiles
          .when(() -> ManifestFiles.write(null, mockOutputFile))
          .thenReturn(mockManifestWriter);

      LazyManifestWriter lazyManifestWriter = new LazyManifestWriter(mockIo, "hello", null);
      assertTrue(!lazyManifestWriter.isInitialized());

      lazyManifestWriter.getInstance();
      assertTrue(lazyManifestWriter.isInitialized());
    }
  }
}
