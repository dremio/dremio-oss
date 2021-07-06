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
package com.dremio.exec.store.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.file.DataFileConstants;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.iceberg.manifestwriter.ManifestWritesHelper;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

public class TestManifestRecordWriter extends BaseTestQuery {

  private static final long HALF_MB = 524288;

  @Test
  public void testWriterWithOneBatch() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc);
      RecordWriter.OutputEntryListener outputEntryListener = mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
        }
      })).length);
      //Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc);
      RecordWriter.OutputEntryListener outputEntryListener = mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      refillIncomingVector(incomingVector, 3000, getTestPartitionSpec(), 1);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
        }
      })).length);
      //Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testWriterWithMultipleBatchExtedingLimit() throws Exception {
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc);
      RecordWriter.OutputEntryListener outputEntryListener = mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      verify(outputEntryListener, times(0)).recordsWritten(recordWrittenCaptor.capture(),
        fileSizeCaptor.capture(), pathCaptor.capture(), metadataCaptor.capture(),
        partitionCaptor.capture(), icebergMetadataCaptor.capture(), any());

      refillIncomingVector(incomingVector, 3000, getTestPartitionSpec(), 0);
      for (int i = 0; i < 60; i++) {
        manifestFileRecordWriter.writeBatch(0, incomingVector.getRecordCount());
      }
      manifestFileRecordWriter.close();

      verify(outputEntryListener, times(2)).recordsWritten(recordWrittenCaptor.capture(),
        fileSizeCaptor.capture(), pathCaptor.capture(), metadataCaptor.capture(),
        partitionCaptor.capture(), icebergMetadataCaptor.capture(), any());

      ManifestFile manifestFile2 = getManifestFile(icebergMetadataCaptor.getValue());
      assertTrue(manifestFile2.addedFilesCount() > 0);

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(2, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
        }
      })).length);
      //Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)

      assertEquals(1, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return (new File(dir, name)).length() >= HALF_MB - DataFileConstants.DEFAULT_SYNC_INTERVAL;
        }
      })).length);
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc);
      RecordWriter.OutputEntryListener outputEntryListener = mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVector(allocator, getTestPartitionSpec());

      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();

      verify(outputEntryListener, times(1)).recordsWritten(recordWrittenCaptor.capture(),
        fileSizeCaptor.capture(), pathCaptor.capture(), metadataCaptor.capture(),
        partitionCaptor.capture(), icebergMetadataCaptor.capture(), any());

      ManifestFile manifestFile = getManifestFile(icebergMetadataCaptor.getValue());

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder

      File[] files = metadataFolder.listFiles();
      List<File> maniFiles = Arrays.stream(files).filter(e -> e.getName().endsWith(".avro")).collect(Collectors.toList());
      assertEquals(1, maniFiles.size());
      long createdTime = maniFiles.get(0).lastModified();
      Thread.sleep(2000);
      //Create IcebergTable
      PartitionSpec spec = getTestPartitionSpec();

      HashMap<String, String> map = new HashMap();
      map.put("compatibility.snapshot-id-inheritance.enabled", "true");
      Table table = new HadoopTables(new Configuration()).create(spec.schema(), spec, SortOrder.unsorted(), map, tempFolderLoc);
      table.newAppend().appendManifest(manifestFile).commit();
      files = metadataFolder.listFiles();
      maniFiles = Arrays.stream(files).filter(e -> e.getName().endsWith(".avro") && !e.getName().startsWith("dremio-")).collect(Collectors.toList());
      assertEquals(2, maniFiles.size());
      maniFiles = maniFiles.stream().filter(e -> !e.getName().startsWith("snap")).collect(Collectors.toList());
      assertEquals(1, maniFiles.size());
      File afterAppendManifestFile = maniFiles.get(0);
      assertEquals(createdTime, afterAppendManifestFile.lastModified());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriter(tempFolderLoc);
      RecordWriter.OutputEntryListener outputEntryListener = mock(RecordWriter.OutputEntryListener.class);
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);
      incomingVector = getIncomingVectorWithDeleteDataFile(allocator, getTestPartitionSpec());
      ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);

      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 3);
      manifestFileRecordWriter.close();

      verify(outputEntryListener, times(2)).recordsWritten(recordWrittenCaptor.capture(),
        fileSizeCaptor.capture(), pathCaptor.capture(), metadataCaptor.capture(),
        partitionCaptor.capture(), icebergMetadataCaptor.capture(), any());

      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
        }
      })).length);
      //Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  private ManifestFileRecordWriter getNewManifestWriter(String metadataLocation) throws IOException {
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(null);
    EasyWriter easyWriter = getEasyWriter(metadataLocation, false);
    IcebergFormatConfig icebergFormatConfig = new IcebergFormatConfig();

    ManifestFileRecordWriter manifestFileRecordWriter = new ManifestFileRecordWriter(operatorContext, easyWriter, icebergFormatConfig) {
      @Override
      ManifestWritesHelper getManifestWritesHelper(EasyWriter writer, IcebergFormatConfig formatConfig) {
        return new ManifestWritesHelper(writer, formatConfig) {
          @Override
          protected PartitionSpec getPartitionSpec(WriterOptions writerOptions) {
            return getTestPartitionSpec();
          }

          @Override
          public boolean hasReachedMaxLen() {
            return length() + DataFileConstants.DEFAULT_SYNC_INTERVAL >= HALF_MB;
          }
        };
      }
    };

    manifestFileRecordWriter = spy(manifestFileRecordWriter);
    doAnswer((i) -> null).when(manifestFileRecordWriter).updateStats(anyLong(), anyLong());

    manifestFileRecordWriter.updateStats(0, 0);
    return manifestFileRecordWriter;
  }

  private PartitionSpec getTestPartitionSpec() {
    Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .bucket("id", 16)
      .build();
    return spec;
  }

  private VectorContainer getIncomingVector(BufferAllocator allocator, PartitionSpec spec) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    icebergMeta.allocateNew(2);
    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, "id_bucket=1");
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, "id_bucket=2");
    icebergMeta.set(0, getIcebergMetadata(dataFile1));
    icebergMeta.set(1, getIcebergMetadata(dataFile2));

    VarBinaryVector metadataVec = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    metadataVec.allocateNew(2);

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(metadataVec);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer getIncomingVectorWithDeleteDataFile(BufferAllocator allocator, PartitionSpec spec) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    icebergMeta.allocateNew(2);
    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, "id_bucket=1");
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, "id_bucket=2");
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec, "id_bucket=3");
    icebergMeta.set(0, getIcebergMetadata(dataFile1));
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));
    icebergMeta.set(2, getIcebergMetadata(dataFile3));

    VarBinaryVector metadataVec = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    metadataVec.allocateNew(2);

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(metadataVec);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }


  private DataFile getDatafile(String path, PartitionSpec spec, String partitionData) {
    DataFiles.Builder dataFile = DataFiles.builder(spec)
      .withPath(path)
      .withFileSizeInBytes(40)
      .withRecordCount(9);
    if(partitionData != null) {
      dataFile.withPartitionPath(partitionData);
    }
    return dataFile.build();
  }

  private byte[] getIcebergMetadata(DataFile dataFile) throws IOException {
    byte[] dataFileBytes = IcebergSerDe.serializeDataFile(dataFile);
    IcebergMetadataInformation icebergMetadataInformation = new IcebergMetadataInformation(
      dataFileBytes,
      IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE
    );
    return IcebergSerDe.serializeToByteArray(icebergMetadataInformation);
  }

  private byte[] getDeleteDataFileIcebergMetadata(DataFile dataFile) throws IOException {
    byte[] dataFileBytes = IcebergSerDe.serializeDataFile(dataFile);
    IcebergMetadataInformation icebergMetadataInformation = new IcebergMetadataInformation(
      dataFileBytes,
      IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE
    );
    return IcebergSerDe.serializeToByteArray(icebergMetadataInformation);
  }

  private void refillIncomingVector(VectorContainer vectorContainer, int records, PartitionSpec spec, int batchId) throws IOException {
    vectorContainer.setRecordCount(records);
    VarBinaryVector icebergMeta = vectorContainer.addOrGet(RecordWriter.ICEBERG_METADATA);
    icebergMeta.allocateNew(records);
    for (int i = 0; i < records; i++) {
      int partition = i % 16;
      DataFile dataFile = getDatafile("/path/to/datafile-" + i + "-"+ batchId+".parquet", spec, "id_bucket=" + partition);
      icebergMeta.setSafe(i, getIcebergMetadata(dataFile));
    }
    VarBinaryVector metadata = vectorContainer.addOrGet(RecordWriter.METADATA);
    metadata.allocateNew(records);
  }

  ManifestFile getManifestFile(byte[] manifestFileBytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(manifestFileBytes);
         ObjectInput in = new ObjectInputStream(bis)) {
      IcebergMetadataInformation icebergMetadataInformation = (IcebergMetadataInformation) in.readObject();
      if(icebergMetadataInformation.getIcebergMetadataFileType() == IcebergMetadataInformation.IcebergMetadataFileType.MANIFEST_FILE) {
        return IcebergSerDe.deserializeManifestFile(icebergMetadataInformation.getIcebergMetadataFileByte());
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriterWithSchemaDiscovery(tempFolderLoc);

      RecordWriter.OutputEntryListener outputEntryListener = (recordCount, fileSize, path, metadata, partitionNumber, icebergMetadataBytes, schemaBytes) -> {
        try {
          IcebergMetadataInformation icebergMetadata = IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
          assertEquals(IcebergMetadataInformation.IcebergMetadataFileType.MANIFEST_FILE, icebergMetadata.getIcebergMetadataFileType());
          ManifestFile manifestFile = IcebergSerDe.deserializeManifestFile(icebergMetadata.getIcebergMetadataFileByte());
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
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedAddedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, metadataFolder.listFiles((dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5).length);
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriterWithSchemaDiscovery(tempFolderLoc);
      AtomicInteger matchedIdx = new AtomicInteger(0);
      List<String> expectedDataFiles = Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      RecordWriter.OutputEntryListener outputEntryListener = (recordCount, fileSize, path, metadata, partitionNumber, icebergMetadataBytes, schemaBytes) -> {
        try {
          assertNull(schemaBytes);
          IcebergMetadataInformation icebergMetadata = IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
          assertEquals(IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE, icebergMetadata.getIcebergMetadataFileType());
          DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadata.getIcebergMetadataFileByte());
          assertEquals(expectedDataFiles.get(matchedIdx.getAndIncrement()), dataFile.path().toString());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      };
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedDeletedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 2);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(0, metadataFolder.listFiles((dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5).length);
      assertEquals(2, matchedIdx.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriterWithSchemaDiscovery(tempFolderLoc);
      AtomicInteger deleteDataFileMatchIdx = new AtomicInteger(0);
      List<String> expectedDeleteDataFiles = Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      AtomicBoolean foundManifest = new AtomicBoolean(false);
      RecordWriter.OutputEntryListener outputEntryListener = (recordCount, fileSize, path, metadata, partitionNumber, icebergMetadataBytes, schemaBytes) -> {
        try {
          IcebergMetadataInformation icebergMetadata = IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
          switch (icebergMetadata.getIcebergMetadataFileType()) {
            case DELETE_DATAFILE:
              DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadata.getIcebergMetadataFileByte());
              assertEquals(expectedDeleteDataFiles.get(deleteDataFileMatchIdx.getAndIncrement()), dataFile.path().toString());
              break;
            case MANIFEST_FILE:
              ManifestFile manifestFile = IcebergSerDe.deserializeManifestFile(icebergMetadata.getIcebergMetadataFileByte());
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
              fail("Unexpected type " + icebergMetadata.getIcebergMetadataFileType());
          }
        } catch (Exception e) {
          fail(e.getMessage());
        }
      };
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingUnpartitionedMixedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 4);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, metadataFolder.listFiles((dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5).length);
      assertEquals(2, deleteDataFileMatchIdx.get());
      assertTrue(foundManifest.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
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
      ManifestFileRecordWriter manifestFileRecordWriter = getNewManifestWriterWithSchemaDiscovery(tempFolderLoc);
      AtomicInteger deleteDataFileMatchIdx = new AtomicInteger(0);
      List<String> expectedDeleteDataFiles = Arrays.asList("/path/to/datafile-1-pre.parquet", "/path/to/datafile-2-pre.parquet");
      AtomicBoolean foundManifest = new AtomicBoolean(false);
      RecordWriter.OutputEntryListener outputEntryListener = (recordCount, fileSize, path, metadata, partitionNumber, icebergMetadataBytes, schemaBytes) -> {
        try {
          IcebergMetadataInformation icebergMetadata = IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes);
          switch (icebergMetadata.getIcebergMetadataFileType()) {
            case DELETE_DATAFILE:
              DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadata.getIcebergMetadataFileByte());
              assertEquals(expectedDeleteDataFiles.get(deleteDataFileMatchIdx.getAndIncrement()), dataFile.path().toString());
              break;
            case MANIFEST_FILE:
              ManifestFile manifestFile = IcebergSerDe.deserializeManifestFile(icebergMetadata.getIcebergMetadataFileByte());
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
              fail("Unexpected type " + icebergMetadata.getIcebergMetadataFileType());
          }
        } catch (Exception e) {
          fail(e.getMessage());
        }
      };
      RecordWriter.WriteStatsListener writeStatsListener = mock(RecordWriter.WriteStatsListener.class);

      incomingVector = incomingPartitionedMixedDataFiles(allocator);
      manifestFileRecordWriter.setup(incomingVector, outputEntryListener, writeStatsListener);
      manifestFileRecordWriter.writeBatch(0, 4);
      manifestFileRecordWriter.close();
      File metadataFolder = new File(tempFolderLoc, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(1, metadataFolder.listFiles((dir, name) -> name.endsWith(".avro") && name.length() == 32 + 4 + 5).length);
      assertEquals(2, deleteDataFileMatchIdx.get());
      assertTrue(foundManifest.get());
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if(incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  // Creates a batch schema with "n" columns. Each column is prefixed with test_field and an identification.
  private BatchSchema getBatchSchema(int colCount) {
    return new BatchSchema(
            IntStream.range(0, colCount)
                    .mapToObj(i -> Field.nullable("test_field" + i, new ArrowType.Int(64, true)))
                    .collect(Collectors.toList()));
  }

  private VectorContainer incomingUnpartitionedMixedDataFiles(BufferAllocator allocator) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(4);
    schema.allocateNew(4);

    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));

    BatchSchema schemaVal = getBatchSchema(2);
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpec(schemaVal, Collections.EMPTY_LIST, null);
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec, null);
    DataFile dataFile4 = getDatafile("/path/to/datafile-4-pre.parquet", spec, null);
    icebergMeta.set(2, getIcebergMetadata(dataFile3));
    icebergMeta.set(3, getIcebergMetadata(dataFile4));
    schema.set(3, schemaVal.serialize());

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(schema);
    container.setRecordCount(4);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer incomingPartitionedMixedDataFiles(BufferAllocator allocator) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(4);
    schema.allocateNew(4);

    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));

    BatchSchema schemaWithoutPartitions = getBatchSchema(2);
    List<Field> withPartitionFields = new ArrayList<>(schemaWithoutPartitions.getFields());
    withPartitionFields.add(Field.nullable("dir0", new ArrowType.Utf8()));
    withPartitionFields.add(Field.nullable("dir1", new ArrowType.Utf8()));
    BatchSchema schemaVal = new BatchSchema(withPartitionFields);
    PartitionSpec spec3 = IcebergUtils.getIcebergPartitionSpec(schemaVal, Arrays.asList("dir0"), null);
    DataFile dataFile3 = getDatafile("/path/to/datafile-3-pre.parquet", spec3, "dir0=1");

    PartitionSpec spec4 = IcebergUtils.getIcebergPartitionSpec(schemaVal, Arrays.asList("dir0", "dir1"), null);
    DataFile dataFile4 = getDatafile("/path/to/datafile-4-pre.parquet", spec4, "dir0=1/dir1=2");
    icebergMeta.set(2, getIcebergMetadata(dataFile3));
    icebergMeta.set(3, getIcebergMetadata(dataFile4));
    schema.set(3, schemaVal.serialize());

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(schema);
    container.setRecordCount(4);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }


  private VectorContainer incomingUnpartitionedDeletedDataFiles(BufferAllocator allocator) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(2);
    schema.allocateNew(2);

    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", PartitionSpec.unpartitioned(), null);
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", PartitionSpec.unpartitioned(), null);
    icebergMeta.set(0, getDeleteDataFileIcebergMetadata(dataFile1));
    icebergMeta.set(1, getDeleteDataFileIcebergMetadata(dataFile2));

    // Intentionally skipping schema set for other indexes in the batch.
    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(schema);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private VectorContainer incomingUnpartitionedAddedDataFiles(BufferAllocator allocator) throws IOException {
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    VarBinaryVector schema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);

    icebergMeta.allocateNew(2);
    schema.allocateNew(2);

    BatchSchema schemaVal = getBatchSchema(2);
    PartitionSpec spec = IcebergUtils.getIcebergPartitionSpec(schemaVal, Collections.EMPTY_LIST, null);

    DataFile dataFile1 = getDatafile("/path/to/datafile-1-pre.parquet", spec, null);
    DataFile dataFile2 = getDatafile("/path/to/datafile-2-pre.parquet", spec, null);
    icebergMeta.set(0, getIcebergMetadata(dataFile1));
    icebergMeta.set(1, getIcebergMetadata(dataFile2));
    schema.set(0, schemaVal.toByteArray());
    // Intentionally skipping schema set for other indexes in the batch.

    VectorContainer container = new VectorContainer();
    container.add(icebergMeta);
    container.add(schema);
    container.setRecordCount(2);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }


  private ManifestFileRecordWriter getNewManifestWriterWithSchemaDiscovery(String metadataLocation) throws IOException {
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(null);
    EasyWriter easyWriter = getEasyWriter(metadataLocation, true);
    IcebergFormatConfig icebergFormatConfig = new IcebergFormatConfig();

    ManifestFileRecordWriter manifestFileRecordWriter = new ManifestFileRecordWriter(operatorContext, easyWriter, icebergFormatConfig);
    manifestFileRecordWriter = spy(manifestFileRecordWriter);
    doAnswer((i) -> null).when(manifestFileRecordWriter).updateStats(anyLong(), anyLong());

    manifestFileRecordWriter.updateStats(0, 0);
    return manifestFileRecordWriter;
  }

  private EasyWriter getEasyWriter(String metadataLocation, boolean detectSchema) throws IOException {
    EasyWriter easyWriter = mock(EasyWriter.class);
    EasyFormatPlugin easyFormatPlugin = mock(EasyFormatPlugin.class);
    when(easyWriter.getFormatPlugin()).thenReturn(easyFormatPlugin);
    FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(easyFormatPlugin.getFsPlugin()).thenReturn(fileSystemPlugin);
    Configuration configuration = new Configuration();
    when(fileSystemPlugin.getFsConfCopy()).thenReturn(configuration);
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    when(fileSystemPlugin.getSystemUserFS()).thenReturn(fs);
    when(easyWriter.getLocation()).thenReturn(metadataLocation + "/queryID");
    WriterOptions writerOptions = mock(WriterOptions.class);
    when(easyWriter.getOptions()).thenReturn(writerOptions);
    IcebergTableProps icebergTableProps = mock(IcebergTableProps.class);
    when(icebergTableProps.getTableLocation()).thenReturn(metadataLocation);
    when(icebergTableProps.getFullSchema()).thenReturn(BatchSchema.EMPTY);
    when(icebergTableProps.isDetectSchema()).thenReturn(detectSchema);
    when(icebergTableProps.isMetadataRefresh()).thenReturn(true);
    when(writerOptions.getIcebergTableProps()).thenReturn(icebergTableProps);
    when(writerOptions.getExtendedProperty()).thenReturn(null);
    return easyWriter;
  }
}
