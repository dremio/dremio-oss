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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergDmlOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.Lists;

public class TestIcebergCommitOpHelper extends BaseTestQuery {
  private final String DELETE_FILE_PATH1 = "file:///path/to/datafile-1+pre.parquet";
  private final String DELETE_FILE_PATH2 = "s3a://path/to/datafile-2-pre.parquet";
  private final String DELETE_FILE_PATH_WITH_CHINESE_CHARACTER = "s3a://路径/文件/数据文件.csv";

  @Test
  public void testDmlCommitDeleteDataFilePath() throws Exception {
    final String tableName = UUID.randomUUID().toString();
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      IcebergCommitOpHelper icebergCommitHelper = getIcebergCommitOpHelper(tempFolderLoc, datasetPath, IcebergCommandType.DELETE);

      List<String> deleteFilePaths = Lists.newArrayList(DELETE_FILE_PATH1, DELETE_FILE_PATH2);
      incomingVector = getIncomingVectorWithDeleteDataFilePath(deleteFilePaths, allocator);
      icebergCommitHelper.setup(incomingVector);
      icebergCommitHelper.consumeData(deleteFilePaths.size());
      IcebergOpCommitter committer = icebergCommitHelper.getIcebergOpCommitter();
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;
      Assert.assertEquals(deleteFilePaths.size(), dmlCommitter.getDeletedDataFilePaths().size());
      Assert.assertEquals(DELETE_FILE_PATH1, dmlCommitter.getDeletedDataFilePaths().get(0));
      Assert.assertEquals(DELETE_FILE_PATH2, dmlCommitter.getDeletedDataFilePaths().get(1));
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  @Test
  public void testDmlCommitDeleteDataFilePathWithNonAsciiCharacter() throws Exception {
    final String tableName = UUID.randomUUID().toString();
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    VectorContainer incomingVector = null;
    String tempFolderLoc = null;
    try {
      tempFolderLoc = TestUtilities.createTempDir();
      IcebergCommitOpHelper icebergCommitHelper = getIcebergCommitOpHelper(tempFolderLoc, datasetPath, IcebergCommandType.DELETE);

      List<String> deleteFilePaths = Lists.newArrayList(DELETE_FILE_PATH_WITH_CHINESE_CHARACTER);
      incomingVector = getIncomingVectorWithDeleteDataFilePath(deleteFilePaths, allocator);
      icebergCommitHelper.setup(incomingVector);
      icebergCommitHelper.consumeData(deleteFilePaths.size());
      IcebergOpCommitter committer = icebergCommitHelper.getIcebergOpCommitter();
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;
      Assert.assertEquals(deleteFilePaths.size(), dmlCommitter.getDeletedDataFilePaths().size());
      Assert.assertEquals(DELETE_FILE_PATH_WITH_CHINESE_CHARACTER, dmlCommitter.getDeletedDataFilePaths().get(0));
    } finally {
      FileUtils.deleteQuietly(new File(tempFolderLoc));
      if (incomingVector != null) {
        incomingVector.close();
      }
    }
  }

  private VectorContainer getIncomingVectorWithDeleteDataFilePath(List<String> deletedFilePaths, BufferAllocator allocator) throws IOException {
    VarCharVector fragment = new VarCharVector(RecordWriter.FRAGMENT_COLUMN, allocator);
    BigIntVector records = new BigIntVector(RecordWriter.RECORDS_COLUMN, allocator);
    VarCharVector pathVector = new VarCharVector(RecordWriter.PATH_COLUMN, allocator);
    VarBinaryVector metadata = new VarBinaryVector(RecordWriter.METADATA_COLUMN, allocator);
    IntVector partition = new IntVector(RecordWriter.PARTITION_COLUMN, allocator);
    BigIntVector fileSize = new BigIntVector(RecordWriter.FILESIZE_COLUMN, allocator);
    VarBinaryVector icebergMeta = new VarBinaryVector(RecordWriter.ICEBERG_METADATA_COLUMN, allocator);
    VarBinaryVector fileSchema = new VarBinaryVector(RecordWriter.FILE_SCHEMA_COLUMN, allocator);
    ListVector partitionData = ListVector.empty(RecordWriter.PARTITION_DATA_COLUMN, allocator);
    IntVector operationType = new IntVector(RecordWriter.OPERATION_TYPE_COLUMN, allocator);

    final int numFiles = deletedFilePaths.size();
    pathVector.allocateNew(numFiles);
    operationType.allocateNew(numFiles);
    for (int i = 0; i < numFiles; i++) {
      pathVector.set(i, new Text(deletedFilePaths.get(i)));
      operationType.set(i, OperationType.DELETE_DATAFILE.value);
    }

    VectorContainer container = new VectorContainer();
    container.add(fragment);
    container.add(records);
    container.add(pathVector);
    container.add(metadata);
    container.add(partition);
    container.add(fileSize);
    container.add(icebergMeta);
    container.add(fileSchema);
    container.add(partitionData);
    container.add(operationType);
    container.setRecordCount(numFiles);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return container;
  }

  private IcebergCommitOpHelper getIcebergCommitOpHelper(String metadataLocation, List<String> datasetPath, IcebergCommandType commandType) throws IOException {
    OperatorContext operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(null);
    OptionManager options = mock(OptionManager.class);
    when(operatorContext.getOptions()).thenReturn(options);
    WriterCommitterPOP writerCommitterPOP = getWriterCommitter(metadataLocation, operatorContext, datasetPath, commandType);

    return IcebergCommitOpHelper.getInstance(operatorContext, writerCommitterPOP);
  }

  private WriterCommitterPOP getWriterCommitter(
    String metadataLocation, OperatorContext operatorContext, List<String> datasetPath, IcebergCommandType commandType) throws IOException {
    WriterCommitterPOP writerCommitterPOP = mock(WriterCommitterPOP.class);

    IcebergTableProps icebergTableProps = mock(IcebergTableProps.class);
    when(icebergTableProps.getTableLocation()).thenReturn(metadataLocation);
    when(icebergTableProps.getIcebergOpType()).thenReturn(commandType);

    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    IcebergModel icebergModel = mock(IcebergModel.class);
    Configuration configuration = new Configuration();
    when(fileSystemPlugin.getFsConfCopy()).thenReturn(configuration);
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    when(fileSystemPlugin.getSystemUserFS()).thenReturn(fs);
    when(fileSystemPlugin.getIcebergModel(icebergTableProps, "testuser", operatorContext, null)).thenReturn(icebergModel);

    when(writerCommitterPOP.getTempLocation()).thenReturn(metadataLocation + "/queryID");
    when(writerCommitterPOP.getPlugin()).thenReturn(fileSystemPlugin);
    OpProps props = mock(OpProps.class);
    when(writerCommitterPOP.getProps()).thenReturn(props);
    when(writerCommitterPOP.getProps().getUserName()).thenReturn("testuser");
    when(writerCommitterPOP.getIcebergTableProps()).thenReturn(icebergTableProps);
    when(writerCommitterPOP.getDatasetConfig()).thenReturn(null);
    IcebergCommand icebergCommand = mock(IcebergCommand.class);
    OperatorStats operatorStats = mock(OperatorStats.class);
    DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
    IcebergMetadata icebergMetadata = new IcebergMetadata();
    icebergMetadata.setMetadataFileLocation(metadataLocation);
    datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
    IcebergDmlOperationCommitter committer = new IcebergDmlOperationCommitter(icebergCommand, operatorStats, datasetConfig);
    when(writerCommitterPOP.getDatasetConfig()).thenReturn((Optional<DatasetConfig>) Optional.of(datasetConfig));
    when(icebergModel.getDmlCommitter(operatorContext.getStats(), icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()), datasetConfig)).thenReturn(committer);
    return writerCommitterPOP;
  }

  private DatasetConfig getDatasetConfig(List<String> datasetPath) {
    NamespaceKey tableNSKey = new NamespaceKey(datasetPath);
    final FileConfig format = new FileConfig();
    format.setType(FileType.PARQUET);
    format.setLocation(tableNSKey.toString());
    final PhysicalDataset physicalDataset = new PhysicalDataset();
    physicalDataset.setFormatSettings(format);
    final ReadDefinition initialReadDef = ReadDefinition.getDefaultInstance();
    final ScanStats stats = new ScanStats();
    stats.setType(ScanStatsType.NO_EXACT_ROW_COUNT);
    stats.setScanFactor(ScanCostFactor.PARQUET.getFactor());
    stats.setRecordCount(500L);
    initialReadDef.setScanStats(stats);

    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setFullPathList(tableNSKey.getPathComponents());
    datasetConfig.setPhysicalDataset(physicalDataset);
    datasetConfig.setId(new EntityId(UUID.randomUUID().toString()));
    datasetConfig.setReadDefinition(initialReadDef);
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);

    datasetConfig.getPhysicalDataset().setInternalSchemaSettings(new UserDefinedSchemaSettings());
    return datasetConfig;
  }
}
