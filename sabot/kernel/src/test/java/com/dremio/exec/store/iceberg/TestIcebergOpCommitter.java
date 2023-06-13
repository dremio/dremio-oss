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

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.BIT;
import static com.dremio.common.expression.CompleteType.DATE;
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.expression.CompleteType.TIME;
import static com.dremio.common.expression.CompleteType.TIMESTAMP;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.exec.store.iceberg.model.IcebergDmlOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IncrementalMetadataRefreshCommitter;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestIcebergOpCommitter extends BaseTestQuery implements SupportsTypeCoercionsAndUpPromotions {

  private final String folder = Files.createTempDir().getAbsolutePath();
  private final DatasetCatalogGrpcClient client = new DatasetCatalogGrpcClient(getSabotContext().getDatasetCatalogBlockingStub().get());
  private IcebergModel icebergModel;
  private OperatorStats operatorStats;
  private OperatorContext operatorContext;

  private final BatchSchema schema = BatchSchema.of(
    Field.nullablePrimitive("id", new ArrowType.Int(64, true)),
    Field.nullablePrimitive("data", new ArrowType.Utf8()));

  @Before
  public void beforeTest() {
    this.operatorStats = mock(OperatorStats.class);
    doNothing().when(operatorStats).addLongStat(any(), anyLong());
    this.operatorContext = mock(OperatorContext.class);
    when(operatorContext.getStats()).thenReturn(operatorStats);
    ExecutionControls executionControls = mock(ExecutionControls.class);
    when(executionControls.lookupExceptionInjection(any(), any())).thenReturn(null);
    when(operatorContext.getExecutionControls()).thenReturn(executionControls);
    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE)).thenReturn(true);
    when(operatorContext.getOptions()).thenReturn(optionManager);
    icebergModel = getIcebergModel(TEMP_SCHEMA);
  }

  public String initialiseTableWithLargeSchema(BatchSchema schema, String tableName) throws IOException {
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    final File tableFolder = new File(folder, tableName);
    tableFolder.mkdirs();

    DatasetConfig config = getDatasetConfig(datasetPath);

    IcebergOpCommitter fullRefreshCommitter = icebergModel.getFullMetadataRefreshCommitter(tableName, datasetPath,
      tableFolder.toPath().toString(),
      tableName,
      icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
      schema,
      Collections.emptyList(), config, operatorStats, null);
    fullRefreshCommitter.commit();

    DataFile dataFile1 = getDatafile("books/add1.parquet");
    DataFile dataFile2 = getDatafile("books/add2.parquet");
    DataFile dataFile3 = getDatafile("books/add3.parquet");
    DataFile dataFile4 = getDatafile("books/add4.parquet");
    DataFile dataFile5 = getDatafile("books/add5.parquet");

    String tag = getTag(datasetPath);
    config.setTag(tag);
    Table table = getIcebergTable(icebergModel, new File(folder, tableName));
    TableOperations tableOperations = ((BaseTable) table).operations();
    String metadataFileLocation = tableOperations.current().metadataFileLocation();
    IcebergMetadata icebergMetadata = new IcebergMetadata();
    icebergMetadata.setMetadataFileLocation(metadataFileLocation);
    config.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

    IcebergOpCommitter incrementalRefreshCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName, datasetPath,
            tableFolder.toPath().toString(),
            tableName,
            icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
            schema,
            Collections.emptyList(), true, config);

    ManifestFile m1 = writeManifest(tableFolder, "manifestFile1", dataFile1, dataFile2, dataFile3, dataFile4, dataFile5);
    incrementalRefreshCommitter.consumeManifestFile(m1);
    incrementalRefreshCommitter.commit();
    return incrementalRefreshCommitter.getRootPointer();
  }

  private String getTag(List<String> datasetPath) {
    return client.getCatalogServiceApi()
            .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build()).getTag();
  }

  @Test
  public void testAddOnlyMetadataRefreshCommitter() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
              datasetPath,
              tableName,
              tableFolder.toPath().toString(),
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(), true, datasetConfig);
      DataFile dataFile6 = getDatafile("books/add1.parquet");
      DataFile dataFile7 = getDatafile("books/add2.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile6, dataFile7);
      insertTableCommitter.consumeManifestFile(m1);
      insertTableCommitter.commit();
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      Assert.assertEquals(2, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile1")) {
          Assert.assertEquals(5, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
          Assert.assertEquals(0, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(0, (int) manifestFile.existingFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testDeleteThenAddMetadataRefreshCommitter() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter metaDataRefreshCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(),
        tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1, dataFile2);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile3);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile4);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile5);
      metaDataRefreshCommitter.consumeManifestFile(m1);
      metaDataRefreshCommitter.commit();

      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly created Manifest is due to rewriting
      // of manifestFile1 file. it is expected to 2 existing file account and 3 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(3, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(2, (int) manifestFile.existingFilesCount());
        }
      }

      UpdatableDatasetConfigFields dataset = client.getCatalogServiceApi()
        .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());

      Assert.assertEquals(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, dataset.getDatasetType());

      BatchSchema newschema = BatchSchema.newBuilder().addFields(schema.getFields())
        .addField(Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true))).build();
      Assert.assertEquals(newschema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

      Assert.assertEquals(tableFolder.toPath().toString(), dataset.getFileFormat().getLocation());
      Assert.assertEquals(FileProtobuf.FileType.PARQUET, dataset.getFileFormat().getType());

      Assert.assertEquals(4, dataset.getReadDefinition().getManifestScanStats().getRecordCount());
      Assert.assertEquals(36, dataset.getReadDefinition().getScanStats().getRecordCount());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testAcrossBatchMetadataRefreshCommitter() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter metaDataRefreshCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(),
        tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1, dataFile2);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile3);
      metaDataRefreshCommitter.consumeManifestFile(m1);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile4);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile5);
      metaDataRefreshCommitter.commit();
      // Sequence of consuming input file is different from testDeleteThenAddMetadataRefreshCommitter
      // This simulates that commit will work across batch

      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly created Manifest is due to rewriting
      // of manifestFile1 file. it is expected to 2 existing file account and 3 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(3, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(2, (int) manifestFile.existingFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testDmlOperation() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter deleteCommitter = icebergModel.getDmlCommitter(
        operatorContext.getStats(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        datasetConfig);

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      String deleteDataFile3 = "books/add3.parquet";
      String deleteDataFile4 = "books/add4.parquet";

      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2, dataFile3);
      deleteCommitter.consumeManifestFile(m1);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile3);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile4);
      deleteCommitter.commit();

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file. This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 1 existing file account and 4 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      Assert.assertEquals(2, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFileDmlDelete")) {
          Assert.assertEquals(3, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(4, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(1, (int) manifestFile.existingFilesCount());
        }
      }
      Assert.assertEquals(4, Iterables.size(table.snapshots()));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testNumberOfSnapshot() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      Table oldTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(oldTable.snapshots()));
      IcebergOpCommitter metaDataRefreshCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(),
        tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile3);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile4);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile5);
      metaDataRefreshCommitter.consumeManifestFile(m1);
      metaDataRefreshCommitter.commit();
      Table table = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(6, Iterables.size(table.snapshots()));
      table.refresh();
      TableOperations tableOperations = ((BaseTable) table).operations();
      metadataFileLocation = tableOperations.current().metadataFileLocation();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      metaDataRefreshCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(),
        tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);
      DataFile dataFile2 = getDatafile("books/add2.parquet");
      ManifestFile m2 = writeManifest(tableFolder, "manifestFile3", dataFile2);
      metaDataRefreshCommitter.consumeManifestFile(m2);
      metaDataRefreshCommitter.commit();
      table = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(8, Iterables.size(table.snapshots()));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testMetadataRefreshSchemaUpdateAndUpPromotion() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      BatchSchema schema1 = new BatchSchema(Arrays.asList(
        INT.toField("field1"),
        INT.toField("field2"),
        BIGINT.toField("field3"),
        INT.toField("field4"),
        BIGINT.toField("field5"),
        FLOAT.toField("field6"),
        DECIMAL.toField("field7"),
        BIT.toField("field8"),
        INT.toField("field9"),
        BIGINT.toField("field10"),
        FLOAT.toField("field11"),
        DOUBLE.toField("field12"),
        DECIMAL.toField("field13"),
        DATE.toField("field14"),
        TIME.toField("field15"),
        TIMESTAMP.toField("field16"),
        INT.toField("field17"),
        BIGINT.toField("field18"),
        FLOAT.toField("field19")
      ));
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema1, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
      datasetPath,
      tableFolder.toPath().toString(), tableName,
      icebergModel.getTableIdentifier(tableFolder.toPath().toString()), schema1,
      Collections.emptyList(),true, datasetConfig);

      BatchSchema schema2 = new BatchSchema(Arrays.asList(
        BIGINT.toField("field1"),
        FLOAT.toField("field2"),
        FLOAT.toField("field3"),
        DOUBLE.toField("field4"),
        DOUBLE.toField("field5"),
        DOUBLE.toField("field6"),
        VARCHAR.toField("field6"),
        DOUBLE.toField("field7"),
        VARCHAR.toField("field8"),
        VARCHAR.toField("field9"),
        VARCHAR.toField("field10"),
        VARCHAR.toField("field11"),
        VARCHAR.toField("field12"),
        VARCHAR.toField("field13"),
        VARCHAR.toField("field14"),
        VARCHAR.toField("field15"),
        VARCHAR.toField("field16"),
        DECIMAL.toField("field17"),
        DECIMAL.toField("field18"),
        DECIMAL.toField("field19")
      ));

      BatchSchema consolidatedSchema = schema1.mergeWithUpPromotion(schema2, this);
      insertTableCommitter.updateSchema(consolidatedSchema);
      insertTableCommitter.commit();

      Table table = getIcebergTable(icebergModel, tableFolder);
      Schema sc = table.schema();
      SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(table.name()).build();
      Assert.assertTrue(consolidatedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testMetadataRefreshSchemaUpdate() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(), tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(),true, datasetConfig);

      BatchSchema newSchema = BatchSchema.of(
        Field.nullablePrimitive("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullablePrimitive("data", new ArrowType.Utf8()),
        Field.nullablePrimitive("boolean", new ArrowType.Bool()),
        Field.nullablePrimitive("stringCol", new ArrowType.Utf8())
      );

      BatchSchema consolidatedSchema = schema.mergeWithUpPromotion(newSchema, this);
      insertTableCommitter.updateSchema(consolidatedSchema);

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1, dataFile2);
      insertTableCommitter.consumeDeleteDataFile(dataFile3);
      insertTableCommitter.consumeManifestFile(m1);
      insertTableCommitter.consumeDeleteDataFile(dataFile4);
      insertTableCommitter.consumeDeleteDataFile(dataFile5);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();
      SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(consolidatedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
      Assert.assertEquals(6, Iterables.size(newTable.snapshots()));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testMetadataRefreshSchemaDropColumns() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
              datasetPath,
              tableFolder.toPath().toString(), tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(), false, datasetConfig);

      BatchSchema newSchema = BatchSchema.of(
              Field.nullablePrimitive("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("boolean", new ArrowType.Bool()),
              Field.nullablePrimitive("stringCol", new ArrowType.Utf8())
      );

      insertTableCommitter.updateSchema(newSchema);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();
      SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(newSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testMetadataRefreshDelete() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(), tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);

      DataFile dataFile1 = getDatafileWithPartitionSpec("books/add4.parquet");
      DataFile dataFile2 = getDatafileWithPartitionSpec("books/add5.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1, dataFile2);
      insertTableCommitter.consumeManifestFile(m1);

      DataFile dataFile1Delete = getDatafile("books/add4.parquet");
      DataFile dataFile2Delete = getDatafile("books/add4.parquet");

      insertTableCommitter.consumeDeleteDataFile(dataFile1);
      insertTableCommitter.consumeManifestFile(m1);
      insertTableCommitter.consumeDeleteDataFile(dataFile1Delete);
      insertTableCommitter.consumeDeleteDataFile(dataFile2Delete);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();

      Assert.assertEquals(6, Iterables.size(newTable.snapshots()));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentIncrementalMetadataRefresh() throws Exception {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      // Two concurrent iceberg committeres
      IcebergOpCommitter insertTableCommitter1 = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableName,
        tableFolder.toPath().toString(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);
      IcebergOpCommitter insertTableCommitter2 = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableName,
        tableFolder.toPath().toString(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), true, datasetConfig);


      DataFile dataFile6 = getDatafile("books/add1.parquet");
      DataFile dataFile7 = getDatafile("books/add2.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile6, dataFile7);

      insertTableCommitter1.consumeManifestFile(m1);
      insertTableCommitter2.consumeManifestFile(m1);

      UserExceptionAssert.assertThatThrownBy(() -> {
          insertTableCommitter1.commit();
          insertTableCommitter2.commit();
        })
        .hasErrorType(CONCURRENT_MODIFICATION)
        .hasMessageContaining("Concurrent DML operation has updated the table, please retry.");

      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      Assert.assertEquals(2, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }

  }

  @Test
  public void testIncrementalRefreshDroppedAndAddedColumns() throws Exception {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      List<Field> childrenField1 = ImmutableList.of(VARCHAR.toField("doubleCol"));

      Field structField1 = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField1);

      BatchSchema schema = BatchSchema.of(
        Field.nullablePrimitive("id", new ArrowType.Int(64, true)),
        Field.nullablePrimitive("data", new ArrowType.Utf8()),
        Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
        Field.nullablePrimitive("intField", new ArrowType.Utf8()),
        structField1);

      List<Field> childrenField2 = ImmutableList.of(CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"));

      Field structField2 = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField2);

      BatchSchema newSchema = BatchSchema.of(
        Field.nullablePrimitive("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullablePrimitive("data", new ArrowType.Utf8()),
        Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
        Field.nullablePrimitive("intField", new ArrowType.Int(32, false)),
        Field.nullablePrimitive("floatField",new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        structField2
      );

      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      datasetConfig.setRecordSchema(schema.toByteString());

      BatchSchema droppedColumns = BatchSchema.of(
        Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
        new Field("structField", FieldType.nullable(STRUCT.getType()), ImmutableList.of(CompleteType.INT.toField("integerCol")))
      );

      BatchSchema updatedColumns = BatchSchema.of(
        Field.nullablePrimitive("intField", new ArrowType.Utf8()),
        new Field("structField", FieldType.nullable(STRUCT.getType()), ImmutableList.of(VARCHAR.toField("doubleCol")))
      );

     datasetConfig.getPhysicalDataset().getInternalSchemaSettings().setDroppedColumns(droppedColumns.toByteString());
     datasetConfig.getPhysicalDataset().getInternalSchemaSettings().setModifiedColumns(updatedColumns.toByteString());

      IcebergOpCommitter insertTableCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
        datasetPath,
        tableFolder.toPath().toString(), tableName,
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList(), false, datasetConfig);

      Field newStructField = new Field("structField", FieldType.nullable(STRUCT.getType()), ImmutableList.of(VARCHAR.toField("doubleCol")));

      BatchSchema expectedSchema = BatchSchema.of(
        Field.nullablePrimitive("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullablePrimitive("data", new ArrowType.Utf8()),
        Field.nullablePrimitive("intField", new ArrowType.Utf8()),
        Field.nullablePrimitive("floatField",new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        newStructField
      );

      insertTableCommitter.updateSchema(newSchema);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();
      SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(expectedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  ManifestFile writeManifest(File tableFolder, String fileName, DataFile... files) throws IOException {
    return writeManifest(tableFolder, fileName, null, files);
  }

  ManifestFile writeManifest(File tableFolder, String fileName, Long snapshotId, DataFile... files) throws IOException {
    File manifestFile = new File(folder, fileName + ".avro");
    Table table = getIcebergTable(icebergModel, tableFolder);
    OutputFile outputFile = table.io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(1, table.spec(), outputFile, snapshotId);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private DataFile getDatafile(String path) {
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath(path)
      .withFileSizeInBytes(40)
      .withRecordCount(9)
      .build();
    return dataFile;
  }

  private DataFile getDatafileWithPartitionSpec(String path) {
    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    PartitionSpec spec_id_and_data_column = PartitionSpec.builderFor(schemaConverter.toIcebergSchema(schema))
      .identity("id")
      .build();

    DataFile dataFile = DataFiles.builder(spec_id_and_data_column)
      .withPath(path)
      .withFileSizeInBytes(40)
      .withRecordCount(9)
      .build();
    return dataFile;
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

  @Test
  public void testConcurrentIncrementalRefresh() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter commiter1 = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(), true, datasetConfig);
      Assert.assertTrue(commiter1 instanceof IncrementalMetadataRefreshCommitter);

      IcebergOpCommitter commiter2 = icebergModel.getIncrementalMetadataRefreshCommitter(operatorContext, tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(), true, datasetConfig);
      Assert.assertTrue(commiter2 instanceof IncrementalMetadataRefreshCommitter);

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile1, dataFile2);
      commiter1.consumeDeleteDataFile(dataFile3);
      commiter1.consumeDeleteDataFile(dataFile4);
      commiter1.consumeDeleteDataFile(dataFile5);
      commiter1.consumeManifestFile(m1);

      commiter2.consumeDeleteDataFile(dataFile3);
      commiter2.consumeDeleteDataFile(dataFile4);
      commiter2.consumeDeleteDataFile(dataFile5);
      commiter2.consumeManifestFile(m1);

      // start both commits
      ((IncrementalMetadataRefreshCommitter) commiter1).beginMetadataRefreshTransaction();
      ((IncrementalMetadataRefreshCommitter) commiter2).beginMetadataRefreshTransaction();

      // end commit 1
      ((IncrementalMetadataRefreshCommitter) commiter1).performUpdates();
      ((IncrementalMetadataRefreshCommitter) commiter1).endMetadataRefreshTransaction();

      // end commit 2 should fail with CommitFailedException
      try {
        ((IncrementalMetadataRefreshCommitter) commiter2).performUpdates();
        ((IncrementalMetadataRefreshCommitter) commiter2).endMetadataRefreshTransaction();
        Assert.fail();
      } catch (ValidationException ve) {
        // ignore
      }

      ((IncrementalMetadataRefreshCommitter) commiter1).postCommitTransaction();
      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly created Manifest is due to rewriting
      // of manifestFile1 file. it is expected to 2 existing file account and 3 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(3, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(2, (int) manifestFile.existingFilesCount());
        }
      }

      UpdatableDatasetConfigFields dataset = client.getCatalogServiceApi()
              .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());

      Assert.assertEquals(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, dataset.getDatasetType());

      BatchSchema newschema = BatchSchema.newBuilder().addFields(schema.getFields())
        .addField(Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true))).build();
      Assert.assertEquals(newschema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

      Assert.assertEquals(tableFolder.toPath().toString(), dataset.getFileFormat().getLocation());
      Assert.assertEquals(FileProtobuf.FileType.PARQUET, dataset.getFileFormat().getType());

      Assert.assertEquals(4, dataset.getReadDefinition().getManifestScanStats().getRecordCount());
      Assert.assertEquals(36, dataset.getReadDefinition().getScanStats().getRecordCount());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentTwoDmlOperations() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergOpCommitter committer1 = icebergModel.getDmlCommitter(
        operatorContext.getStats(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        datasetConfig);
      Assert.assertTrue(committer1 instanceof IcebergDmlOperationCommitter);

      IcebergOpCommitter committer2 = icebergModel.getDmlCommitter(
        operatorContext.getStats(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        datasetConfig);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2);
      committer1.consumeManifestFile(m1);
      committer1.consumeDeleteDataFilePath(deleteDataFile1);
      committer1.consumeDeleteDataFilePath(deleteDataFile2);

      committer2.consumeManifestFile(m1);
      committer2.consumeDeleteDataFilePath(deleteDataFile1);
      committer2.consumeDeleteDataFilePath(deleteDataFile2);

      // start both commits
      ((IcebergDmlOperationCommitter) committer1).beginDmlOperationTransaction();
      ((IcebergDmlOperationCommitter) committer2).beginDmlOperationTransaction();

      // end commit 1
      ((IcebergDmlOperationCommitter) committer1).performUpdates();
      ((IcebergDmlOperationCommitter) committer1).endDmlOperationTransaction();

      // end commit 2 should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(() -> {
            ((IcebergDmlOperationCommitter) committer2).performUpdates();
            ((IcebergDmlOperationCommitter) committer2).endDmlOperationTransaction();
          }
        ).hasErrorType(CONCURRENT_MODIFICATION)
        .hasMessageContaining("Concurrent DML operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file. This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 3 existing file account and 2 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      Assert.assertEquals(2, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFileDmlDelete")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(2, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(3, (int) manifestFile.existingFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testDmlCommittedSnapshotNumber() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      final int countBeforeDmlCommit = Iterables.size(tableBefore.snapshots());

      IcebergOpCommitter committer = icebergModel.getDmlCommitter(
        operatorContext.getStats(),
        icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
        datasetConfig);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2);
      dmlCommitter.consumeManifestFile(m1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      dmlCommitter.beginDmlOperationTransaction();
      dmlCommitter.performUpdates();
      Table tableAfter = dmlCommitter.endDmlOperationTransaction();
      int countAfterDmlCommit = Iterables.size(tableAfter.snapshots());
      Assert.assertEquals("Expect to increase 1 snapshot", 1, countAfterDmlCommit - countBeforeDmlCommit);
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  private static String getManifestCrcFileName(String manifestFilePath) {
    com.dremio.io.file.Path p = com.dremio.io.file.Path.of(manifestFilePath);
    String fileName = p.getName();
    com.dremio.io.file.Path parentPath = p.getParent();
    return parentPath + com.dremio.io.file.Path.SEPARATOR + "." + fileName + ".crc";
  }

  @Test
  public void testDeleteManifestFiles() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);

    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      String dataFile1Name = "books/add1.parquet";
      String dataFile2Name = "books/add2.parquet";

      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1 = getDatafile(dataFile1Name);
      DataFile dataFile2 = getDatafile(dataFile2Name);

      ManifestFile m = writeManifest(tableFolder, "manifestFileDml", dataFile1, dataFile2);
      Table table = getIcebergTable(icebergModel, tableFolder);
      InputFile inputFile = table.io().newInputFile(m.path(), m.length());
      DremioFileIO dremioFileIO = Mockito.mock(DremioFileIO.class);
      Set<String> actualDeletedFiles = new HashSet<>();

      when(dremioFileIO.newInputFile(m.path(), m.length())).thenReturn(inputFile);
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          Assert.assertEquals("one file path arg is expected", args.length, 1);
          actualDeletedFiles.add((String)args[0]);
          return null;
        }
      }).when(dremioFileIO).deleteFile(anyString());

      // scenario 1: delete both manifest file and data files
      IcebergCommitOpHelper.deleteManifestFiles(dremioFileIO, ImmutableList.of(m), true);
      Set<String> expectedDeletedFilesIncludeDataFiles = ImmutableSet.of(
        dataFile1Name, dataFile2Name,
        m.path(), getManifestCrcFileName(m.path()));
      Assert.assertEquals(expectedDeletedFilesIncludeDataFiles, actualDeletedFiles);

      // scenario 2: delete manifest file only
      actualDeletedFiles.clear();
      IcebergCommitOpHelper.deleteManifestFiles(dremioFileIO, ImmutableList.of(m), false);
      expectedDeletedFilesIncludeDataFiles = ImmutableSet.of(m.path(), getManifestCrcFileName(m.path()));
      Assert.assertEquals(expectedDeletedFilesIncludeDataFiles, actualDeletedFiles);
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }
}
