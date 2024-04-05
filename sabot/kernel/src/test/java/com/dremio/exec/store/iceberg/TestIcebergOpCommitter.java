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
import static com.dremio.exec.store.iceberg.model.IcebergCommandType.INCREMENTAL_METADATA_REFRESH;
import static com.dremio.exec.store.iceberg.model.IcebergCommandType.PARTIAL_METADATA_REFRESH;
import static com.dremio.exec.store.iceberg.model.IncrementalMetadataRefreshCommitter.MAX_NUM_SNAPSHOTS_TO_EXPIRE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergDmlOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IncrementalMetadataRefreshCommitter;
import com.dremio.io.file.Path;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestIcebergOpCommitter extends TestIcebergCommitterBase {

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
      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableName,
              tableFolder.toPath().toString(),
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
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
      IcebergOpCommitter metaDataRefreshCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

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
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly
      // created Manifest is due to rewriting
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

      UpdatableDatasetConfigFields dataset =
          client
              .getCatalogServiceApi()
              .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());

      Assert.assertEquals(
          DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER,
          dataset.getDatasetType());

      BatchSchema newschema =
          BatchSchema.newBuilder()
              .addFields(schema.getFields())
              .addField(
                  Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true)))
              .build();
      Assert.assertEquals(
          newschema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

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
      IcebergOpCommitter metaDataRefreshCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

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
      // Sequence of consuming input file is different from
      // testDeleteThenAddMetadataRefreshCommitter
      // This simulates that commit will work across batch

      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly
      // created Manifest is due to rewriting
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

      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergOpCommitter deleteCommitter =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      String deleteDataFile3 = "books/add3.parquet";
      String deleteDataFile4 = "books/add4.parquet";

      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2, dataFile3);
      deleteCommitter.consumeManifestFile(m1);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile3);
      deleteCommitter.consumeDeleteDataFilePath(deleteDataFile4);
      deleteCommitter.commit();

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file.
      // This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 1 existing file account and
      // 4 deleted file count.
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
      IcebergOpCommitter metaDataRefreshCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

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

      metaDataRefreshCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
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
  public void testIncrementalRefreshExpireSnapshots() throws Exception {
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

      // Increase more snapshots
      long startTimestampExpiry = System.currentTimeMillis();
      for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 5; j++) {
          IcebergOpCommitter metaDataRefreshCommitter =
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  null,
                  INCREMENTAL_METADATA_REFRESH,
                  null);

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
          table.refresh();
          TableOperations tableOperations = ((BaseTable) table).operations();
          metadataFileLocation = tableOperations.current().metadataFileLocation();
          icebergMetadata.setMetadataFileLocation(metadataFileLocation);
          datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
        }

        if (i == 1) {
          startTimestampExpiry = System.currentTimeMillis();
        }
      }
      Table table = getIcebergTable(icebergModel, tableFolder);
      table.refresh();
      final int numTotalSnapshots = Iterables.size(table.snapshots());
      Assert.assertEquals(63, numTotalSnapshots);

      Thread.sleep(100);

      IncrementalMetadataRefreshCommitter refreshCommitter =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  null,
                  INCREMENTAL_METADATA_REFRESH,
                  null);

      Pair<Set<String>, Long> entry =
          refreshCommitter.cleanSnapshotsAndMetadataFiles(table, startTimestampExpiry);
      Set<String> orphanFiles = entry.first();
      Long numValidSnapshots = entry.second();
      // For one commit, we don't expire all old snapshots. Instead, we expire a limit number of
      // them.
      table = getIcebergTable(icebergModel, tableFolder);
      table.refresh();
      final int numSnapshotsAfterExpiry = Iterables.size(table.snapshots());
      Assert.assertEquals(48, numSnapshotsAfterExpiry);
      final int numExpiredSnapshots = numTotalSnapshots - numSnapshotsAfterExpiry;
      Assert.assertEquals(MAX_NUM_SNAPSHOTS_TO_EXPIRE, numExpiredSnapshots);
      Assert.assertEquals(19, orphanFiles.size());
      // Only need to loop through valid snapshots (not all remaining snapshots) to determine final
      // orphan files.
      Assert.assertEquals(30, numValidSnapshots.intValue());
      for (String filePath : orphanFiles) {
        // Should not collect any data/parquet file as orphan files.
        String fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);
        Assert.assertFalse("parquet".equalsIgnoreCase(fileExtension));

        // Orphan files should be deleted.
        Assert.assertFalse(localFs.exists(Path.of(filePath)));
      }

      TableOperations tableOperations = ((BaseTable) table).operations();
      metadataFileLocation = tableOperations.current().metadataFileLocation();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      icebergMetadata.setSnapshotId(tableOperations.current().currentSnapshot().snapshotId());
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IncrementalMetadataRefreshCommitter refreshCommitter2 =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  100L,
                  INCREMENTAL_METADATA_REFRESH,
                  null);
      // Do empty commit. Still clean table's snapshots
      refreshCommitter2.disableUseDefaultPeriod();
      refreshCommitter2.commit();
      table = getIcebergTable(icebergModel, tableFolder);
      table.refresh();
      final int numSnapshotsAfterSecondExpiry = Iterables.size(table.snapshots());
      Assert.assertEquals(33, numSnapshotsAfterSecondExpiry);

      // Table properties should be updated to delete metadata entry file.
      Map<String, String> tblProperties = table.properties();
      Assert.assertTrue(
          tblProperties.containsKey(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED));
      Assert.assertTrue(
          tblProperties
              .get(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED)
              .equalsIgnoreCase("true"));
      Assert.assertTrue(tblProperties.containsKey(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX));
      Assert.assertTrue(
          tblProperties
              .get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX)
              .equalsIgnoreCase("100"));
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
      BatchSchema schema1 =
          new BatchSchema(
              Arrays.asList(
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
                  FLOAT.toField("field19")));
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema1, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema1,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      BatchSchema schema2 =
          new BatchSchema(
              Arrays.asList(
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
                  DECIMAL.toField("field19")));

      BatchSchema consolidatedSchema = schema1.mergeWithUpPromotion(schema2, this);
      insertTableCommitter.updateSchema(consolidatedSchema);
      insertTableCommitter.commit();

      Table table = getIcebergTable(icebergModel, tableFolder);
      Schema sc = table.schema();
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(table.name()).build();
      Assert.assertTrue(
          consolidatedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
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
      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      BatchSchema newSchema =
          BatchSchema.of(
              Field.nullablePrimitive(
                  "id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("data", new ArrowType.Utf8()),
              Field.nullablePrimitive("boolean", new ArrowType.Bool()),
              Field.nullablePrimitive("stringCol", new ArrowType.Utf8()));

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
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(
          consolidatedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
      Assert.assertEquals(6, Iterables.size(newTable.snapshots()));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testMetadataRefreshColumnNameIncludeDot() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      BatchSchema newSchema =
          BatchSchema.of(
              Field.nullablePrimitive(
                  "id.A", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("data.A", new ArrowType.Utf8()),
              Field.nullablePrimitive("boolean.A", new ArrowType.Bool()),
              Field.nullablePrimitive("stringCol.A", new ArrowType.Utf8()));

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
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(
          consolidatedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
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
      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              false,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      BatchSchema newSchema =
          BatchSchema.of(
              Field.nullablePrimitive(
                  "id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("boolean", new ArrowType.Bool()),
              Field.nullablePrimitive("stringCol", new ArrowType.Utf8()));

      insertTableCommitter.updateSchema(newSchema);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(newTable.name()).build();
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

      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
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
      IcebergOpCommitter firstCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableName,
              tableFolder.toPath().toString(),
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      IcebergOpCommitter secondCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableName,
              tableFolder.toPath().toString(),
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      DataFile dataFile6 = getDatafile("books/add1.parquet");
      DataFile dataFile7 = getDatafile("books/add2.parquet");
      ManifestFile m1 = writeManifest(tableFolder, "manifestFile2", dataFile6, dataFile7);

      firstCommitter.consumeManifestFile(m1);
      secondCommitter.consumeManifestFile(m1);

      // The first commit succeeds.
      firstCommitter.commit();
      Table firstCommitTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          firstCommitTable.currentSnapshot().allManifests(firstCommitTable.io());
      Assert.assertEquals(2, manifestFileList.size());

      // Due to concurrent operation, the second commit should not make any update to table. The
      // commit should be omitted.
      Snapshot snapshot = secondCommitter.commit();
      Assert.assertNull(snapshot);
      Assert.assertTrue(secondCommitter.isIcebergTableUpdated());
      Table secondCommitTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(
          firstCommitTable.currentSnapshot().snapshotId(),
          secondCommitTable.currentSnapshot().snapshotId());
      Assert.assertEquals(
          manifestFileList.size(),
          secondCommitTable.currentSnapshot().allManifests(secondCommitTable.io()).size());
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

      Field structField1 =
          new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField1);

      BatchSchema schema =
          BatchSchema.of(
              Field.nullablePrimitive("id", new ArrowType.Int(64, true)),
              Field.nullablePrimitive("data", new ArrowType.Utf8()),
              Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
              Field.nullablePrimitive("intField", new ArrowType.Utf8()),
              structField1);

      List<Field> childrenField2 =
          ImmutableList.of(
              CompleteType.INT.toField("integerCol"), CompleteType.DOUBLE.toField("doubleCol"));

      Field structField2 =
          new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField2);

      BatchSchema newSchema =
          BatchSchema.of(
              Field.nullablePrimitive(
                  "id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("data", new ArrowType.Utf8()),
              Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
              Field.nullablePrimitive("intField", new ArrowType.Int(32, false)),
              Field.nullablePrimitive(
                  "floatField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              structField2);

      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      datasetConfig.setRecordSchema(schema.toByteString());

      BatchSchema droppedColumns =
          BatchSchema.of(
              Field.nullablePrimitive("stringField", new ArrowType.Utf8()),
              new Field(
                  "structField",
                  FieldType.nullable(STRUCT.getType()),
                  ImmutableList.of(CompleteType.INT.toField("integerCol"))));

      BatchSchema updatedColumns =
          BatchSchema.of(
              Field.nullablePrimitive("intField", new ArrowType.Utf8()),
              new Field(
                  "structField",
                  FieldType.nullable(STRUCT.getType()),
                  ImmutableList.of(VARCHAR.toField("doubleCol"))));

      datasetConfig
          .getPhysicalDataset()
          .getInternalSchemaSettings()
          .setDroppedColumns(droppedColumns.toByteString());
      datasetConfig
          .getPhysicalDataset()
          .getInternalSchemaSettings()
          .setModifiedColumns(updatedColumns.toByteString());

      IcebergOpCommitter insertTableCommitter =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              false,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);

      Field newStructField =
          new Field(
              "structField",
              FieldType.nullable(STRUCT.getType()),
              ImmutableList.of(VARCHAR.toField("doubleCol")));

      BatchSchema expectedSchema =
          BatchSchema.of(
              Field.nullablePrimitive(
                  "id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullablePrimitive("data", new ArrowType.Utf8()),
              Field.nullablePrimitive("intField", new ArrowType.Utf8()),
              Field.nullablePrimitive(
                  "floatField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              newStructField);

      insertTableCommitter.updateSchema(newSchema);
      insertTableCommitter.commit();

      Table newTable = getIcebergTable(icebergModel, tableFolder);
      Schema sc = newTable.schema();
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(newTable.name()).build();
      Assert.assertTrue(
          expectedSchema.equalsTypesWithoutPositions(schemaConverter.fromIceberg(sc)));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentIncrementalPartialRefresh() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter commiter1 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter1 instanceof IncrementalMetadataRefreshCommitter);

      IcebergOpCommitter commiter2 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              PARTIAL_METADATA_REFRESH,
              null);
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

      // end commit 2 should fail with CONCURRENT_MODIFICATION error
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                ((IncrementalMetadataRefreshCommitter) commiter2).performUpdates();
                ((IncrementalMetadataRefreshCommitter) commiter2).endMetadataRefreshTransaction();
                Assert.fail();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining(
              "Unable to refresh metadata for the dataset (due to concurrent updates). Please retry");

      ((IncrementalMetadataRefreshCommitter) commiter1).postCommitTransaction();
      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly
      // created Manifest is due to rewriting
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

      UpdatableDatasetConfigFields dataset =
          client
              .getCatalogServiceApi()
              .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());

      Assert.assertEquals(
          DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER,
          dataset.getDatasetType());

      BatchSchema newschema =
          BatchSchema.newBuilder()
              .addFields(schema.getFields())
              .addField(
                  Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true)))
              .build();
      Assert.assertEquals(
          newschema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

      Assert.assertEquals(tableFolder.toPath().toString(), dataset.getFileFormat().getLocation());
      Assert.assertEquals(FileProtobuf.FileType.PARQUET, dataset.getFileFormat().getType());

      Assert.assertEquals(4, dataset.getReadDefinition().getManifestScanStats().getRecordCount());
      Assert.assertEquals(36, dataset.getReadDefinition().getScanStats().getRecordCount());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentTwoIncrementalRefresh() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter commiter1 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter1 instanceof IncrementalMetadataRefreshCommitter);

      IcebergOpCommitter commiter2 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
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

      // Two INCREMENTAL_METADATA_REFRESH commits, skip the second without throwing exception.

      // start both commits
      ((IncrementalMetadataRefreshCommitter) commiter1).beginMetadataRefreshTransaction();
      ((IncrementalMetadataRefreshCommitter) commiter2).beginMetadataRefreshTransaction();

      // end commit 1
      ((IncrementalMetadataRefreshCommitter) commiter1).performUpdates();
      Snapshot snapshot1 =
          ((IncrementalMetadataRefreshCommitter) commiter1).endMetadataRefreshTransaction();
      Assert.assertNotNull(snapshot1);

      // end commit 2 should not fail. Basically, skip second commit.
      ((IncrementalMetadataRefreshCommitter) commiter2).performUpdates();
      Snapshot snapshot2 =
          ((IncrementalMetadataRefreshCommitter) commiter2).endMetadataRefreshTransaction();
      Assert.assertNull(snapshot2);

      ((IncrementalMetadataRefreshCommitter) commiter1).postCommitTransaction();
      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly
      // created Manifest is due to rewriting
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

      UpdatableDatasetConfigFields dataset =
          client
              .getCatalogServiceApi()
              .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());

      Assert.assertEquals(
          DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER,
          dataset.getDatasetType());

      BatchSchema newschema =
          BatchSchema.newBuilder()
              .addFields(schema.getFields())
              .addField(
                  Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true)))
              .build();
      Assert.assertEquals(
          newschema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

      Assert.assertEquals(tableFolder.toPath().toString(), dataset.getFileFormat().getLocation());
      Assert.assertEquals(FileProtobuf.FileType.PARQUET, dataset.getFileFormat().getType());

      Assert.assertEquals(4, dataset.getReadDefinition().getManifestScanStats().getRecordCount());
      Assert.assertEquals(36, dataset.getReadDefinition().getScanStats().getRecordCount());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentIncrementalPartialRefreshPostCommit() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter commiter1 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter1 instanceof IncrementalMetadataRefreshCommitter);

      IcebergOpCommitter commiter2 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              PARTIAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter2 instanceof IncrementalMetadataRefreshCommitter);

      Table table = getIcebergTable(icebergModel, tableFolder);

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

      // One INCREMENTAL_METADATA_REFRESH commit and one PARTIAL_METADATA_REFRESH, skip the second
      // without throwing exception.

      // start both commits
      ((IncrementalMetadataRefreshCommitter) commiter1).beginMetadataRefreshTransaction();
      ((IncrementalMetadataRefreshCommitter) commiter2).beginMetadataRefreshTransaction();

      // end commit 1
      ((IncrementalMetadataRefreshCommitter) commiter1).performUpdates();
      Snapshot snapshot1 =
          ((IncrementalMetadataRefreshCommitter) commiter1).endMetadataRefreshTransaction();
      Snapshot snapshot2 =
          ((IncrementalMetadataRefreshCommitter) commiter1).postCommitTransaction();
      Assert.assertNotNull(snapshot1);
      Assert.assertNotNull(snapshot2);

      // end commit 2 should fail with CONCURRENT_MODIFICATION error
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                ((IncrementalMetadataRefreshCommitter) commiter2).performUpdates();
                ((IncrementalMetadataRefreshCommitter) commiter2).endMetadataRefreshTransaction();
                Assert.fail();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining(
              "Unable to refresh metadata for the dataset (due to concurrent updates). Please retry");

      // Set the Iceberg table instance to test postCommitTransaction.
      // For PARTIAL_METADATA_REFRESH command, we throw CME as the post commit fails due to
      // StatusRuntimeException (ABORTED)
      ((IncrementalMetadataRefreshCommitter) commiter2).setTable(table);
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                ((IncrementalMetadataRefreshCommitter) commiter2).postCommitTransaction();
                Assert.fail();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining(
              "Unable to refresh metadata for the dataset (due to concurrent updates). Please retry");
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentTwoIncrementalRefreshPostCommit() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IcebergOpCommitter commiter1 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter1 instanceof IncrementalMetadataRefreshCommitter);

      IcebergOpCommitter commiter2 =
          icebergModel.getIncrementalMetadataRefreshCommitter(
              operatorContext,
              tableName,
              datasetPath,
              tableFolder.toPath().toString(),
              tableName,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              schema,
              Collections.emptyList(),
              true,
              datasetConfig,
              localFs,
              null,
              INCREMENTAL_METADATA_REFRESH,
              null);
      Assert.assertTrue(commiter2 instanceof IncrementalMetadataRefreshCommitter);

      Table table = getIcebergTable(icebergModel, tableFolder);

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

      // Two INCREMENTAL_METADATA_REFRESH commits, skip the second without throwing exception.

      // start both commits
      ((IncrementalMetadataRefreshCommitter) commiter1).beginMetadataRefreshTransaction();
      ((IncrementalMetadataRefreshCommitter) commiter2).beginMetadataRefreshTransaction();

      // end commit 1
      ((IncrementalMetadataRefreshCommitter) commiter1).performUpdates();
      Snapshot snapshot1 =
          ((IncrementalMetadataRefreshCommitter) commiter1).endMetadataRefreshTransaction();
      Snapshot snapshot2 =
          ((IncrementalMetadataRefreshCommitter) commiter1).postCommitTransaction();
      Assert.assertNotNull(snapshot1);
      Assert.assertNotNull(snapshot2);

      // end commit 2 should not fail. Basically, skip second commit.
      ((IncrementalMetadataRefreshCommitter) commiter2).performUpdates();
      Snapshot snapshot3 =
          ((IncrementalMetadataRefreshCommitter) commiter2).endMetadataRefreshTransaction();
      Assert.assertNull(snapshot3);

      // Set the Iceberg table instance to test postCommitTransaction.
      // For INCREMENTAL_METADATA_REFRESH command, we skip to throw CME, even the post commit fails
      // with StatusRuntimeException.
      ((IncrementalMetadataRefreshCommitter) commiter2).setTable(table);
      Snapshot snapshot4 =
          ((IncrementalMetadataRefreshCommitter) commiter2).postCommitTransaction();
      Assert.assertEquals(table.currentSnapshot().snapshotId(), snapshot4.snapshotId());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentCommitsFailToUpdateKVStore() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IncrementalMetadataRefreshCommitter incrementalCommitter =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  null,
                  INCREMENTAL_METADATA_REFRESH,
                  null);

      long startingSnapshotId =
          getIcebergTable(icebergModel, tableFolder).currentSnapshot().snapshotId();

      DataFile dataFile6 = getDatafile("books/add6.parquet");
      DataFile dataFile7 = getDatafile("books/add7.parquet");
      ManifestFile manifestFile2 = writeManifest(tableFolder, "manifestFile2", dataFile6);
      ManifestFile manifestFile3 = writeManifest(tableFolder, "manifestFile3", dataFile7);

      incrementalCommitter.consumeManifestFile(manifestFile2);

      // Start incremental commit into Iceberg and update table's snapshot and metadata file
      incrementalCommitter.beginMetadataRefreshTransaction();
      incrementalCommitter.performUpdates();
      incrementalCommitter.endMetadataRefreshTransaction();
      long secondSnapshotId =
          getIcebergTable(icebergModel, tableFolder).currentSnapshot().snapshotId();
      Assert.assertNotEquals(
          "Iceberg table should be updated", startingSnapshotId, secondSnapshotId);

      // Before incremental commit updates KV store, start the partial commit process. In this case,
      // we can build a new
      // snapshot upon incremental commit.
      IncrementalMetadataRefreshCommitter partialCommitter =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  null,
                  PARTIAL_METADATA_REFRESH,
                  null);

      String incrementalCommitTag =
          incrementalCommitter
              .getDatasetCatalogRequestBuilder()
              .build()
              .getDatasetConfig()
              .getTag();
      String partialCommitTag =
          partialCommitter.getDatasetCatalogRequestBuilder().build().getDatasetConfig().getTag();
      Assert.assertEquals("Tags should not change", incrementalCommitTag, partialCommitTag);

      partialCommitter.consumeManifestFile(manifestFile3);
      partialCommitter.beginMetadataRefreshTransaction();

      // Update Incremental commit into KV store and update dataset's tag info.
      incrementalCommitter.postCommitTransaction();
      String updatedTag = getTag(datasetPath);
      Assert.assertNotEquals("Tag should be updated", updatedTag, partialCommitTag);

      // Make another Iceberg commit and update KV store. The KV store update should fail due to tag
      // mismatch.
      partialCommitter.performUpdates();
      partialCommitter.endMetadataRefreshTransaction();
      long thirdSnapshotId =
          getIcebergTable(icebergModel, tableFolder).currentSnapshot().snapshotId();
      Assert.assertNotEquals("Iceberg table should be updated", secondSnapshotId, thirdSnapshotId);

      // For PARTIAL_METADATA_REFRESH command, it throws CME as the post commit fails due to
      // StatusRuntimeException (ABORTED)
      // It does not update the KV store successfully. But, it needs the manifest files not deleted.
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                partialCommitter.postCommitTransaction();
                Assert.fail();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining(
              "Unable to refresh metadata for the dataset (due to concurrent updates). Please retry");

      // Manifest files are not deleted.
      Assert.assertTrue(localFs.exists(Path.of(manifestFile2.path())));
      Assert.assertTrue(localFs.exists(Path.of(manifestFile3.path())));
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testIncrementalRefreshMissingManifestListFile() throws Exception {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IncrementalMetadataRefreshCommitter incrementalCommitter =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  null,
                  INCREMENTAL_METADATA_REFRESH,
                  null);

      // Check the manifest list file we plan to delete does exist now.
      String manifestListToDelete =
          getIcebergTable(icebergModel, tableFolder).currentSnapshot().manifestListLocation();
      Assert.assertTrue(
          "Manifest list file should exist", localFs.exists(Path.of(manifestListToDelete)));

      DataFile dataFile6 = getDatafile("books/add6.parquet");
      DataFile dataFile7 = getDatafile("books/add7.parquet");
      ManifestFile manifestFile2 = writeManifest(tableFolder, "manifestFile2", dataFile6);
      ManifestFile manifestFile3 = writeManifest(tableFolder, "manifestFile3", dataFile7);

      // Make a refresh and increase a snapshot.
      incrementalCommitter.consumeManifestFile(manifestFile2);
      incrementalCommitter.commit();

      // Delete the intermediate snapshot's manifest list file.
      localFs.delete(Path.of(manifestListToDelete), false);
      Assert.assertFalse(
          "Manifest list file should be deleted", localFs.exists(Path.of(manifestListToDelete)));

      // Make another commit at the situation that a manifest list file missed. The commit should
      // succeed.

      // Set new Iceberg metadata file location.
      icebergMetadata.setMetadataFileLocation(incrementalCommitter.getRootPointer());
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      IncrementalMetadataRefreshCommitter partialCommitter =
          (IncrementalMetadataRefreshCommitter)
              icebergModel.getIncrementalMetadataRefreshCommitter(
                  operatorContext,
                  tableName,
                  datasetPath,
                  tableFolder.toPath().toString(),
                  tableName,
                  icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
                  schema,
                  Collections.emptyList(),
                  true,
                  datasetConfig,
                  localFs,
                  1000L,
                  PARTIAL_METADATA_REFRESH,
                  null);

      partialCommitter.consumeManifestFile(manifestFile3);
      partialCommitter.disableUseDefaultPeriod();
      partialCommitter.setMinSnapshotsToKeep(1);
      Assert.assertNotNull(partialCommitter.commit());
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

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
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
      Assert.assertEquals(
          "Expect to increase 1 snapshot", 1, countAfterDmlCommit - countBeforeDmlCommit);
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
      doAnswer(
              new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) {
                  Object[] args = invocation.getArguments();
                  Assert.assertEquals("one file path arg is expected", args.length, 1);
                  actualDeletedFiles.add((String) args[0]);
                  return null;
                }
              })
          .when(dremioFileIO)
          .deleteFile(anyString());

      // scenario 1: delete both manifest file and data files
      IcebergCommitOpHelper.deleteManifestFiles(dremioFileIO, ImmutableList.of(m), true);
      Set<String> expectedDeletedFilesIncludeDataFiles =
          ImmutableSet.of(dataFile1Name, dataFile2Name, m.path(), getManifestCrcFileName(m.path()));
      Assert.assertEquals(expectedDeletedFilesIncludeDataFiles, actualDeletedFiles);

      // scenario 2: delete manifest file only
      actualDeletedFiles.clear();
      IcebergCommitOpHelper.deleteManifestFiles(dremioFileIO, ImmutableList.of(m), false);
      expectedDeletedFilesIncludeDataFiles =
          ImmutableSet.of(m.path(), getManifestCrcFileName(m.path()));
      Assert.assertEquals(expectedDeletedFilesIncludeDataFiles, actualDeletedFiles);
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }
}
