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

import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION;

import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergDmlOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergInsertOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergOptimizeOperationCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergCommitConcurrency extends TestIcebergCommitterBase {

  @Test
  public void testConcurrentDmlsFail() throws IOException {
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
      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter2 = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2);
      dmlCommitter.consumeManifestFile(m1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      dmlCommitter2.consumeManifestFile(m1);
      dmlCommitter2.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter2.consumeDeleteDataFilePath(deleteDataFile2);

      // start both commits
      dmlCommitter.beginDmlOperationTransaction();
      dmlCommitter2.beginDmlOperationTransaction();

      // end commit 1
      dmlCommitter.performUpdates();
      dmlCommitter.endDmlOperationTransaction();

      // end commit 2 should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                dmlCommitter2.commitImpl(true);
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file.
      // This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 3 existing file account and
      // 2 deleted file count.
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
  public void testConcurrentDeletesSucceedOnDifferentPartitions() throws IOException {
    // This test mimics to concurrently delete two different partitions data completely.
    // In that case, the first-committed DmlCommitter will only commit DataFiles that will be
    // removed from
    // Iceberg table, but not commit new data files.

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
      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.DELETE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.DELETE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter2 = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      String deleteDataFile3 = "books/add3.parquet";
      String deleteDataFile4 = "books/add4.parquet";
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "newManifestFileDml", newDataFile1, newDataFile2);

      // First DmlCommitter only commit the data files that will be removed from the table. But, it
      // does not commit
      // any new data files.
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      // The concurrent and second DmlCommitter could commit 1) only date files to delete; 2) date
      // files to delete and/or
      // new data files
      dmlCommitter2.consumeManifestFile(m1);
      dmlCommitter2.consumeDeleteDataFilePath(deleteDataFile3);
      dmlCommitter2.consumeDeleteDataFilePath(deleteDataFile4);

      // start both commits
      dmlCommitter.beginDmlOperationTransaction();
      dmlCommitter2.beginDmlOperationTransaction();

      // First DML commits
      dmlCommitter.performUpdates();
      dmlCommitter.endDmlOperationTransaction();
      Table tableAfterFirstDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "DML commit succeeds and increases table snapshots",
          tableAfterFirstDml.currentSnapshot().snapshotId(),
          tableBefore.currentSnapshot().snapshotId());

      // Second DML commits and succeeds.
      dmlCommitter2.performUpdates();
      dmlCommitter2.endDmlOperationTransaction();

      Table tableAfterSecondDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "DML commit succeeds and increases table snapshots",
          tableAfterSecondDml.currentSnapshot().snapshotId(),
          tableAfterFirstDml.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          tableAfterSecondDml.currentSnapshot().allManifests(tableAfterSecondDml.io());
      Assert.assertEquals(2, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("newManifestFileDml")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(2, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(1, (int) manifestFile.existingFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentDmlFailAfterInsert() throws IOException {
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
      Assert.assertEquals(3, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter =
          (IcebergInsertOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", dataFile1, dataFile2);
      ManifestFile manifestFile2 =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);
      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      insertCommitter.consumeManifestFile(manifestFile2);

      // Start the DML commit
      dmlCommitter.beginDmlOperationTransaction();

      // Make insert commit to increase a new snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert.snapshots()));

      // DML commit fails as there are new data files added that conflicts the DML commit's
      // conflictDetectionFilter: Expressions.alwaysTrue()

      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                dmlCommitter.commitImpl(true);
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // Insert commit succeeds and DML fails. The final table status should include the Insert's
      // manifest,
      // but not include DML's manifest.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(2, manifestFileList.size());
      for (ManifestFile mFile : manifestFileList) {
        Assert.assertFalse(mFile.path().contains("manifestFileDml"));
        if (mFile.path().contains("manifestFileInsert")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentInsertSucceedAfterDml() throws IOException {
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
      Assert.assertEquals(3, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter =
          (IcebergInsertOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", dataFile1, dataFile2);
      ManifestFile manifestFile2 =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);
      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      insertCommitter.consumeManifestFile(manifestFile2);

      // DML commits and increase snapshot and update metadata file.
      dmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterDml.snapshots()));

      // Insert commit should succeed and increase a new snapshot and update the metadata file.
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(5, Iterables.size(tableAfterInsert.snapshots()));

      // Both DML and Insert commits succeed, as Insert commit happens after DML.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(3, manifestFileList.size());
      for (ManifestFile mFile : manifestFileList) {
        if (mFile.path().contains("manifestFileInsert")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else if (manifestFile.path().contains("manifestFileDml")) {
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
  public void testConcurrentInsertsSucceed() throws IOException {
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
      Assert.assertEquals(3, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter = (IcebergInsertOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter2 =
          (IcebergInsertOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileInsert", dataFile1, dataFile2);
      ManifestFile manifestFile2 =
          writeManifest(tableFolder, "manifestFileInsert2", dataFile3, dataFile4);
      insertCommitter.consumeManifestFile(manifestFile);
      insertCommitter2.consumeManifestFile(manifestFile2);

      // First insert commits and increase the snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert.snapshots()));

      // Second insert commits successfully and increase the snapshot and update the metadata file
      // version
      insertCommitter2.commit();
      Table tableAfterInsert2 = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(5, Iterables.size(tableAfterInsert2.snapshots()));

      // Both insert commits should be successfully and the table should include both committed
      // manifest files.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(3, manifestFileList.size());
      for (ManifestFile mFile : manifestFileList) {
        if (mFile.path().contains("manifestFileInsert")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else if (mFile.path().contains("manifestFileInsert2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentDmlSucceedAfterRenameColumn() throws IOException {
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

      // Change the table's column name to update its metadata file version
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      String secondColumnName = schema.getColumn(1).getName();
      icebergModel.renameColumn(tableIdentifier, secondColumnName, secondColumnName + "1");

      // DML commit could succeed, as metadata file update does not block concurrent DML commit.
      dmlCommitter.performUpdates();
      dmlCommitter.endDmlOperationTransaction();

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file.
      // This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 3 existing file account and
      // 2 deleted file count.
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
  public void testConcurrentDmlFailRollback() throws IOException {
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
      Assert.assertEquals(3, Iterables.size(tableBefore.snapshots()));
      final long rollbackToSnapshotId = tableBefore.currentSnapshot().snapshotId();

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", dataFile1, dataFile2);
      ManifestFile manifestFile2 =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);

      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      // Increase one more snapshot
      IcebergOpCommitter committer =
          icebergModel.getInsertTableCommitter(tableIdentifier, operatorContext.getStats());
      Assert.assertTrue(committer instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter = (IcebergInsertOperationCommitter) committer;

      insertCommitter.consumeManifestFile(manifestFile2);
      // Make insert commit to increase a new snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert.snapshots()));
      Assert.assertNotEquals(rollbackToSnapshotId, tableAfterInsert.currentSnapshot().snapshotId());

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableAfterInsert.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer2;

      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      // Start the DML commit
      dmlCommitter.beginDmlOperationTransaction();

      // Rollback the table's status back to an old snapshot that will disturb DML commit.
      RollbackOption rollbackOption =
          new RollbackOption(
              RollbackOption.Type.SNAPSHOT,
              rollbackToSnapshotId,
              String.valueOf(rollbackToSnapshotId));
      icebergModel.rollbackTable(tableIdentifier, rollbackOption);
      Table tableAfterRollback = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(rollbackToSnapshotId, tableAfterRollback.currentSnapshot().snapshotId());

      // DML commit fails as Rollback disturbs the snapshot chain that DML originally has.
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                dmlCommitter.commitImpl(true);
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // Insert commit succeeds and DML fails. The final table status should include the Insert's
      // manifest,
      // but not include DML's manifest.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(1, manifestFileList.size());
      for (ManifestFile mFile : manifestFileList) {
        Assert.assertFalse(mFile.path().contains("manifestFileDml"));
        if (mFile.path().contains("manifestFileInsert")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeSucceedAfterRenameColumn() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile deleteDataFile5 = getDatafile("books/add5.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeAddDataFile(newDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile5);

      // Change the table's column name to update its metadata file version
      String secondColumnName = schema.getColumn(1).getName();
      icebergModel.renameColumn(tableIdentifier, secondColumnName, secondColumnName + "1");

      // Optimize commit should be successfully and update the snapshot and metadata file
      optimizeCommitter.commit(null);

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(), finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(2, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeFailAfterDml() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile deleteDataFile5 = getDatafile("books/add5.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", newDataFile3, newDataFile4);

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile5);

      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // DML commit first to delete some data files that Optimize needs to use.
      dmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(), tableAfterDml.currentSnapshot().snapshotId());

      // Optimize commits and fails due to current DML commit.
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                optimizeCommitter.commit(null);
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(
          "Optimize commit should not increase table snapshots",
          tableAfterDml.currentSnapshot().snapshotId(),
          finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(2, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeFailAfterOptimize() throws IOException {
    // Current Optimize queries work on at least one shared data files, etc.
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer2 instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter2 =
          (IcebergOptimizeOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);

      optimizeCommitter2.consumeAddDataFile(newDataFile3);
      optimizeCommitter2.consumeAddDataFile(newDataFile4);
      // Consume a shared deleted data file.
      optimizeCommitter2.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter2.consumeDeleteDataFile(deleteDataFile3);

      // OptimizeCommitter commit first to delete the shared data file that another
      // optimizeCommitter needs to check.
      optimizeCommitter.commit(null);
      Table tableAfterFirstOptimize = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterFirstOptimize.currentSnapshot().snapshotId());

      // OptimizeCommitters commits and fails due to missing data file..
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                optimizeCommitter2.commit(null);
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(
          "Second optimize commit should not increase table snapshots",
          tableAfterFirstOptimize.currentSnapshot().snapshotId(),
          finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(2, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeSucceedAfterDmlOnDifferentPartitions() throws IOException {
    // This test mimics to OPTIMIZE a table on its one partition, while concurrently DELETE the
    // whole data from
    // another partition and the concurrent DELETE finishes and commits earlier than OPTIMIZE.

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
      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.DELETE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "newManifestFileDml", newDataFile1, newDataFile2);

      // DmlCommitter could commit new data files and delete existing data files, which does not
      // affect the Optimize commit
      dmlCommitter.consumeManifestFile(m1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      // OptimizeCommitter assumes to commit new data files and remove old data files from another
      // partitions, which
      // is basically not affected by DmlCommitter.
      optimizeCommitter.consumeAddDataFile(newDataFile3);
      optimizeCommitter.consumeAddDataFile(newDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);

      // Dmlcommiter commits first.
      dmlCommitter.beginDmlOperationTransaction();
      dmlCommitter.performUpdates();
      dmlCommitter.endDmlOperationTransaction();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "DML commit succeeds and increases table snapshots",
          tableAfterDml.currentSnapshot().snapshotId(),
          tableBefore.currentSnapshot().snapshotId());

      // OptimizeCommitter commits and succeeds.
      optimizeCommitter.commit(null);
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "Optimize commit succeeds and increases table snapshots",
          finalTable.currentSnapshot().snapshotId(),
          tableAfterDml.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(3, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeSucceedAfterOptimizeOnDifferentPartitions() throws IOException {
    // This test mimics to OPTIMIZE a table on its one partition, while concurrently another
    // OPTIMIZE works on
    // another partition, finishes and commits earlier than the first OPTIMIZE.

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
      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer2 instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter2 =
          (IcebergOptimizeOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "newManifestFileDml", newDataFile1, newDataFile2);

      // Two OptimizeCommitters works on optimize different data files and don't overlap each other,
      // e.g., two different partitions.
      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);

      // Second OptimizeCommitter works on different data files
      optimizeCommitter2.consumeAddDataFile(newDataFile3);
      optimizeCommitter2.consumeAddDataFile(newDataFile4);
      optimizeCommitter2.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter2.consumeDeleteDataFile(deleteDataFile4);

      // First OptimizeCommitter commits.
      optimizeCommitter.commit(null);
      Table tableAfterFirstOptimize = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "First Optimize commit succeeds and increases table snapshots",
          tableAfterFirstOptimize.currentSnapshot().snapshotId(),
          tableBefore.currentSnapshot().snapshotId());

      // Second OptimizeCommitter commits and succeeds.
      optimizeCommitter2.commit(null);
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "Second Optimize commit succeeds and increases table snapshots",
          finalTable.currentSnapshot().snapshotId(),
          tableAfterFirstOptimize.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(3, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentOptimizeSucceedAfterInsert() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter =
          (IcebergInsertOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile deleteDataFile5 = getDatafile("books/add5.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileInsert", newDataFile3, newDataFile4);

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile5);

      insertCommitter.consumeManifestFile(manifestFile);

      // Insert commit first its own manifest.
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterInsert.currentSnapshot().snapshotId());

      // Optimize succeed to commit. As Insert does not suppose to impact the data files that
      // Optimize works on.
      optimizeCommitter.commit(null);

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableAfterInsert.currentSnapshot().snapshotId(),
          finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(3, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentDmlFailAfterOptimize() throws IOException {
    final String tableName = UUID.randomUUID().toString();
    final File tableFolder = new File(folder, tableName);
    final List<String> datasetPath = Lists.newArrayList("dfs", tableName);
    try {
      DatasetConfig datasetConfig = getDatasetConfig(datasetPath);
      String metadataFileLocation = initialiseTableWithLargeSchema(schema, tableName);
      IcebergMetadata icebergMetadata = new IcebergMetadata();
      icebergMetadata.setMetadataFileLocation(metadataFileLocation);
      datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);

      IcebergTableProps tableProps =
          new IcebergTableProps(tableFolder.getAbsolutePath(), tableName);
      Table tableBefore = getIcebergTable(icebergModel, tableFolder);
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      IcebergOpCommitter committer =
          icebergModel.getOptimizeCommitter(
              operatorContext.getStats(),
              tableIdentifier,
              datasetConfig,
              2L,
              tableBefore.currentSnapshot().snapshotId(),
              tableProps,
              localFs);
      Assert.assertTrue(committer instanceof IcebergOptimizeOperationCommitter);
      IcebergOptimizeOperationCommitter optimizeCommitter =
          (IcebergOptimizeOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId());
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter dmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile deleteDataFile5 = getDatafile("books/add5.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", newDataFile3, newDataFile4);

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile5);

      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // Optimize commit first to delete some data files that DML needs to use.
      optimizeCommitter.commit(null);
      Table tableAfterOptimize = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterOptimize.currentSnapshot().snapshotId());

      // DML commits and fails due to current Optimize commit.
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                dmlCommitter.commit();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(
          "DML commit should not increase table snapshots",
          tableAfterOptimize.currentSnapshot().snapshotId(),
          finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(2, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentDmlFailAfterTruncate() throws IOException {
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
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
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
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", newDataFile3, newDataFile4);
      dmlCommitter.consumeManifestFile(manifestFile);
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      dmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // Truncate the Iceberg table and the table is supposed to empty.
      icebergModel.truncateTable(tableIdentifier);
      Table tableAfterTruncate = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterTruncate.currentSnapshot().snapshotId());

      // DML commits and fails due to current Optimize commit.
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                dmlCommitter.commit();
              })
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");

      // After this operation, the manifestList was expected to have two manifest file.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(
          "DML commit should not increase table snapshots",
          tableAfterTruncate.currentSnapshot().snapshotId(),
          finalTable.currentSnapshot().snapshotId());
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(1, manifestFileList.size());
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }
}
