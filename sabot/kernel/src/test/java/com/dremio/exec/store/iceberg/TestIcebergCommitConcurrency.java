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
import com.dremio.exec.expr.fn.impl.ByteArrayWrapper;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergCommitConcurrency extends TestIcebergCommitterBase {

  @Test
  public void testConcurrentOnTwoCowDmlsFail() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter1 = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter2 = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2);
      cowDmlCommitter1.consumeManifestFile(m1);
      cowDmlCommitter1.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter1.consumeDeleteDataFilePath(deleteDataFile2);

      cowDmlCommitter2.consumeManifestFile(m1);
      cowDmlCommitter2.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter2.consumeDeleteDataFilePath(deleteDataFile2);

      // start both commits
      cowDmlCommitter1.beginDmlOperationTransaction();
      cowDmlCommitter2.beginDmlOperationTransaction();

      // end commit 1
      cowDmlCommitter1.performCopyOnWriteTransaction();
      cowDmlCommitter1.endDmlOperationTransaction();

      // end commit 2 should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                cowDmlCommitter2.commitImpl(true);
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
  public void testConcurrentOneCowOneMorDmlsFail() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Copy On Write DML
      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1Cow = getDatafile("books/add7.parquet");
      DataFile dataFile2Cow = getDatafile("books/add8.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "manifestFileCowDmlDelete", dataFile1Cow, dataFile2Cow);
      cowDmlCommitter.consumeManifestFile(m1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      // Merge On Read DML
      DataFile dataFile1Mor = getDatafile("books/add9.parquet");
      DataFile dataFile2Mor = getDatafile("books/add10.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1Mor, dataFile2Mor);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);

      morDmlCommitter.consumePositionalDeleteFile(
          positionalDeleteFile, getReferencedDataFiles("books/add1.parquet"));
      morDmlCommitter.consumePositionalDeleteFile(
          positionalDeleteFile2, getReferencedDataFiles("books/add2.parquet"));

      // start both commits
      cowDmlCommitter.beginDmlOperationTransaction();
      morDmlCommitter.beginDmlOperationTransaction();

      // end COW commit
      cowDmlCommitter.performCopyOnWriteTransaction();
      cowDmlCommitter.endDmlOperationTransaction();

      // end MOR commit. should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                morDmlCommitter.commitImpl(true);
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
        if (manifestFile.path().contains("manifestFileCowDmlDelete")) {
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
  public void testConcurrentOneMorOneCowDmlsFails() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Merge On Read DML
      DataFile dataFile1Mor = getDatafile("books/add9.parquet");
      DataFile dataFile2Mor = getDatafile("books/add10.parquet");
      DataFile dataFile3Mor = getDatafile("books/add11.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1Mor, dataFile2Mor, dataFile3Mor);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, Collections.emptySet());
      }

      // Copy On Write DML
      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1Cow = getDatafile("books/add7.parquet");
      DataFile dataFile2Cow = getDatafile("books/add8.parquet");

      ManifestFile m1 =
          writeManifest(tableFolder, "manifestFileCowDmlDelete", dataFile1Cow, dataFile2Cow);
      cowDmlCommitter.consumeManifestFile(m1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      // start both commits
      morDmlCommitter.beginDmlOperationTransaction();
      cowDmlCommitter.beginDmlOperationTransaction();

      // end commit 1
      morDmlCommitter.performMergeOnReadTransaction();
      morDmlCommitter.endDmlOperationTransaction();

      // end commit 2 should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                cowDmlCommitter.commitImpl(true);
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
      Assert.assertEquals(3, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.content() == ManifestContent.DELETES) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else if (!manifestFile.path().contains("manifestFile1.avro")
            && manifestFile.content() == ManifestContent.DATA) {
          Assert.assertEquals(3, (int) manifestFile.addedFilesCount());
        }
      }
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }

  @Test
  public void testConcurrentTwoMorDmlsFail() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter1 = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter2 = (IcebergDmlOperationCommitter) committer2;

      // Merge On Read DML 1
      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1Mor1 = getDatafile("books/add7.parquet");
      DataFile dataFile2Mor1 = getDatafile("books/add8.parquet");
      DataFile dataFile3Mor1 = getDatafile("books/add9.parquet");

      DeleteFile positionalDeleteFile1Mor1 = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2Mor1 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFilesMor1 =
          Lists.newArrayList(dataFile1Mor1, dataFile2Mor1, dataFile3Mor1);
      List<DeleteFile> deleteFilesMor1 =
          Lists.newArrayList(positionalDeleteFile1Mor1, positionalDeleteFile2Mor1);

      dataFilesMor1.forEach(morDmlCommitter1::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFilesMor1) {
        morDmlCommitter1.consumePositionalDeleteFile(
            deleteFile, getReferencedDataFiles("books/add1.parquet"));
      }

      // Merge On Read DML 2
      DataFile dataFile1Mor2 = getDatafile("books/add7.parquet");
      DataFile dataFile2Mor2 = getDatafile("books/add8.parquet");
      DataFile dataFile3Mor2 = getDatafile("books/add9.parquet");

      DeleteFile positionalDeleteFile1Mor2 = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2Mor2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFilesMor2 =
          Lists.newArrayList(dataFile1Mor2, dataFile2Mor2, dataFile3Mor2);
      List<DeleteFile> deleteFilesMor2 =
          Lists.newArrayList(positionalDeleteFile1Mor2, positionalDeleteFile2Mor2);

      dataFilesMor2.forEach(morDmlCommitter2::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFilesMor2) {
        morDmlCommitter2.consumePositionalDeleteFile(
            deleteFile, getReferencedDataFiles("books/add2.parquet"));
      }

      // start both commits
      morDmlCommitter1.beginDmlOperationTransaction();
      morDmlCommitter2.beginDmlOperationTransaction();

      // end commit 1
      morDmlCommitter1.performMergeOnReadTransaction();
      morDmlCommitter1.endDmlOperationTransaction();

      // end commit 2 should fail with CommitFailedException
      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                morDmlCommitter2.commitImpl(true);
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
      Assert.assertEquals(3, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.content() == ManifestContent.DELETES) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else if (!manifestFile.path().contains("manifestFile1.avro")
            && manifestFile.content() == ManifestContent.DATA) {
          Assert.assertEquals(3, (int) manifestFile.addedFilesCount());
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter1 = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.DELETE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter2 = (IcebergDmlOperationCommitter) committer2;

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
      cowDmlCommitter1.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter1.consumeDeleteDataFilePath(deleteDataFile2);

      // The concurrent and second DmlCommitter could commit 1) only date files to delete; 2) date
      // files to delete and/or
      // new data files
      cowDmlCommitter2.consumeManifestFile(m1);
      cowDmlCommitter2.consumeDeleteDataFilePath(deleteDataFile3);
      cowDmlCommitter2.consumeDeleteDataFilePath(deleteDataFile4);

      // start both commits
      cowDmlCommitter1.beginDmlOperationTransaction();
      cowDmlCommitter2.beginDmlOperationTransaction();

      // First DML commits
      cowDmlCommitter1.performCopyOnWriteTransaction();
      cowDmlCommitter1.endDmlOperationTransaction();
      Table tableAfterFirstDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          "DML commit succeeds and increases table snapshots",
          tableAfterFirstDml.currentSnapshot().snapshotId(),
          tableBefore.currentSnapshot().snapshotId());

      // Second DML commits and succeeds.
      cowDmlCommitter2.performCopyOnWriteTransaction();
      cowDmlCommitter2.endDmlOperationTransaction();

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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

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
      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      insertCommitter.consumeManifestFile(manifestFile2);

      // Start the DML commit
      cowDmlCommitter.beginDmlOperationTransaction();

      // Make insert commit to increase a new snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(tableAfterInsert.snapshots()));

      // DML commit fails as there are new data files added that conflicts the DML commit's
      // conflictDetectionFilter: Expressions.alwaysTrue()

      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                cowDmlCommitter.commitImpl(true);
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
  public void testConcurrentMorDmlFailAfterInsert() throws IOException {
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter =
          (IcebergInsertOperationCommitter) committer2;

      // Merge On Read DML 1
      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);
      insertCommitter.consumeManifestFile(manifestFile);

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(cowDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        cowDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      // Start the DML commit
      cowDmlCommitter.beginDmlOperationTransaction();

      // Make insert commit to increase a new snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(tableAfterInsert.snapshots()));

      // DML commit fails as there are new data files added that conflicts the DML commit's
      // conflictDetectionFilter: Expressions.alwaysTrue()

      UserExceptionAssert.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                cowDmlCommitter.commitImpl(true);
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

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
      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      insertCommitter.consumeManifestFile(manifestFile2);

      // DML commits and increase snapshot and update metadata file.
      cowDmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(tableAfterDml.snapshots()));

      // Insert commit should succeed and increase a new snapshot and update the metadata file.
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert.snapshots()));

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
  public void testConcurrentInsertSucceedAfterMorDml() throws IOException {
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));

      IcebergOpCommitter committer =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

      IcebergOpCommitter committer2 =
          icebergModel.getInsertTableCommitter(
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              operatorContext.getStats());
      Assert.assertTrue(committer2 instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter =
          (IcebergInsertOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(cowDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        cowDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      ManifestFile manifestFile1 =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);
      insertCommitter.consumeManifestFile(manifestFile1);

      // DML commits and increase snapshot and update metadata file.
      cowDmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(tableAfterDml.snapshots()));

      // Insert commit should succeed and increase a new snapshot and update the metadata file.
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert.snapshots()));

      // Both DML and Insert commits succeed, as Insert commit happens after DML.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(4, manifestFileList.size());
      boolean initialManifestFound = false;
      boolean dmlDataFileManifestFound = false;
      for (ManifestFile mFile : manifestFileList) {
        if (mFile.path().contains("manifestFileInsert")) {
          Assert.assertEquals(2, (int) mFile.addedFilesCount());
        } else if (mFile.content() == ManifestContent.DELETES) {
          Assert.assertEquals(2, (int) mFile.addedFilesCount());
        } else {
          if (mFile.addedFilesCount() == 5) {
            initialManifestFound = true;
          } else if (mFile.addedFilesCount() == 3) {
            dmlDataFileManifestFound = true;
          }
        }
      }
      Assert.assertTrue(initialManifestFound);
      Assert.assertTrue(dmlDataFileManifestFound);
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));

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
      Assert.assertEquals(3, Iterables.size(tableAfterInsert.snapshots()));

      // Second insert commits successfully and increase the snapshot and update the metadata file
      // version
      insertCommitter2.commit();
      Table tableAfterInsert2 = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(4, Iterables.size(tableAfterInsert2.snapshots()));

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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

      // Add a new manifest list, and delete several previous datafiles
      String deleteDataFile1 = "books/add1.parquet";
      String deleteDataFile2 = "books/add2.parquet";
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");

      ManifestFile m1 = writeManifest(tableFolder, "manifestFileDmlDelete", dataFile1, dataFile2);
      cowDmlCommitter.consumeManifestFile(m1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      cowDmlCommitter.beginDmlOperationTransaction();

      // Change the table's column name to update its metadata file version
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      String secondColumnName = schema.getColumn(1).getName();
      icebergModel.renameColumn(tableIdentifier, secondColumnName, secondColumnName + "1");

      // DML commit could succeed, as metadata file update does not block concurrent DML commit.
      cowDmlCommitter.performCopyOnWriteTransaction();
      cowDmlCommitter.endDmlOperationTransaction();

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
  public void testConcurrentMorDmlSucceedAfterRenameColumn() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer;

      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }
      morDmlCommitter.beginDmlOperationTransaction();

      // Change the table's column name to update its metadata file version
      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      String secondColumnName = schema.getColumn(1).getName();
      icebergModel.renameColumn(tableIdentifier, secondColumnName, secondColumnName + "1");

      // DML commit could succeed, as metadata file update does not block concurrent DML commit.
      morDmlCommitter.performMergeOnReadTransaction();
      morDmlCommitter.endDmlOperationTransaction();

      // After this operation, the manifestList was expected to have two manifest file.
      // One is 'manifestFileDelete' and the other is the newly created due to delete data file.
      // This newly created manifest
      // is due to rewriting of 'manifestFile1' file. It is expected to 3 existing file account and
      // 2 deleted file count.
      Table table = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests(table.io());
      Assert.assertEquals(3, manifestFileList.size());
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.content() == ManifestContent.DELETES) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else if (!manifestFile.path().contains("manifestFile1.avro")
            && manifestFile.content() == ManifestContent.DATA) {
          Assert.assertEquals(3, (int) manifestFile.addedFilesCount());
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));
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
      Assert.assertEquals(3, Iterables.size(tableAfterInsert.snapshots()));
      Assert.assertNotEquals(rollbackToSnapshotId, tableAfterInsert.currentSnapshot().snapshotId());

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableAfterInsert.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer2;

      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);
      // Start the DML commit
      cowDmlCommitter.beginDmlOperationTransaction();

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
                cowDmlCommitter.commitImpl(true);
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
  public void testConcurrentMorDmlFailRollback() throws IOException {
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
      Assert.assertEquals(2, Iterables.size(tableBefore.snapshots()));
      final long rollbackToSnapshotId = tableBefore.currentSnapshot().snapshotId();

      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");
      DataFile dataFile4 = getDatafile("books/add10.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      ManifestFile manifestFileIns =
          writeManifest(tableFolder, "manifestFileInsert", dataFile3, dataFile4);

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      IcebergTableIdentifier tableIdentifier =
          icebergModel.getTableIdentifier(tableFolder.toPath().toString());
      // Increase one more snapshot
      IcebergOpCommitter committer =
          icebergModel.getInsertTableCommitter(tableIdentifier, operatorContext.getStats());
      Assert.assertTrue(committer instanceof IcebergInsertOperationCommitter);
      IcebergInsertOperationCommitter insertCommitter = (IcebergInsertOperationCommitter) committer;

      insertCommitter.consumeManifestFile(manifestFileIns);
      // Make insert commit to increase a new snapshot and update the metadata file version
      insertCommitter.commit();
      Table tableAfterInsert = getIcebergTable(icebergModel, tableFolder);
      Assert.assertEquals(3, Iterables.size(tableAfterInsert.snapshots()));
      Assert.assertNotEquals(rollbackToSnapshotId, tableAfterInsert.currentSnapshot().snapshotId());

      IcebergOpCommitter committer2 =
          icebergModel.getDmlCommitter(
              operatorContext,
              icebergModel.getTableIdentifier(tableFolder.toPath().toString()),
              datasetConfig,
              IcebergCommandType.UPDATE,
              tableAfterInsert.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer2;

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      // Start the DML commit
      morDmlCommitter.beginDmlOperationTransaction();

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
      Assertions.assertThatThrownBy(
              () -> {
                // Don't need to start beginOperation, as it started.
                morDmlCommitter.commitImpl(true);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageMatching("^Snapshot \\d+ is not an ancestor of \\d+$");

      // Insert commit succeeds and DML fails. The final table status should include the Insert's
      // manifest,
      // but not include DML's maxifest.
      Table finalTable = getIcebergTable(icebergModel, tableFolder);
      List<ManifestFile> manifestFileList =
          finalTable.currentSnapshot().allManifests(finalTable.io());
      Assert.assertEquals(1, manifestFileList.size());
      for (ManifestFile mFile : manifestFileList) {
        Assert.assertNotSame(mFile.content(), ManifestContent.DELETES);
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer2;

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

      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // DML commit first to delete some data files that Optimize needs to use.
      cowDmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(), tableAfterDml.currentSnapshot().snapshotId());

      // Optimize commits and fails due to current DML commit.
      UserExceptionAssert.assertThatThrownBy(() -> optimizeCommitter.commit(null))
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
  public void testConcurrentMorOptimizeFailAfterDml() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer2;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile deleteDataFile3 = getDatafile("books/add3.parquet");
      DataFile deleteDataFile4 = getDatafile("books/add4.parquet");
      DataFile deleteDataFile5 = getDatafile("books/add5.parquet");
      DataFile newDataFile1 = getDatafile("books/add7.parquet");
      DataFile newDataFile2 = getDatafile("books/add8.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(newDataFile1, newDataFile2, newDataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      optimizeCommitter.consumeAddDataFile(newDataFile1);
      optimizeCommitter.consumeAddDataFile(newDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile1);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile2);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile5);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      // DML commit first to delete some data files that Optimize needs to use.
      morDmlCommitter.commit();
      Table tableAfterDml = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(), tableAfterDml.currentSnapshot().snapshotId());

      // Optimize commits and fails due to current DML commit.
      UserExceptionAssert.assertThatThrownBy(() -> optimizeCommitter.commit(null))
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
      Assert.assertEquals(3, manifestFileList.size());
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
      UserExceptionAssert.assertThatThrownBy(() -> optimizeCommitter2.commit(null))
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer2;

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
      cowDmlCommitter.consumeManifestFile(m1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2);

      // OptimizeCommitter assumes to commit new data files and remove old data files from another
      // partitions, which
      // is basically not affected by DmlCommitter.
      optimizeCommitter.consumeAddDataFile(newDataFile3);
      optimizeCommitter.consumeAddDataFile(newDataFile4);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile3);
      optimizeCommitter.consumeDeleteDataFile(deleteDataFile4);

      // Dmlcommiter commits first.
      cowDmlCommitter.beginDmlOperationTransaction();
      cowDmlCommitter.performCopyOnWriteTransaction();
      cowDmlCommitter.endDmlOperationTransaction();
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer2;

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

      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // Optimize commit first to delete some data files that DML needs to use.
      optimizeCommitter.commit(null);
      Table tableAfterOptimize = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterOptimize.currentSnapshot().snapshotId());

      // DML commits and fails due to current Optimize commit.
      UserExceptionAssert.assertThatThrownBy(cowDmlCommitter::commit)
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
  public void testConcurrentMorDmlFailAfterOptimize() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer2 instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer2;

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

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(newDataFile1, newDataFile2, newDataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      // Optimize commit first to delete some data files that DML needs to use.
      optimizeCommitter.commit(null);
      Table tableAfterOptimize = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterOptimize.currentSnapshot().snapshotId());

      // DML commits and fails due to current Optimize commit.
      UserExceptionAssert.assertThatThrownBy(morDmlCommitter::commit)
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.COPY_ON_WRITE);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter cowDmlCommitter = (IcebergDmlOperationCommitter) committer;

      // Add a new manifest list, and delete several previous datafiles
      DataFile deleteDataFile1 = getDatafile("books/add1.parquet");
      DataFile deleteDataFile2 = getDatafile("books/add2.parquet");
      DataFile newDataFile3 = getDatafile("books/add9.parquet");
      DataFile newDataFile4 = getDatafile("books/add10.parquet");

      ManifestFile manifestFile =
          writeManifest(tableFolder, "manifestFileDml", newDataFile3, newDataFile4);
      cowDmlCommitter.consumeManifestFile(manifestFile);
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile1.path().toString());
      cowDmlCommitter.consumeDeleteDataFilePath(deleteDataFile2.path().toString());

      // Truncate the Iceberg table and the table is supposed to empty.
      icebergModel.truncateTable(tableIdentifier);
      Table tableAfterTruncate = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterTruncate.currentSnapshot().snapshotId());

      // DML commits and fails due to current Optimize commit.
      UserExceptionAssert.assertThatThrownBy(cowDmlCommitter::commit)
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

  @Test
  public void testConcurrentMorDmlFailAfterTruncate() throws IOException {
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
              tableBefore.currentSnapshot().snapshotId(),
              RowLevelOperationMode.MERGE_ON_READ);
      Assert.assertTrue(committer instanceof IcebergDmlOperationCommitter);
      IcebergDmlOperationCommitter morDmlCommitter = (IcebergDmlOperationCommitter) committer;

      // Add a new manifest list, and delete several previous datafiles
      DataFile dataFile1 = getDatafile("books/add7.parquet");
      DataFile dataFile2 = getDatafile("books/add8.parquet");
      DataFile dataFile3 = getDatafile("books/add9.parquet");

      // Truncate the Iceberg table and the table is supposed to empty.
      icebergModel.truncateTable(tableIdentifier);
      Table tableAfterTruncate = getIcebergTable(icebergModel, tableFolder);
      Assert.assertNotEquals(
          tableBefore.currentSnapshot().snapshotId(),
          tableAfterTruncate.currentSnapshot().snapshotId());

      DeleteFile positionalDeleteFile = getPositionalDeleteFile("books/posDel1.parquet");
      DeleteFile positionalDeleteFile2 = getPositionalDeleteFile("books/posDel2.parquet");

      List<DataFile> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3);
      List<DeleteFile> deleteFiles =
          Lists.newArrayList(positionalDeleteFile, positionalDeleteFile2);

      dataFiles.forEach(morDmlCommitter::consumeMergeOnReadAddDataFile);

      Set<ByteArrayWrapper> referencedFilesAsBytes = getReferencedDataFiles("books/add1.parquet");
      for (DeleteFile deleteFile : deleteFiles) {
        morDmlCommitter.consumePositionalDeleteFile(deleteFile, referencedFilesAsBytes);
      }

      // DML commits and fails due to current truncate commit.
      UserExceptionAssert.assertThatThrownBy(morDmlCommitter::commit)
          .hasErrorType(CONCURRENT_MODIFICATION)
          .hasMessageContaining("Concurrent operation has updated the table, please retry.");
    } finally {
      FileUtils.deleteDirectory(tableFolder);
    }
  }
}
