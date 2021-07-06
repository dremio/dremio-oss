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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogRequestBuilder;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.google.common.collect.Iterables;

public class TestIcebergOpCommitter extends BaseTestQuery {

  String tableName;
  TemporaryFolder folder = new TemporaryFolder();
  File tableFolder = null;
  IcebergHadoopModel icebergHadoopModel = null;

  BatchSchema schema = BatchSchema.of(
    Field.nullablePrimitive("id", new ArrowType.Int(64, false)),
    Field.nullablePrimitive("data", new ArrowType.Utf8()));

  DatasetCatalogGrpcClient client = new DatasetCatalogGrpcClient(getSabotContext().getDatasetCatalogBlockingStub().get());

  @Before
  public void initialiseCommitter() throws IOException {
    tableName = UUID.randomUUID().toString();
    folder.create();
    tableFolder = new File(folder.getRoot(), tableName);
    tableFolder.mkdir();

    FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    icebergHadoopModel = new IcebergHadoopModel("", new Configuration(), null, null, null, client);
    when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergHadoopModel);

    IcebergOpCommitter committer = icebergHadoopModel.getCreateTableCommitter(tableName,
      icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
      schema, null);
    committer.commit();  //Creates Table

    client.getCatalogServiceApi().addOrUpdateDataset(
      DatasetCatalogRequestBuilder.forFullMetadataRefresh(
        tableName,
        tableFolder.toPath().toString(),
        schema, Collections.emptyList()).build());

    IcebergOpCommitter insertTableCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
      tableFolder.toPath().toString(),
      icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
      schema,
      Collections.emptyList());
    DataFile dataFile1 = getDatafile("books/add1.parquet");
    DataFile dataFile2 = getDatafile("books/add2.parquet");
    DataFile dataFile3 = getDatafile("books/add3.parquet");
    DataFile dataFile4 = getDatafile("books/add4.parquet");
    DataFile dataFile5 = getDatafile("books/add5.parquet");

    ManifestFile m1 = writeManifest("manifestFile1", dataFile1, dataFile2, dataFile3, dataFile4, dataFile5);
    insertTableCommitter.consumeManifestFile(m1);
    insertTableCommitter.commit();  // Inserts few files in table.
  }

  @Test
  public void testAddOnlyMetadataRefreshCommitter() throws IOException {
    IcebergOpCommitter insertTableCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
      tableFolder.toPath().toString(),
      icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
      schema,
      Collections.emptyList());
    DataFile dataFile6 = getDatafile("books/add1.parquet");
    DataFile dataFile7 = getDatafile("books/add2.parquet");
    ManifestFile m1 = writeManifest("manifestFile2", dataFile6, dataFile7);
    insertTableCommitter.consumeManifestFile(m1);
    insertTableCommitter.commit();
    Table table = getIcebergTable(tableFolder);
    List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests();
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
  }

  @Test
  public void testDeleteThenAddMetadataRefreshCommitter() throws IOException {
    try {
      IcebergOpCommitter metaDataRefreshCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
        tableFolder.toPath().toString(),
        icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList());

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest("manifestFile2", dataFile1, dataFile2);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile3);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile4);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile5);
      metaDataRefreshCommitter.consumeManifestFile(m1);
      metaDataRefreshCommitter.commit();

      // After this operation manifestList was expected to have two manifest file
      // One is manifestFile2 and other one is newly created due to delete data file. as This newly created Manifest is due to rewriting
      // of manifestFile1 file. it is expected to 2 existing file account and 3 deleted file count.
      Table table = getIcebergTable(tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests();
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(3, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(2, (int) manifestFile.existingFilesCount());
        }
      }

      UpdatableDatasetConfigFields dataset = client.getCatalogServiceApi()
        .getDataset(GetDatasetRequest.newBuilder().addDatasetPath(tableName).build());

      Assert.assertEquals(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET, dataset.getDatasetType());

      Assert.assertEquals(schema, BatchSchema.deserialize(dataset.getBatchSchema().toByteArray()));

      Assert.assertEquals(tableFolder.toPath().toString(), dataset.getFileFormat().getLocation());
      Assert.assertEquals(FileProtobuf.FileType.ICEBERG, dataset.getFileFormat().getType());

      Assert.assertEquals(4, dataset.getReadDefinition().getManifestScanStats().getRecordCount());
      Assert.assertEquals(36, dataset.getReadDefinition().getScanStats().getRecordCount());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteDirectory(folder.getRoot());
    }
  }

  @Test
  public void testAcrossBatchMetadataRefreshCommitter() throws IOException {
    try {
      IcebergOpCommitter metaDataRefreshCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
        tableFolder.toPath().toString(),
        icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList());

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile2 = getDatafile("books/add5.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest("manifestFile2", dataFile1, dataFile2);
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
      Table table = getIcebergTable(tableFolder);
      List<ManifestFile> manifestFileList = table.currentSnapshot().allManifests();
      for (ManifestFile manifestFile : manifestFileList) {
        if (manifestFile.path().contains("manifestFile2")) {
          Assert.assertEquals(2, (int) manifestFile.addedFilesCount());
        } else {
          Assert.assertEquals(3, (int) manifestFile.deletedFilesCount());
          Assert.assertEquals(2, (int) manifestFile.existingFilesCount());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteDirectory(folder.getRoot());
    }
  }

  @Test
  public void testNumberOfSnapshot() throws IOException {
    try {
      Table oldTable = getIcebergTable(tableFolder);
      Assert.assertEquals(2, Iterables.size(oldTable.snapshots()));
      IcebergOpCommitter metaDataRefreshCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
        tableFolder.toPath().toString(),
        icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList());

      DataFile dataFile1 = getDatafile("books/add4.parquet");
      DataFile dataFile3 = getDatafile("books/add3.parquet");
      DataFile dataFile4 = getDatafile("books/add4.parquet");
      DataFile dataFile5 = getDatafile("books/add5.parquet");
      ManifestFile m1 = writeManifest("manifestFile2", dataFile1);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile3);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile4);
      metaDataRefreshCommitter.consumeDeleteDataFile(dataFile5);
      metaDataRefreshCommitter.consumeManifestFile(m1);
      metaDataRefreshCommitter.commit();
      Table table = getIcebergTable(tableFolder);
      Assert.assertEquals(4, Iterables.size(table.snapshots()));

      metaDataRefreshCommitter = icebergHadoopModel.getIncrementalMetadataRefreshCommitter(tableName,
        tableFolder.toPath().toString(),
        icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
        schema,
        Collections.emptyList());
      DataFile dataFile2 = getDatafile("books/add2.parquet");
      ManifestFile m2 = writeManifest("manifestFile3", dataFile2);
      metaDataRefreshCommitter.consumeManifestFile(m2);
      metaDataRefreshCommitter.commit();
      table = getIcebergTable(tableFolder);
      Assert.assertEquals(5, Iterables.size(table.snapshots()));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteDirectory(folder.getRoot());
    }
  }

  ManifestFile writeManifest(String fileName, DataFile... files) throws IOException {
    return writeManifest(fileName, null, files);
  }

  ManifestFile writeManifest(String fileName, Long snapshotId, DataFile... files) throws IOException {
    File manifestFile = folder.newFile(fileName + ".avro");
    Table table = getIcebergTable(tableFolder);
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
}
