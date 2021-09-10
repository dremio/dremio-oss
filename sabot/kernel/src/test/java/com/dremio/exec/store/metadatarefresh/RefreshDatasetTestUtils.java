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
package com.dremio.exec.store.metadatarefresh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

import com.dremio.BaseTestQuery;

public class RefreshDatasetTestUtils {

  public static FileSystem setupLocalFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    return FileSystem.get(conf);
  }

  public static void fsDelete(FileSystem fs, Path path) throws IOException {
    FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      fs.delete(status.getPath(), true);
    }
  }

  private static int getTotalFileCount(Table icebergTable) {
    return icebergTable.currentSnapshot().dataManifests().stream().map(x -> x.addedFilesCount() + x.existingFilesCount()).reduce(0, Integer::sum);
  }

  public static Table getIcebergTable(String tableFolderPath) {
    File tableFolder = new File(tableFolderPath);
    assertTrue(tableFolder.exists());
    File tablePath = tableFolder.listFiles()[0];
    return BaseTestQuery.getIcebergTable(tablePath);
  }

  public static void verifyIcebergMetadata(String tableFolderPath, int expectedAddedFiles, int expectedRemovedFiles, Schema expectedSchema, Set<String> expectedPartitionCols, int expectedTotalFileCount) {
    Table icebergTable = getIcebergTable(tableFolderPath);

    Schema sc = icebergTable.schema();
    PartitionSpec spec = icebergTable.spec();
    List<String> addedFilesList = getAddedFilePaths(icebergTable);
    List<String> deletedFilesList = getDeletedFilePaths(icebergTable);
    logDataFilePaths(addedFilesList, "List of added files in " + icebergTable.location());
    logDataFilePaths(deletedFilesList, "List of deleted files in " + icebergTable.location());
    int addedFiles = addedFilesList.size();
    int removedFiles = deletedFilesList.size();

    Assert.assertEquals(expectedAddedFiles, addedFiles);
    Assert.assertEquals(expectedRemovedFiles, removedFiles);
    verifyIcebergSchema(sc, expectedSchema);
    Set<String> actualPartitionCols = spec.fields().stream().map(f -> f.name()).collect(Collectors.toSet());
    assertEquals(expectedPartitionCols, actualPartitionCols);
    Assert.assertEquals(getTotalFileCount(icebergTable), expectedTotalFileCount);
  }

  public static void verifyIcebergSchema(Schema tableSchema, Schema expectedSchema) {
    List<Types.NestedField> schemaColumns = tableSchema.columns();

    Assert.assertEquals(schemaColumns.size(), tableSchema.columns().size());
    schemaColumns.stream().forEach(tableColumn -> {
      Types.NestedField expectedColumn = expectedSchema.findField(tableColumn.name());
      Assert.assertNotNull(expectedColumn);
      Assert.assertEquals(expectedColumn.name(), tableColumn.name());
      Assert.assertEquals(expectedColumn.type().toString(), tableColumn.type().toString());
      Assert.assertEquals(expectedColumn.isRequired(), tableColumn.isRequired());
    });
  }

  public static List<String> getAddedFilePaths(Table icebergTable) {
    List<String> addedPaths = new ArrayList<>();
    for (DataFile dataFile : icebergTable.currentSnapshot().addedFiles()) {
      addedPaths.add(dataFile.path().toString());
    }
    return addedPaths;
  }

  public static List<String> getDeletedFilePaths(Table icebergTable) {
    List<String> deletedPaths = new ArrayList<>();
    for (DataFile dataFile : icebergTable.currentSnapshot().deletedFiles()) {
      deletedPaths.add(dataFile.path().toString());
    }
    return deletedPaths;
  }

  public static void logDataFilePaths(List<String> dataFiles, String headerString) {
    System.out.println(headerString);
    for (String dataFile: dataFiles) {
      System.out.println(dataFile);
    }
  }
}
