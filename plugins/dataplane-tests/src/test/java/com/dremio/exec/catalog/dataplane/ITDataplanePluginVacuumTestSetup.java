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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createPartitionTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.getLastSnapshotQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarOnSnapshotQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useReferenceQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;

import com.dremio.common.util.TestTools;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionStatsMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ITDataplanePluginVacuumTestSetup extends ITDataplanePluginTestSetup {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ITDataplanePluginVacuumTestSetup.class);

  ITDataplanePluginVacuumTestSetup() {
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  protected String getMetadataLoc(List<String> table, Reference ref)
      throws NessieNotFoundException {
    ContentKey key = ContentKey.of(table);
    return ((IcebergTable) getNessieApi().getContent().key(key).reference(ref).get().get(key))
        .getMetadataLocation();
  }

  protected Predicate<String> manifestListsCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && path.contains("snap") && path.endsWith("avro");
  }

  protected Predicate<String> manifestsCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && !path.contains("snap") && path.endsWith("avro");
  }

  protected Predicate<String> dataFilesCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && path.endsWith("parquet");
  }

  protected List<String> getAllPartitionStatsFiles(List<String> table)
      throws NessieNotFoundException {
    return getAllPartitionStatsFiles(table, getNessieApi().getDefaultBranch(), null);
  }

  protected List<String> getAllPartitionStatsFiles(
      List<String> table, Branch branch, String snapshot) {
    String table1QualifiedName = String.format("%s@%s", String.join(".", table), branch.getName());
    Table icebergTable = loadTable(table1QualifiedName);
    Snapshot icebergSnapshot =
        snapshot != null
            ? icebergTable.snapshot(Long.parseLong(snapshot))
            : icebergTable.currentSnapshot();
    PartitionStatsMetadata partitionStatsMeta = icebergSnapshot.partitionStatsMetadata();
    Collection<String> partitionStatsFiles =
        partitionStatsMeta.partitionStatsFiles(icebergTable.io()).all().values();

    List<String> allStatsFiles = new ArrayList<>(partitionStatsFiles);
    allStatsFiles.add(partitionStatsMeta.metadataFileLocation());
    return allStatsFiles;
  }

  protected List<String> createTable() {
    try {
      String tableName = generateUniqueTableName();
      List<String> tablePath = Collections.singletonList(tableName);
      runSQL(createEmptyTableQuery(tablePath));
      return tablePath;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void cleanupSilently(List<String> tablePath) {
    try {
      List<String> table1Files = getFiles(tablePath, s -> true);
      getDataplaneStorage().deleteObjects(PRIMARY_BUCKET, table1Files);
      runSQL(dropTableQuery(tablePath));
    } catch (Exception e) {
      LOGGER.warn("Error while cleaning up the table", e);
    }
  }

  protected void createTableAtBranch(List<String> tablePath, String branchName) {
    try {
      runSQL(createEmptyTableQueryWithAt(tablePath, branchName));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<String> createPartitionedTable() throws Exception {
    String tableName = generateUniqueTableName();
    List<String> tablePath = Collections.singletonList(tableName);
    runSQL(createPartitionTableQuery(tablePath));
    return tablePath;
  }

  protected String placeOrphanFile(List<String> tablePath) {
    final String parquetFile1 =
        TestTools.getWorkingPath() + "/src/test/resources/vacuum/data.parquet";
    String orphanFileKeyName =
        String.join("/", tablePath) + "/metadata/" + UUID.randomUUID() + ".parquet";
    String orphanFilePath =
        String.join("/", getDataplaneStorage().getWarehousePath(), orphanFileKeyName);
    getDataplaneStorage().putObject(orphanFilePath, new File(parquetFile1));
    return orphanFileKeyName;
  }

  protected String ref(String hash) {
    return String.format("REFERENCE \"%s\"", hash);
  }

  protected void selectQuery(List<String> table, String specifier) {
    try {
      runSQL(selectStarQueryWithSpecifier(table, specifier));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void selectOnSnapshotAtRef(List<String> table, String snapshotId, String specifier)
      throws Exception {
    runSQL(useReferenceQuery(specifier));
    runSQL(selectStarOnSnapshotQuery(table, snapshotId));
  }

  protected List<String> snapshotsOnHash(List<String> table, String commitHash) {
    Reference ref = Detached.of(commitHash);
    return snapshots(table, ref);
  }

  protected List<String> snapshots(List<String> table, String refName) {
    Reference ref = null;
    try {
      ref = getNessieApi().getReference().refName(refName).get();
      return snapshots(table, ref);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected List<String> snapshots(List<String> table, Reference ref) {
    try {
      FileIO fileIO = getFileIO(getDataplanePlugin());
      String metaLoc = getMetadataLoc(table, ref);
      TableMetadata meta = TableMetadataParser.read(fileIO, metaLoc);
      return meta.snapshots().stream()
          .map(Snapshot::snapshotId)
          .map(String::valueOf)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected String newSnapshot(List<String> table) {
    try {
      runSQL(insertTableQuery(table));

      List<List<String>> result = runSqlWithResults(getLastSnapshotQuery(table));
      return result.stream().flatMap(List::stream).findFirst().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<String> getFiles(List<String> tablePath, Predicate<String> predicate) {
    return getDataplaneStorage()
        .listObjectNames(PRIMARY_BUCKET, String.join("/", tablePath), predicate)
        .collect(Collectors.toList());
  }

  protected DeleteFile addDeleteFile(Table table, String fileName) {
    final String deleteFilePath =
        TestTools.getWorkingPath() + "/src/test/resources/vacuum/" + fileName;
    final String deleteFileTablePath = String.format("%s/data/%s", table.location(), fileName);
    getDataplaneStorage().putObject(deleteFileTablePath, new File(deleteFilePath));

    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withFileSizeInBytes(1250L)
            .withRecordCount(1)
            .withPath(deleteFileTablePath)
            .build();

    table.newRowDelta().addDeletes(deleteFile).commit();
    return deleteFile;
  }

  protected DataFile appendDataFile(Table table, String fileName) {
    final String parquetFile1 =
        TestTools.getWorkingPath() + "/src/test/resources/vacuum/" + fileName;
    final String dataFileLocation = String.format("%s/data/%s", table.location(), fileName);
    getDataplaneStorage().putObject(dataFileLocation, new File(parquetFile1));

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(dataFileLocation)
            .withFileSizeInBytes(859L)
            .withRecordCount(6L)
            .build();

    table.newFastAppend().appendFile(dataFile).commit();
    return dataFile;
  }

  protected void wait1MS() {
    waitUntilAfter(System.currentTimeMillis() + 1);
  }
}
