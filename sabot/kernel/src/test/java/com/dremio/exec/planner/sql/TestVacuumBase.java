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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_SIZE_MB;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_DATA_FILE_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_EQUALITY_DELETE_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_MANIFEST_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_MANIFEST_LISTS_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_PARTITION_STATS_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_POSITION_DELETE_FILES_COUNT;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addRows;
import static com.dremio.exec.store.iceberg.IcebergUtils.getPartitionStatsFiles;

import com.dremio.TestBuilder;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.Path;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.PartitionStatsMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.Assert;

/**
 * Base class for vacuum queries tests.
 *
 * <p>Note: Add tests used across all platforms here.
 */
public class TestVacuumBase extends ITDmlQueryBase {

  protected static DmlQueryTestUtils.Table newSnapshots(
      DmlQueryTestUtils.Table table, int noOfSnapshots) throws Exception {
    for (int i = 0; i < noOfSnapshots; i++) {
      table = addRows(table, 1);
    }
    return table;
  }

  protected static Table loadIcebergTable(DmlQueryTestUtils.Table table) {
    String tableName =
        table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
    return getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  protected static Set<String> collectAllFilesFromTable(Table icebergTable) {
    Set<String> files = Sets.newHashSet();
    if (icebergTable == null) {
      return files;
    }

    // Snapshots files
    Iterator<Snapshot> iterator = icebergTable.snapshots().iterator();
    while (iterator.hasNext()) {
      Snapshot snapshot = iterator.next();
      files.addAll(collectAllFilesFromSnapshot(snapshot, icebergTable.io()));
    }

    File metadataFolder = new File(icebergTable.location(), "metadata");
    // Metadata files
    int numSnapshots = Iterables.size(icebergTable.snapshots());
    for (int i = 1; i <= numSnapshots; i++) {
      String metadataFile =
          metadataFolder.getPath() + Path.SEPARATOR + String.format("v%d.metadata.json", i);
      files.add(metadataFile);
    }
    files.add(metadataFolder.getPath() + Path.SEPARATOR + "version-hint.text");

    return files;
  }

  protected static Set<String> collectDataFilesFromTable(Table icebergTable) {
    Set<String> files = Sets.newHashSet();
    if (icebergTable == null) {
      return files;
    }

    Iterator<Snapshot> iterator = icebergTable.snapshots().iterator();
    while (iterator.hasNext()) {
      Snapshot snapshot = iterator.next();
      files.addAll(collectDataFilesFromSnapshot(snapshot, icebergTable.io()));
    }

    return files;
  }

  protected static Set<String> collectAllFilesFromSnapshot(Snapshot snapshot, FileIO io) {
    Set<String> files = Sets.newHashSet();
    files.addAll(pathSet(snapshot.addedDataFiles(io)));
    files.add(snapshot.manifestListLocation());
    files.addAll(manifestPaths(snapshot.allManifests(io)));
    files.addAll(partitionStatsPaths(snapshot.partitionStatsMetadata(), io));
    return files;
  }

  protected static Set<String> collectDataFilesFromSnapshot(Snapshot snapshot, FileIO io) {
    return pathSet(snapshot.addedDataFiles(io));
  }

  protected static Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }

  protected static Set<String> manifestPaths(Iterable<ManifestFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }

  protected static Set<String> partitionStatsPaths(
      PartitionStatsMetadata partitionStatsMetadata, FileIO io) {
    Set<String> partitionStatsFiles = Sets.newHashSet();
    if (partitionStatsMetadata != null) {
      String partitionStatsMetadataLocation = partitionStatsMetadata.metadataFileLocation();
      PartitionStatsFileLocations partitionStatsLocations =
          getPartitionStatsFiles(io, partitionStatsMetadataLocation);
      if (partitionStatsLocations != null) {
        // Partition stats have metadata file and partition files.
        partitionStatsFiles.add(partitionStatsMetadataLocation);
        partitionStatsFiles.addAll(
            partitionStatsLocations.all().values().stream().collect(Collectors.toList()));
      }
    }

    return partitionStatsFiles;
  }

  protected static void validateOutputResult(
      BufferAllocator allocator,
      String query,
      Object[] args,
      DmlQueryTestUtils.Table table,
      Long[] results)
      throws Exception {
    Assert.assertEquals(6, results.length);
    new TestBuilder(allocator)
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns(
            DELETE_DATA_FILE_COUNT,
            DELETE_POSITION_DELETE_FILES_COUNT,
            DELETE_EQUALITY_DELETE_FILES_COUNT,
            DELETE_MANIFEST_FILES_COUNT,
            DELETE_MANIFEST_LISTS_COUNT,
            DELETE_PARTITION_STATS_FILES_COUNT)
        .baselineValues(results[0], results[1], results[2], results[3], results[4], results[5])
        .go();
  }

  protected static void validateRemoveOrphanFilesOutputResult(
      BufferAllocator allocator, String query, Object[] args, Long[] results) throws Exception {
    Assert.assertEquals(2, results.length);
    new TestBuilder(allocator)
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
        .baselineValues(results[0], results[1])
        .go();
  }

  protected static Table appendManifestFiles(Table icebergTable, String branch) throws IOException {
    List<ManifestFile> manifestFiles = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      ManifestFile manifestFile = writeManifestFile(icebergTable, 10);
      manifestFiles.add(manifestFile);
    }
    AppendFiles append = icebergTable.newFastAppend().toBranch(branch);
    manifestFiles.forEach(append::appendManifest);
    append.commit();
    icebergTable.refresh();
    return icebergTable;
  }
}
