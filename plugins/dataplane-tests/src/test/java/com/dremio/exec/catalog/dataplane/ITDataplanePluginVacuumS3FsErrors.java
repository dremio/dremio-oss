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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.dremio.exec.catalog.dataplane.test.DataplaneStorage;
import com.dremio.exec.catalog.dataplane.test.SkipForStorageType;
import com.dremio.exec.catalog.dataplane.test.TestExtendedS3AFilesystem;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;

/**
 *
 *
 * <pre>
 * This represents very important test cases in terms of VACUUM CATALOG ''
 * It should never be skipped unless we have some good alternatives to validate the same.
 * All the tests validate what are all the possible ways to delete orphan/expired files.
 * It should never impact the table state. So it's very important to validate the post-vacuum scenarios.
 * </pre>
 */
@NotThreadSafe
@SkipForStorageType(DataplaneStorage.StorageType.AZURE)
@SkipForStorageType(DataplaneStorage.StorageType.GCS_MOCK)
public class ITDataplanePluginVacuumS3FsErrors extends ITDataplanePluginVacuumTestSetup {

  @Test
  public void testAccessDeniedOnS3Table() throws Exception {
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    List<String> table1 = createPartitionedTable();
    String table1s1 = newSnapshot(table1);
    List<String> table1Files = getFiles(table1, s -> true);

    // Complete table is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(p -> p.contains(table1.get(0)));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(manifestListsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .hasMessageContaining("Access Denied");

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(manifestsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .hasMessageContaining("Access Denied");

    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(
        path ->
            getAllPartitionStatsFiles(table1, defaultBranch, table1s1).stream()
                .anyMatch(ps -> ps.contains(path)));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .hasMessageContaining("Access Denied");

    assertThat(table1Files)
        .allMatch(file -> getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, file));

    // Data files inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(dataFilesCriteria(table1));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    TestExtendedS3AFilesystem.noAccessDeniedExceptions();
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH main"));
  }

  @Test
  public void testNonTolerantErrors() {
    TestExtendedS3AFilesystem.noGenericExceptions();

    List<String> table1 = createTable();
    newSnapshot(table1);
    List<String> table1Files = getFiles(table1, s -> true);

    // Complete table is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(p -> p.contains(table1.get(0)));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(manifestListsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(manifestsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    assertThat(table1Files)
        .allMatch(file -> getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, file));

    // Data files inaccessible, error thrown during deletes is tolerable
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(dataFilesCriteria(table1));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Table is queryable
    TestExtendedS3AFilesystem.noGenericExceptions();
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH main"));
  }

  @Test
  public void testWithMultipleBatchesAndPartialErrors() throws Exception {
    final int numOfTables = 5;
    final int numOfSnapshots = 5;
    List<List<String>> tables =
        IntStream.range(0, numOfTables).mapToObj(i -> createTable()).collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));
    String skipTableName = tables.get(2).get(0);

    setTargetBatchSize("2");
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(p -> p.contains(skipTableName));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    TestExtendedS3AFilesystem.noAccessDeniedExceptions();
    resetTargetBatchSize();

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));

    tables.stream()
        .filter(t -> !t.get(0).equals(skipTableName))
        .forEach(t -> assertThat(snapshots(t, "main")).hasSize(1));

    assertThat(snapshots(ImmutableList.of(skipTableName), "main")).hasSize(6);
  }

  @Test
  public void testContinueOnMissingPartitionStatsFiles() throws Exception {
    final List<String> table = createPartitionedTable(); // Missing partition stats

    List<String> tablePartitionStatsFiles = getAllPartitionStatsFiles(table);
    newSnapshot(table);
    tablePartitionStatsFiles.addAll(getAllPartitionStatsFiles(table));

    int bucketPrefixLength = (getDataplaneStorage().getWarehousePath() + "/").length();
    Function<String, String> toRelativePath = path -> path.substring(bucketPrefixLength);
    tablePartitionStatsFiles.stream()
        .map(toRelativePath)
        .forEach(
            partitionStatsFile -> {
              getDataplaneStorage().deleteObject(PRIMARY_BUCKET, partitionStatsFile);
            });

    // Should ignore partition stats files if missing and continue
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));
  }
}
