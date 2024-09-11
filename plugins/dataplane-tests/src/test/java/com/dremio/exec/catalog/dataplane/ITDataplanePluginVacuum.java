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
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_SIZE_MB;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.NEW_FILES_GRACE_PERIOD;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.dataplane.test.DataplaneStorage;
import com.dremio.exec.catalog.dataplane.test.SkipForStorageType;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.Reference;

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
public class ITDataplanePluginVacuum extends ITDataplanePluginVacuumTestSetup {

  private SqlConverter converter;
  private SqlHandlerConfig config;

  @Test
  public void testRemoveOrphan() throws Exception {
    final List<String> tablePath = createTable(); // 1 Snapshot
    String orphanPath = placeOrphanFile(tablePath);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, orphanPath)).isFalse();
    setNessieGCDefaultCutOffPolicy("PT100M"); // only live snapshots will be retained

    // Ensure orphan is retained if creation time is recent to cut-off
    String retainedOrphanPath = placeOrphanFile(tablePath);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, retainedOrphanPath)).isTrue();
    setNessieGCDefaultCutOffPolicy("PT0S");

    cleanupSilently(tablePath);
  }

  @Test
  public void testGCDisabled() throws Exception {
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    final List<String> table2 = createTable(); // 1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);
    String table2s2 = newSnapshot(table2);

    String table1OrphanPath = placeOrphanFile(table1);
    String table2OrphanPath = placeOrphanFile(table2);

    String table1QualifiedName =
        String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1
        .updateProperties()
        .set(GC_ENABLED, Boolean.FALSE.toString())
        .set(
            TableProperties.COMMIT_NUM_RETRIES,
            "5") // Set an additional property for the change to reach table metadata.
        .commit();

    wait1MS();
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Table 1 has all snapshots retained
    assertThat(snapshots(table1, defaultBranch.getName())).contains(table1s1, table1s2);
    assertThat(snapshots(table2, defaultBranch.getName())).containsExactly(table2s2);

    // Query on older snapshot works for table1
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint1.getHash())); // not collected
    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table2, table2s1, commitPoint1.getHash())); // collected

    // Orphans cleaned for table 2 but survives for table1
    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, table1OrphanPath)).isTrue();
    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, table2OrphanPath)).isFalse();

    cleanupSilently(table1);
    cleanupSilently(table2);
  }

  @Test
  public void testExchangesInPlan() throws Exception {
    setSliceTarget("1"); // force early slicing

    final int numOfTables = 10;
    final int numOfSnapshots = 5;
    List<List<String>> tables =
        IntStream.range(0, numOfTables).mapToObj(i -> createTable()).collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));
    tables.forEach(t -> assertThat(snapshots(t, "main")).hasSize(1));

    tables.forEach(super::cleanupSilently);
    resetSliceTarget();
  }

  @Test
  public void testRemoveOrphanWithGracePeriod() throws Exception {
    final List<String> tablePath = createTable(); // 1 Snapshot
    String orphanPath = placeOrphanFile(tablePath);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, orphanPath)).isFalse();
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy("PT0S")
            .newFilesGracePeriod(Duration.ofDays(1))
            .build();
    getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();

    // Ensure orphan is retained if creation time is recent to cut-off
    String retainedOrphanPath = placeOrphanFile(tablePath);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, retainedOrphanPath)).isTrue();
    setNessieGCDefaultCutOffPolicy("PT0S");

    cleanupSilently(tablePath);
  }

  @Test
  public void testRemoveOrphanWithGracePeriodLessThan24Hrs() throws Exception {
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder()
            .defaultCutoffPolicy("PT0S")
            .newFilesGracePeriod(Duration.ofMinutes(1))
            .build();
    getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(NEW_FILES_GRACE_PERIOD);
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testNoTableFound() throws Exception {
    List<String> table1 = createTable();
    newSnapshot(table1);
    List<String> table1Files = getFiles(table1, s -> true);
    getDataplaneStorage().deleteObjects(PRIMARY_BUCKET, table1Files);

    testBuilder() // no exception thrown
        .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
        .unOrdered()
        .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
        .baselineValues(0L, 0L)
        .go();
  }

  @Test
  public void testWithMultipleBatches() throws Exception {
    final int numOfTables = 5;
    final int numOfSnapshots = 5;
    List<List<String>> tables =
        IntStream.range(0, numOfTables).mapToObj(i -> createTable()).collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));

    setTargetBatchSize("2");
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    resetTargetBatchSize();

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));
    tables.forEach(t -> assertThat(snapshots(t, "main")).hasSize(1));

    tables.forEach(super::cleanupSilently);
  }
}
