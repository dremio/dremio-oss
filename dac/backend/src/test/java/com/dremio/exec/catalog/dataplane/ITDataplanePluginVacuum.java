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

import static com.dremio.PlanTestBase.testMatchingPatterns;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createPartitionTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.getLastSnapshotQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.getSnapshotIdQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarOnSnapshotQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useReferenceQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.vacuumTableQuery;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_SIZE_MB;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.NEW_FILES_GRACE_PERIOD;
import static com.google.common.base.Predicates.alwaysTrue;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionStatsMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.Reference;

import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.VacuumOutputSchema;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.VacuumCatalogHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.test.UserExceptionAssert;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * <pre>
 * This represents very important test cases in terms of VACUUM CATALOG ''
 * It should never be skipped unless we have some good alternatives to validate the same.
 * All the tests validate what are all the possible ways to delete orphan/expired files.
 * It should never impact the table state. So it's very important to validate the post-vacuum scenarios.
 * </pre>
 */
public class ITDataplanePluginVacuum extends ITDataplanePluginTestSetup {
  private final BufferAllocator allocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);

  private SqlConverter converter;
  private SqlHandlerConfig config;

  ITDataplanePluginVacuum() {
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  private void setUpForPhysicalPlan() {
    SabotContext context = getSabotContext();

    UserSession session = UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
      .setSupportComplexTypes(true)
      .build();

    final QueryContext queryContext = new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
    queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));

    converter = new SqlConverter(
      queryContext.getPlannerSettings(),
      queryContext.getOperatorTable(),
      queryContext,
      queryContext.getMaterializationProvider(),
      queryContext.getFunctionRegistry(),
      queryContext.getSession(),
      observer,
      queryContext.getSubstitutionProviderFactory(),
      queryContext.getConfig(),
      queryContext.getScanResult(),
      queryContext.getRelMetadataQuerySupplier());

    config = new SqlHandlerConfig(queryContext, converter, observer, null);
  }

  @Test
  public void testVacuumCatalogPlan() throws Exception {
    enableVacuumCatalogFF();
    setUpForPhysicalPlan();

    final List<String> table1 = createTable();

    newSnapshot(table1);

    String vacuumSQL = "VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME;
    SqlNode deleteNode = converter.parse(vacuumSQL);
    VacuumCatalogHandler vacuumCatalogHandler = new VacuumCatalogHandler();
    vacuumCatalogHandler.getPlan(config, vacuumSQL, deleteNode);
    testMatchingPatterns(vacuumCatalogHandler.getTextPlan(), new String[]{
      // We should have at least all these operators
      "IcebergOrphanFileDelete",
      "IcebergLocationFinder",
      "NessieCommitsScan",
      "IcebergManifestScan",
      "IcebergManifestListScan",
      "PartitionStatsScan",
      "TableFunction",

      // The operators should be in this order for HashExchange with LocationFinder operator
      "(?s)" +
        "IcebergLocationFinder.*" +
        "Project.*" +
        "HashToRandomExchange.*" +
        "Project.*" +
        "NessieCommitsScan.*",

      // The operators should be in this order for RoundRobinExchange with ManifestScan operator
      "(?s)" +
        "IcebergManifestScan.*" +
        "RoundRobinExchange.*" +
        "IcebergManifestListScan.*"
    });
  }

  @Test
  public void testRemoveAllHistoryInMultipleTables() throws Exception {
    enableVacuumCatalogFF();
     //only live snapshots will be retained
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    final List<String> table2 = createTable(); //1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);

    Reference commitPoint2 = getNessieClient().getReference().refName("main").get();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Assert that older snapshots are collected
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table2, "BRANCH " + defaultBranch.getName())).containsExactly(table2s1);

    // Commit 2 shows all the snapshots prior to vacuum, but expired snapshots cannot be queried
    assertThat(snapshots(table1, ref(commitPoint2.getHash()))).contains(table1s1, table1s2);
    assertThatThrownBy(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table2, table2s1, commitPoint2.getHash())); // not collected because it's also the latest

    // Table 1 is completely collected at Commit 1. Table 2 is in the same state as on branch head, hence queryable.
    assertThatThrownBy(() -> selectQuery(table1, ref(commitPoint1.getHash())));
    assertDoesNotThrow(() -> selectQuery(table2, ref(commitPoint1.getHash())));
    resetVacuumCatalogFF();
  }

  @Test
  public void testRemoveOrphan() throws Exception {
    enableVacuumCatalogFF();

    final List<String> tablePath = createTable(); //1 Snapshot
    String orphanPath = placeOrphanFile(tablePath);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, orphanPath)).isFalse();
    setNessieGCDefaultCutOffPolicy("PT100M"); //only live snapshots will be retained

    // Ensure orphan is retained if creation time is recent to cut-off
    String retainedOrphanPath = placeOrphanFile(tablePath);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, retainedOrphanPath)).isTrue();
    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }

  @Test
  public void testNoExpiryOnTag() throws Exception {
    enableVacuumCatalogFF();
    String tagName = "Tag1";
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);

    runSQL(createTagQuery(tagName, defaultBranch.getName()));
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "TAG " + tagName));

    // Assert that snapshot history is collected on branch, but unchanged on tag
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table1, "TAG " + tagName)).contains(table1s1, table1s2);

    resetVacuumCatalogFF();
  }

  @Test
  public void testBranchHeadRetained() throws Exception {
    enableVacuumCatalogFF();
    String branchName = "branch1";
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    String table1s1 = newSnapshot(table1);
    runSQL(createBranchAtBranchQuery(branchName, defaultBranch.getName()));

    useBranchQuery(defaultBranch.getName());
    String table1s2 = newSnapshot(table1);
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branchName));

    // Assert that snapshot history is retained at branch head
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table1, "BRANCH " + branchName)).containsExactly(table1s1);

    resetVacuumCatalogFF();
  }

  @Test
  public void testVacuumTableNotSupported() throws Exception {
    enableVacuumCatalogFF();

    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    final long expireSnapshotTimestamp = System.currentTimeMillis();

    // Test ExpireSnapshots query
    UserExceptionAssert.assertThatThrownBy(() ->
        test(vacuumTableQuery(tablePath, expireSnapshotTimestamp)))
      .hasErrorType(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION)
      .hasMessageContaining("VACUUM TABLE command is not supported for this source");

    // Cleanup
    runSQL(dropTableQuery(tablePath));

    resetVacuumCatalogFF();
  }

  @Test
  public void testGCDisabled() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    final List<String> table2 = createTable(); //1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);
    String table2s2 = newSnapshot(table2);

    String table1OrphanPath = placeOrphanFile(table1);
    String table2OrphanPath = placeOrphanFile(table2);

    String table1QualifiedName = String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1.updateProperties()
        .set(GC_ENABLED, Boolean.FALSE.toString())
        .set(TableProperties.COMMIT_NUM_RETRIES, "5") // Set an additional property for the change to reach table metadata.
        .commit();

    waitUntilAfter(1);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Table 1 has all snapshots retained
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).contains(table1s1, table1s2);
    assertThat(snapshots(table2, "BRANCH " + defaultBranch.getName())).containsExactly(table2s2);

    // Query on older snapshot works for table1
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint1.getHash())); // not collected
    assertThatThrownBy(() -> selectOnSnapshotAtRef(table2, table2s1, commitPoint1.getHash())); // collected

    // Orphans cleaned for table 2 but survives for table1
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, table1OrphanPath)).isTrue();
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, table2OrphanPath)).isFalse();

    resetVacuumCatalogFF();
  }

  @Test
  public void testGCDisabledOnOneBranchOnly() throws Exception {
    enableVacuumCatalogFF();

    String branchName = "branch1";
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    String table1s1 = newSnapshot(table1);
    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();
    String table1s2 = newSnapshot(table1);
    String table1OrphanPath = placeOrphanFile(table1);

    runSQL(createBranchAtBranchQuery(branchName, defaultBranch.getName()));

    // Disable GC on one branch
    String table1QualifiedName = String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1.updateProperties()
      .set(GC_ENABLED, Boolean.FALSE.toString())
      .set(TableProperties.COMMIT_NUM_RETRIES, "5") // Set an additional property for the change to reach table metadata.
      .commit();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch heads
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branchName));

    // Table 1 has all snapshots retained on main, but expiry ran on branch1
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).contains(table1s1, table1s2);
    assertThat(snapshots(table1, "BRANCH " + branchName)).doesNotContain(table1s1);

    // Query on older commit works for table1@main
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint1.getHash())); // not collected

    // orphans are collected if at-least one live commit has gc.enabled=true for this table
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, table1OrphanPath)).isFalse();

    resetVacuumCatalogFF();
  }

  @Test
  public void testVacuumOnPartitionedTable() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    List<String> table1 = createPartitionedTable();
    String table1s1 = newSnapshot(table1);
    List<String> t1s1Stats = getAllPartitionStatsFiles(table1, defaultBranch, table1s1);

    String table1s2 = newSnapshot(table1);
    List<String> t1s2Stats = getAllPartitionStatsFiles(table1, defaultBranch, table1s2);

    waitUntilAfter(1);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch heads
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).doesNotContain(table1s1).contains(table1s2);

    int bucketPrefixLength = ("s3://" + BUCKET_NAME + "/").length();
    Function<String, String> toRelativePath = path -> path.substring(bucketPrefixLength);

    t1s1Stats.stream().map(toRelativePath).forEach(f -> assertThat(getS3Client().doesObjectExist(BUCKET_NAME, f)).isFalse());
    t1s2Stats.stream().map(toRelativePath).forEach(f -> assertThat(getS3Client().doesObjectExist(BUCKET_NAME, f)).isTrue());
    resetVacuumCatalogFF();
  }

  @Test
  public void testVacuumOnEmptyCatalog() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    createBranchAtBranchQuery("branch1", defaultBranch.getName());

    testBuilder()
      .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
      .ordered()
      .baselineColumns(VacuumOutputSchema.DELETED_FILES_COUNT, VacuumOutputSchema.DELETED_FILES_SIZE_MB)
      .baselineValues(0L, 0L)
      .go();
    resetVacuumCatalogFF();
  }

  @Test
  public void testTableOverrideMinSnapshotsHistory() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();
    String table1s3 = newSnapshot(table1);

    // Set MIN_SNAPSHOTS_TO_KEEP property
    String table1QualifiedName = String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1.updateProperties().set(GC_ENABLED, "true").set(MIN_SNAPSHOTS_TO_KEEP, "2").commit();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));

    // Assert that 2 snapshots are retained as per table property, but the prior ones are expired
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName()))
      .contains(table1s2, table1s3).doesNotContain(table1s1);

    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint1.getHash())); // Not collected

    resetVacuumCatalogFF();
  }

  @Test
  public void testTableOverrideCutoffTime() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();
    String table1s3 = newSnapshot(table1);

    // Setup snapshot age to retain in table property
    String table1QualifiedName = String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1.updateProperties().set(GC_ENABLED, "true")
        .set(MAX_SNAPSHOT_AGE_MS, String.valueOf(TimeUnit.HOURS.toMillis(1))) // all snapshots will get retained
        .commit();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));

    // Assert that all snapshots are retained
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).contains(table1s1, table1s2, table1s3);
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint1.getHash())); // Not collected

    setNessieGCDefaultCutOffPolicy("PT10M");

    // Test other way round, default policy is conservative whereas table level policy is aggressive
    icebergTable1.updateProperties().set(GC_ENABLED, "true").set(MAX_SNAPSHOT_AGE_MS, "1").commit();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).contains(table1s1, table1s2, table1s3);
    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }

  @Test
  public void testExchangesInPlan() throws Exception {
    enableVacuumCatalogFF();

    setSliceTarget("1"); // force early slicing

    final int numOfTables = 10;
    final int numOfSnapshots = 5;
    List<List<String>> tables = IntStream.range(0,numOfTables).mapToObj(i -> createTable())
        .collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));
    tables.forEach(t -> assertThat(snapshots(t, "BRANCH main")).hasSize(1));

    resetSliceTarget();
    resetVacuumCatalogFF();
  }

  @Test
  public void testDifferentBranchTablesWithSameName() throws Exception {
    enableVacuumCatalogFF();

    String branch1 = "diffbranch1";
    String branch2 = "diffbranch2";
    runSQL(createBranchAtBranchQuery(branch1, "main"));
    runSQL(createBranchAtBranchQuery(branch2, "main"));

    useBranchQuery("main");
    final List<String> table1 = createTable();
    final List<String> table2 = createTable();
    newSnapshot(table1);

    createTableAtBranch(table1, branch1);
    createTableAtBranch(table2, branch1);

    createTableAtBranch(table1, branch2);
    createTableAtBranch(table2, branch2);

    useBranchQuery(branch2);
    newSnapshot(table1);
    newSnapshot(table2);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH main"));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH main"));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branch1));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + branch1));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branch2));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + branch2));

    resetVacuumCatalogFF();
  }

  @Test
  public void testRemoveAllHistoryInMultipleTablesUsingNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    Branch defaultBranch = getNessieClient().getDefaultBranch();

    final List<String> table1 = createTable(); //1 Snapshot
    final List<String> table2 = createTable(); //1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieClient().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);

    Reference commitPoint2 = getNessieClient().getReference().refName("main").get();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Assert that older snapshots are collected
    assertThat(snapshots(table1, "BRANCH " + defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table2, "BRANCH " + defaultBranch.getName())).containsExactly(table2s1);

    // Commit 2 shows all the snapshots prior to vacuum, but expired snapshots cannot be queried
    assertThat(snapshots(table1, ref(commitPoint2.getHash()))).contains(table1s1, table1s2);
    assertThatThrownBy(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table2, table2s1, commitPoint2.getHash())); // not collected because it's also the latest

    // Table 1 is completely collected at Commit 1. Table 2 is in the same state as on branch head, hence queryable.
    assertThatThrownBy(() -> selectQuery(table1, ref(commitPoint1.getHash())));
    assertDoesNotThrow(() -> selectQuery(table2, ref(commitPoint1.getHash())));
    resetVacuumCatalogFF();
  }

  @Test
  public void testNoneCutoffPolicyWithNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    setNessieGCDefaultCutOffPolicy("NONE");
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));
    resetVacuumCatalogFF();
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testNullCutoffPolicyWithNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().build();
    getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).isInstanceOf(UserException.class).hasMessageContaining(DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE);
    resetVacuumCatalogFF();
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testNumericCutOffPolicyNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    setNessieGCDefaultCutOffPolicy("2");
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).isInstanceOf(UserException.class)
      .hasMessageContaining(DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE);
    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }

  @Test
  public void testSnapshotsAfterVacuumWithCutoffPolicyAtNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    final List<String> table1 = createTable();

    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    String table1s3 = newSnapshot(table1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    String table1s4 = newSnapshot(table1);
    Reference commitPoint = getNessieClient().getReference().refName("main").get();

    assertThatThrownBy(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint.getHash())); // not collected
    assertThatThrownBy(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint.getHash())); // not collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s3, commitPoint.getHash())); //collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s4, commitPoint.getHash())); //collected
    resetVacuumCatalogFF();
  }

  @Test
  public void testRemoveOrphanWithGracePeriod() throws Exception {
    enableVacuumCatalogFF();

    final List<String> tablePath = createTable(); //1 Snapshot
    String orphanPath = placeOrphanFile(tablePath);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, orphanPath)).isFalse();
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("PT0S").newFilesGracePeriod(Duration.ofDays(1)).build();
    getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();

    // Ensure orphan is retained if creation time is recent to cut-off
    String retainedOrphanPath = placeOrphanFile(tablePath);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    assertThat(getS3Client().doesObjectExist(BUCKET_NAME, retainedOrphanPath)).isTrue();
    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }

  @Test
  public void testRemoveOrphanWithGracePeriodLessThan24Hrs() throws Exception {
    enableVacuumCatalogFF();
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy("PT0S").newFilesGracePeriod(Duration.ofMinutes(1)).build();
    getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).isInstanceOf(UserException.class)
      .hasMessageContaining(NEW_FILES_GRACE_PERIOD);
    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }

  @Test
  public void testInstantTimeNessieConfig() throws Exception {
    enableVacuumCatalogFF();
    final List<String> table1 = createTable(); //1 Snapshot

    String table1s1 = newSnapshot(table1);
    waitUntilAfter(1);
    //all the snapshots should be expired before this time.
    String instantTime = Instant.now().toString();
    waitUntilAfter(1);
    String table1s2 = newSnapshot(table1);
    String table1s3 = newSnapshot(table1);
    setNessieGCDefaultCutOffPolicy(instantTime);
    Reference commitPoint2 = getNessieClient().getReference().refName("main").get();
    waitUntilAfter(1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThatThrownBy(() -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(() -> selectOnSnapshotAtRef(table1, table1s3, commitPoint2.getHash())); //  collected

    setNessieGCDefaultCutOffPolicy("PT0S");
    resetVacuumCatalogFF();
  }
  @Test
  public void testWithFolder() throws Exception {
    enableVacuumCatalogFF();


    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Create a subFolder within Nessie
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    //Create table under the subfolder
    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    resetVacuumCatalogFF();
  }

  @Test
  public void testV2TableWithPositionalDeletes() throws Exception {
    enableVacuumCatalogFF();

    Map<String, String> tableProps = ImmutableMap.of(GC_ENABLED, "true", FORMAT_VERSION, "2", DELETE_MODE, "merge-on-read");
    Table table = createTable("table_with_positional_deletes@main", tableProps);

    DataFile dataFile = appendDataFile(table, "data.parquet");
    DeleteFile deleteFile = addDeleteFile(table, "positional_delete.parquet");
    table.updateProperties().set(GC_ENABLED, "true").set(COMMIT_NUM_RETRIES, "2").commit(); // Reset GC enabled as it is defaulted in NessieCatalog

    AmazonS3URI dataFileUri = new AmazonS3URI(dataFile.path().toString());
    AmazonS3URI deleteFileUri = new AmazonS3URI(deleteFile.path().toString());

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getS3Client().doesObjectExist(dataFileUri.getBucket(), dataFileUri.getKey())).isTrue();
    assertThat(getS3Client().doesObjectExist(deleteFileUri.getBucket(), deleteFileUri.getKey())).isTrue();

    resetVacuumCatalogFF();
  }

  @Test
  public void testMissingMetadataError() throws Exception {
    enableVacuumCatalogFF();

    final List<String> table1 = createTable(); // Missing metadata

    newSnapshot(table1);

    List<String> table1SurvivingFiles = getFiles(table1, alwaysTrue());
    List<String> table1MetadataFiles = getFiles(table1, s -> s.getKey().endsWith(".json"));
    table1SurvivingFiles.removeIf(path -> path.endsWith(".json"));
    table1MetadataFiles.forEach(metadataFile -> getS3Client().deleteObject(BUCKET_NAME, metadataFile));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    table1SurvivingFiles.forEach(
      file -> assertThat(getS3Client().doesObjectExist(BUCKET_NAME, file)).isTrue());

    testBuilder()
      .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
      .unOrdered()
      .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
      .baselineValues(0L, 0L)
      .go();

    resetVacuumCatalogFF();
  }

  @Test
  public void testMissingManifestListError() throws Exception {
    enableVacuumCatalogFF();

    final List<String> table = createTable(); // Missing manifest list

    newSnapshot(table);

    List<String> tableSurvivingFiles = getFiles(table, alwaysTrue());
    List<String> tableManifestLists = getFiles(table, k -> manifestListsCriteria(table).test(k.getKey()));
    tableSurvivingFiles.removeAll(tableManifestLists);

//     Respective manifests and data files are unreachable, hence will be considered orphan and deleted
    tableManifestLists.forEach(manifestListFile -> getS3Client().deleteObject(BUCKET_NAME, manifestListFile));

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    resetVacuumCatalogFF();
  }

  @Test
  public void testMissingManifestError() throws Exception {
    enableVacuumCatalogFF();

    final List<String> table = createTable(); // Missing manifest

    newSnapshot(table);

    List<String> tableManifestFiles = getFiles(table, k -> manifestsCriteria(table).test(k.getKey()));
    tableManifestFiles.forEach(manifestFile -> getS3Client().deleteObject(BUCKET_NAME, manifestFile));

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    resetVacuumCatalogFF();
  }

  @Test
  public void testContinueOnMissingPartitionStatsFiles() throws Exception {
    enableVacuumCatalogFF();

    final List<String> table = createPartitionedTable(); // Missing partition stats

    List<String> tablePartitionStatsFiles = getAllPartitionStatsFiles(table);
    newSnapshot(table);
    tablePartitionStatsFiles.addAll(getAllPartitionStatsFiles(table));

    int bucketPrefixLength = ("s3://" + BUCKET_NAME + "/").length();
    Function<String, String> toRelativePath = path -> path.substring(bucketPrefixLength);
    tablePartitionStatsFiles.stream().map(toRelativePath)
      .forEach(partitionStatsFile -> {
        getS3Client().deleteObject(BUCKET_NAME, partitionStatsFile);
      });

    // Should ignore partition stats files if missing and continue
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    resetVacuumCatalogFF();
  }

  @Test
  public void testNoTableFound() throws Exception {
    enableVacuumCatalogFF();

    List<String> table1 = createTable();
    newSnapshot(table1);
    List<String> table1Files = getFiles(table1, alwaysTrue());
    getS3Client().deleteObjects(new DeleteObjectsRequest(BUCKET_NAME).withKeys(
      table1Files.toArray(new String[table1Files.size()])));

    testBuilder() // no exception thrown
      .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
      .unOrdered()
      .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
      .baselineValues(0L, 0L)
      .go();

    resetVacuumCatalogFF();
  }

  @Test
  public void testAccessDeniedOnS3Table() throws Exception {
    enableVacuumCatalogFF();

    Branch defaultBranch = getNessieClient().getDefaultBranch();

    List<String> table1 = createPartitionedTable();
    String table1s1 = newSnapshot(table1);
    List<String> table1Files = getFiles(table1, alwaysTrue());

    // Complete table is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(p -> p.contains(table1.get(0)));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(manifestListsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).hasMessageContaining("Access Denied");

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(manifestsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).hasMessageContaining("Access Denied");

    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(path ->
      getAllPartitionStatsFiles(table1, defaultBranch, table1s1).stream().anyMatch(ps -> ps.contains(path)));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)).hasMessageContaining("Access Denied");

    table1Files.forEach(file -> assertThat(getS3Client().doesObjectExist(BUCKET_NAME, file)).isTrue());

    // Data files inaccessible
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(dataFilesCriteria(table1));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    TestExtendedS3AFilesystem.noAccessDeniedExceptions();
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH main"));

    resetVacuumCatalogFF();
  }

  @Test
  public void testNonTolerantErrors() {
    enableVacuumCatalogFF();

    TestExtendedS3AFilesystem.noGenericExceptions();

    List<String> table1 = createTable();
    newSnapshot(table1);
    List<String> table1Files = getFiles(table1, alwaysTrue());

    // Complete table is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(p -> p.contains(table1.get(0)));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest list is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(manifestListsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Manifest is inaccessible
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(manifestsCriteria(table1));
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    table1Files.forEach(file -> assertThat(getS3Client().doesObjectExist(BUCKET_NAME, file)).isTrue());

    // Data files inaccessible, error thrown during deletes is tolerable
    TestExtendedS3AFilesystem.setGenericExceptionOnPaths(dataFilesCriteria(table1));
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    // Table is queryable
    TestExtendedS3AFilesystem.noGenericExceptions();
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH main"));

    resetVacuumCatalogFF();
  }

  @Test
  public void testWithMultipleBatches() throws Exception {
    enableVacuumCatalogFF();

    final int numOfTables = 5;
    final int numOfSnapshots = 5;
    List<List<String>> tables = IntStream.range(0,numOfTables).mapToObj(i -> createTable())
      .collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));
    String skipTableName = tables.get(2).get(0);

    setTargetBatchSize("2");
    TestExtendedS3AFilesystem.setAccessDeniedExceptionOnPaths(p -> p.contains(skipTableName));
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    TestExtendedS3AFilesystem.noAccessDeniedExceptions();
    resetTargetBatchSize();

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));

    tables.stream().filter(t -> !t.get(0).equals(skipTableName))
      .forEach(t -> assertThat(snapshots(t, "BRANCH main")).hasSize(1));

    assertThat(snapshots(ImmutableList.of(skipTableName), "BRANCH main")).hasSize(6);

    resetVacuumCatalogFF();
  }

  @Test
  public void testWithMultipleBatchesAndPartialErrors() throws Exception {
    enableVacuumCatalogFF();

    final int numOfTables = 5;
    final int numOfSnapshots = 5;
    List<List<String>> tables = IntStream.range(0,numOfTables).mapToObj(i -> createTable())
      .collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));

    setTargetBatchSize("2");
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    resetTargetBatchSize();

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));
    tables.forEach(t -> assertThat(snapshots(t, "BRANCH main")).hasSize(1));

    resetVacuumCatalogFF();
  }

  private DeleteFile addDeleteFile(Table table, String fileName) {
    final String deleteFilePath = TestTools.getWorkingPath() + "/src/test/resources/vacuum/" + fileName;
    final String deleteFileTablePath = String.format("%s/data/%s", table.location(), fileName);
    final AmazonS3URI deleteFileUri = new AmazonS3URI(deleteFileTablePath);
    getS3Client().putObject(deleteFileUri.getBucket(), deleteFileUri.getKey(), new File(deleteFilePath));

    DeleteFile deleteFile = FileMetadata.deleteFileBuilder(table.spec())
      .ofPositionDeletes()
      .withFileSizeInBytes(1250L)
      .withRecordCount(1)
      .withPath(deleteFileTablePath)
      .build();

    table.newRowDelta().addDeletes(deleteFile).commit();
    return deleteFile;
  }

  private DataFile appendDataFile(Table table, String fileName) {
    final String parquetFile1 = TestTools.getWorkingPath() + "/src/test/resources/vacuum/" + fileName;
    final String dataFileLocation = String.format("%s/data/%s", table.location(), fileName);
    final AmazonS3URI dataFileUri = new AmazonS3URI(dataFileLocation);
    getS3Client().putObject(dataFileUri.getBucket(), dataFileUri.getKey(), new File(parquetFile1));

    DataFile dataFile = DataFiles.builder(table.spec())
      .withPath(dataFileLocation)
      .withFileSizeInBytes(859L)
      .withRecordCount(6L)
      .build();

    table.newFastAppend().appendFile(dataFile).commit();
    return dataFile;
  }

  private String getMetadataLoc(List<String> table) throws NessieNotFoundException {
    ContentKey key = ContentKey.of(table);
    return ((IcebergTable)getNessieClient().getContent().key(key)
      .reference(getNessieClient().getDefaultBranch()).get().get(key))
      .getMetadataLocation();
  }

  private Predicate<String> manifestListsCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && path.contains("snap") && path.endsWith("avro");
  }

  private Predicate<String> manifestsCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && !path.contains("snap") && path.endsWith("avro");
  }

  private Predicate<String> dataFilesCriteria(List<String> table) {
    return path -> path.contains(table.get(0)) && path.endsWith("parquet");
  }

  private List<String> getAllPartitionStatsFiles(List<String> table) throws NessieNotFoundException {
    return getAllPartitionStatsFiles(table, getNessieClient().getDefaultBranch(), null);
  }

  private List<String> getAllPartitionStatsFiles(List<String> table, Branch branch, String snapshot) {
    String table1QualifiedName = String.format("%s@%s", String.join(".", table), branch.getName());
    Table icebergTable = loadTable(table1QualifiedName);
    Snapshot icebergSnapshot = snapshot != null ? icebergTable.snapshot(Long.parseLong(snapshot)) : icebergTable.currentSnapshot();
    PartitionStatsMetadata partitionStatsMeta = icebergSnapshot.partitionStatsMetadata();
    Collection<String> partitionStatsFiles = partitionStatsMeta.partitionStatsFiles(icebergTable.io()).all().values();

    List<String> allStatsFiles = new ArrayList<>(partitionStatsFiles);
    allStatsFiles.add(partitionStatsMeta.metadataFileLocation());
    return allStatsFiles;
  }

  private List<String> createTable() {
    try {
      String tableName = generateUniqueTableName();
      List<String> tablePath = Collections.singletonList(tableName);
      runSQL(createEmptyTableQuery(tablePath));
      return tablePath;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createTableAtBranch(List<String> tablePath, String branchName) {
    try {
      runSQL(createEmptyTableQueryWithAt(tablePath, branchName));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> createPartitionedTable() throws Exception {
    String tableName = generateUniqueTableName();
    List<String> tablePath = Collections.singletonList(tableName);
    runSQL(createPartitionTableQuery(tablePath));
    return tablePath;
  }

  private String placeOrphanFile(List<String> tablePath) {
    final String parquetFile1 = TestTools.getWorkingPath() + "/src/test/resources/vacuum/data.parquet";
    String orphanFileKeyName = String.join("/", tablePath) + "/metadata/" + UUID.randomUUID() + ".parquet";
    getS3Client().putObject(BUCKET_NAME, orphanFileKeyName, new File(parquetFile1));
    return orphanFileKeyName;
  }

  private String ref(String hash) {
    return String.format("REFERENCE \"%s\"", hash);
  }

  private void selectQuery(List<String> table, String specifier) {
    try {
      runSQL(selectStarQueryWithSpecifier(table, specifier));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void selectOnSnapshotAtRef(List<String> table, String snapshotId, String specifier) throws Exception {
    runSQL(useReferenceQuery(specifier));
    runSQL(selectStarOnSnapshotQuery(table, snapshotId));
  }

  private List<String> snapshots(List<String> table, String specifier) {
    try {
      List<QueryDataBatch> resultBatches = testRunAndReturn(QueryType.SQL,
          getSnapshotIdQueryWithSpecifier(table, specifier));
      return Arrays.asList(readResult(resultBatches).split("\n"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String newSnapshot(List<String> table) {
    try {
      runSQL(insertTableQuery(table));

      List<QueryDataBatch> resultBatches = testRunAndReturn(QueryType.SQL,
          getLastSnapshotQuery(table));
      return readResult(resultBatches).replace("\n", "").trim();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String readResult(List<QueryDataBatch> resultBatches) {
    Preconditions.checkState(resultBatches.size() == 1); // Not expecting tests to create multi-batch results
    QueryDataBatch result = resultBatches.get(0);
    final RecordBatchLoader loader = new RecordBatchLoader(allocator);
    loader.load(result.getHeader().getDef(), result.getData());
    StringBuilder resultBuilder = new StringBuilder();
    VectorUtil.appendVectorAccessibleContent(loader, resultBuilder, "", false);
    return resultBuilder.toString();
  }

  private List<String> getFiles(List<String> tablePath, Predicate<? super S3ObjectSummary> predicate) {
    return getS3Client()
      .listObjects(BUCKET_NAME, String.join("/", tablePath))
      .getObjectSummaries()
      .stream()
      .filter(predicate)
      .map(S3ObjectSummary::getKey)
      .collect(Collectors.toList());
  }
}
