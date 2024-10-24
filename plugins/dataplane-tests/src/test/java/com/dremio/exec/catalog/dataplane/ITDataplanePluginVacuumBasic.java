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
import static com.dremio.exec.ExecConstants.ENABLE_VACUUM_CATALOG_BRIDGE_OPERATOR;
import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.vacuumTableQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.getSubPathFromNessieTableContent;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_SIZE_MB;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE;
import static com.dremio.services.nessie.validation.GarbageCollectorConfValidator.DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE;
import static org.apache.iceberg.DremioTableProperties.NESSIE_GC_ENABLED;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.VacuumOutputSchema;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.VacuumCatalogHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
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
public class ITDataplanePluginVacuumBasic extends ITDataplanePluginVacuumTestSetup {

  private SqlConverter converter;
  private SqlHandlerConfig config;

  private void setUpForPhysicalPlan() {
    SabotContext context = getSabotContext();

    UserSession session =
        UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
                getSabotContext().getOptionManager())
            .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
            .withCredentials(
                UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
            .setSupportComplexTypes(true)
            .build();

    final QueryContext queryContext =
        new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
    queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
    final AttemptObserver observer =
        new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));

    converter =
        new SqlConverter(
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
  public void testLegacyVacuumCatalogPlan() throws Exception {
    try (AutoCloseable c = withSystemOption(ENABLE_VACUUM_CATALOG_BRIDGE_OPERATOR, false)) {
      setUpForPhysicalPlan();

      final List<String> table1 = createTable();

      newSnapshot(table1);

      String vacuumSQL = "VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME;
      SqlNode deleteNode = converter.parse(vacuumSQL);
      VacuumCatalogHandler vacuumCatalogHandler = new VacuumCatalogHandler();
      vacuumCatalogHandler.getPlan(config, vacuumSQL, deleteNode);
      testMatchingPatterns(
          vacuumCatalogHandler.getTextPlan(),
          new String[] {
            // We should have at least all these operators
            "IcebergOrphanFileDelete",
            "IcebergLocationFinder",
            "NessieCommitsScan",
            "IcebergManifestScan",
            "IcebergManifestListScan",
            "PartitionStatsScan",
            "TableFunction",

            // The operators should be in this order for HashExchange with LocationFinder operator
            "(?s)"
                + "IcebergLocationFinder.*"
                + "Project.*"
                + "HashToRandomExchange.*"
                + "Project.*"
                + "NessieCommitsScan.*",

            // The operators should be in this order for RoundRobinExchange with ManifestScan
            // operator
            "(?s)"
                + "IcebergManifestScan.*"
                + "HashToRandomExchange.*"
                + "IcebergManifestListScan.*"
          });

      cleanupSilently(table1);
    }
  }

  @Test
  public void testVacuumCatalogPlanWithBridgeOperator() throws Exception {
    try (AutoCloseable c = withSystemOption(ENABLE_VACUUM_CATALOG_BRIDGE_OPERATOR, true)) {
      setUpForPhysicalPlan();

      final List<String> table1 = createTable();

      newSnapshot(table1);

      String vacuumSQL = "VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME;
      SqlNode deleteNode = converter.parse(vacuumSQL);
      VacuumCatalogHandler vacuumCatalogHandler = new VacuumCatalogHandler();
      vacuumCatalogHandler.getPlan(config, vacuumSQL, deleteNode);
      testMatchingPatterns(
          vacuumCatalogHandler.getTextPlan(),
          new String[] {
            // We should have at least all these operators
            "IcebergOrphanFileDelete",
            "IcebergLocationFinder",
            "NessieCommitsScan",
            "IcebergManifestScan",
            "IcebergManifestListScan",
            "PartitionStatsScan",
            "TableFunction",

            // The operators should be in this order for HashExchange with LocationFinder operator
            "(?s)"
                + "IcebergLocationFinder.*"
                + "Project.*"
                + "HashToRandomExchange.*"
                + "Project.*"
                + "BridgeExchange.*"
                + "NessieCommitsScan.*",

            // The operators should be in this order for RoundRobinExchange with ManifestScan
            // operator
            "(?s)"
                + "IcebergManifestScan.*"
                + "HashToRandomExchange.*"
                + "IcebergManifestListScan.*"
                + "BridgeReader.*"
          });

      cleanupSilently(table1);
    }
  }

  @Test
  public void testRemoveAllHistoryInMultipleTables() throws Exception {
    // only live snapshots will be retained
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    final List<String> table2 = createTable(); // 1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieApi().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);

    Reference commitPoint2 = getNessieApi().getReference().refName("main").get();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Assert that older snapshots are collected
    assertThat(snapshots(table1, defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table2, defaultBranch.getName())).containsExactly(table2s1);

    // Commit 2 shows all the snapshots prior to vacuum, but expired snapshots cannot be queried
    assertThat(snapshotsOnHash(table1, commitPoint2.getHash())).contains(table1s1, table1s2);
    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () ->
            selectOnSnapshotAtRef(
                table2,
                table2s1,
                commitPoint2.getHash())); // not collected because it's also the latest

    // Table 1 is completely collected at Commit 1. Table 2 is in the same state as on branch head,
    // hence queryable.
    assertThatThrownBy(() -> selectQuery(table1, ref(commitPoint1.getHash())));
    assertDoesNotThrow(() -> selectQuery(table2, ref(commitPoint1.getHash())));

    cleanupSilently(table1);
    cleanupSilently(table2);
  }

  @Test
  public void testNoExpiryOnTag() throws Exception {
    String tagName = "Tag1";
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);

    runSQL(createTagQuery(tagName, defaultBranch.getName()));
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "TAG " + tagName));

    // Assert that snapshot history is collected on branch, but unchanged on tag
    assertThat(snapshots(table1, defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table1, tagName)).contains(table1s1, table1s2);

    cleanupSilently(table1);
  }

  @Test
  public void testBranchHeadRetained() throws Exception {
    String branchName = "branch1";
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    String table1s1 = newSnapshot(table1);
    runSQL(createBranchAtBranchQuery(branchName, defaultBranch.getName()));

    useBranchQuery(defaultBranch.getName());
    String table1s2 = newSnapshot(table1);
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branchName));

    // Assert that snapshot history is retained at branch head
    assertThat(snapshots(table1, defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table1, branchName)).containsExactly(table1s1);

    cleanupSilently(table1);
  }

  @Test
  public void testVacuumTableNotSupported() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    final long expireSnapshotTimestamp = System.currentTimeMillis();

    // Test ExpireSnapshots query
    UserExceptionAssert.assertThatThrownBy(
            () -> test(vacuumTableQuery(tablePath, expireSnapshotTimestamp)))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("VACUUM TABLE command is not supported for this source");

    // Cleanup
    cleanupSilently(tablePath);
  }

  @Test
  public void testGCDisabledOnOneBranchOnly() throws Exception {
    String branchName = "branch1";
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    String table1s1 = newSnapshot(table1);
    Reference commitPoint1 = getNessieApi().getReference().refName("main").get();
    String table1s2 = newSnapshot(table1);
    String table1OrphanPath =
        placeOrphanFile(getSubPathFromNessieTableContent(table1, DEFAULT_BRANCH_NAME, this));

    runSQL(createBranchAtBranchQuery(branchName, defaultBranch.getName()));

    // Disable GC on one branch
    String table1QualifiedName =
        String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1
        .updateProperties()
        .set(NESSIE_GC_ENABLED, Boolean.FALSE.toString())
        .set(
            TableProperties.COMMIT_NUM_RETRIES,
            "5") // Set an additional property for the change to reach table metadata.
        .commit();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch heads
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + branchName));

    // Table 1 has all snapshots retained on main, but expiry ran on branch1
    assertThat(snapshots(table1, defaultBranch.getName())).contains(table1s1, table1s2);
    assertThat(snapshots(table1, branchName)).doesNotContain(table1s1);

    // Query on older commit works for table1@main
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint1.getHash())); // not collected

    // orphans are collected if at-least one live commit has gc.enabled=true for this table
    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, table1OrphanPath)).isFalse();

    cleanupSilently(getSubPathFromNessieTableContent(table1, DEFAULT_BRANCH_NAME, this));
  }

  @Test
  public void testVacuumOnPartitionedTable() throws Exception {
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    List<String> table1 = createPartitionedTable();
    String table1s1 = newSnapshot(table1);
    List<String> t1s1Stats = getAllPartitionStatsFiles(table1, defaultBranch, table1s1);

    String table1s2 = newSnapshot(table1);
    List<String> t1s2Stats = getAllPartitionStatsFiles(table1, defaultBranch, table1s2);

    wait1MS();
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch heads
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertThat(snapshots(table1, defaultBranch.getName()))
        .doesNotContain(table1s1)
        .contains(table1s2);

    t1s1Stats.stream()
        .forEach(
            f -> assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, f)).isFalse());
    t1s2Stats.stream()
        .forEach(
            f -> assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, f)).isTrue());

    cleanupSilently(table1);
  }

  @Test
  public void testVacuumOnEmptyCatalog() throws Exception {
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    createBranchAtBranchQuery("branch1", defaultBranch.getName());

    testBuilder()
        .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
        .ordered()
        .baselineColumns(
            VacuumOutputSchema.DELETED_FILES_COUNT, VacuumOutputSchema.DELETED_FILES_SIZE_MB)
        .baselineValues(0L, 0L)
        .go();
  }

  @Test
  public void testTableOverrideMinSnapshotsHistory() throws Exception {
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    Reference commitPoint1 = getNessieApi().getReference().refName("main").get();
    String table1s3 = newSnapshot(table1);

    // Set MIN_SNAPSHOTS_TO_KEEP property
    String table1QualifiedName =
        String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(MIN_SNAPSHOTS_TO_KEEP, "2")
        .commit();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));

    // Assert that 2 snapshots are retained as per table property, but the prior ones are expired
    assertThat(snapshots(table1, defaultBranch.getName()))
        .contains(table1s2, table1s3)
        .doesNotContain(table1s1);

    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint1.getHash())); // Not collected

    cleanupSilently(table1);
  }

  @Test
  public void testTableOverrideCutoffTime() throws Exception {
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    Reference commitPoint1 = getNessieApi().getReference().refName("main").get();
    String table1s3 = newSnapshot(table1);

    // Setup snapshot age to retain in table property
    String table1QualifiedName =
        String.format("%s@%s", String.join(".", table1), defaultBranch.getName());
    Table icebergTable1 = loadTable(table1QualifiedName);
    icebergTable1
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(
            MAX_SNAPSHOT_AGE_MS,
            String.valueOf(TimeUnit.HOURS.toMillis(1))) // all snapshots will get retained
        .commit();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch and tag head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));

    // Assert that all snapshots are retained
    assertThat(snapshots(table1, defaultBranch.getName())).contains(table1s1, table1s2, table1s3);
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint1.getHash())); // Not collected

    setNessieGCDefaultCutOffPolicy("PT10M");

    // Test other way round, default policy is conservative whereas table level policy is aggressive
    icebergTable1
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(MAX_SNAPSHOT_AGE_MS, "1")
        .commit();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(snapshots(table1, defaultBranch.getName())).contains(table1s1, table1s2, table1s3);
    setNessieGCDefaultCutOffPolicy("PT0S");

    cleanupSilently(table1);
  }

  @Test
  public void testDifferentBranchTablesWithSameName() throws Exception {
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

    cleanupSilently(table1);
    cleanupSilently(table2);
  }

  @Test
  public void testRemoveAllHistoryInMultipleTablesUsingNessieConfig() throws Exception {
    Branch defaultBranch = getNessieApi().getDefaultBranch();

    final List<String> table1 = createTable(); // 1 Snapshot
    final List<String> table2 = createTable(); // 1 Snapshot

    String table1s1 = newSnapshot(table1);
    String table2s1 = newSnapshot(table2);

    Reference commitPoint1 = getNessieApi().getReference().refName("main").get();

    String table1s2 = newSnapshot(table1);

    Reference commitPoint2 = getNessieApi().getReference().refName("main").get();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    // Select queries work on branch head
    assertDoesNotThrow(() -> selectQuery(table1, "BRANCH " + defaultBranch.getName()));
    assertDoesNotThrow(() -> selectQuery(table2, "BRANCH " + defaultBranch.getName()));

    // Assert that older snapshots are collected
    assertThat(snapshots(table1, defaultBranch.getName())).containsExactly(table1s2);
    assertThat(snapshots(table2, defaultBranch.getName())).containsExactly(table2s1);

    // Commit 2 shows all the snapshots prior to vacuum, but expired snapshots cannot be queried
    assertThat(snapshotsOnHash(table1, commitPoint2.getHash())).contains(table1s1, table1s2);
    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () ->
            selectOnSnapshotAtRef(
                table2,
                table2s1,
                commitPoint2.getHash())); // not collected because it's also the latest

    // Table 1 is completely collected at Commit 1. Table 2 is in the same state as on branch head,
    // hence queryable.
    assertThatThrownBy(() -> selectQuery(table1, ref(commitPoint1.getHash())));
    assertDoesNotThrow(() -> selectQuery(table2, ref(commitPoint1.getHash())));

    cleanupSilently(table1);
    cleanupSilently(table2);
  }

  @Test
  public void testNoneCutoffPolicyWithNessieConfig() throws Exception {
    setNessieGCDefaultCutOffPolicy("NONE");
    assertDoesNotThrow(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testNullCutoffPolicyWithNessieConfig() throws Exception {
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().build();
    getNessieApi().updateRepositoryConfig().repositoryConfig(gcConfig).update();
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(DEFAULT_CUTOFF_POLICY_EMPTY_ERROR_MESSAGE);
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testNumericCutOffPolicyNessieConfig() throws Exception {
    setNessieGCDefaultCutOffPolicy("2");
    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(DEFAULT_CUTOFF_POLICY_WITH_COMMITS_ERROR_MESSAGE);
    setNessieGCDefaultCutOffPolicy("PT0S");
  }

  @Test
  public void testSnapshotsAfterVacuumWithCutoffPolicyAtNessieConfig() throws Exception {
    final List<String> table1 = createTable();

    String table1s1 = newSnapshot(table1);
    String table1s2 = newSnapshot(table1);
    String table1s3 = newSnapshot(table1);

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    String table1s4 = newSnapshot(table1);
    Reference commitPoint = getNessieApi().getReference().refName("main").get();

    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint.getHash())); // not collected
    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint.getHash())); // not collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s3, commitPoint.getHash())); // collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s4, commitPoint.getHash())); // collected

    cleanupSilently(table1);
  }

  @Test
  public void testInstantTimeNessieConfig() throws Exception {
    final List<String> table1 = createTable(); // 1 Snapshot

    String table1s1 = newSnapshot(table1);
    wait1MS();
    // all the snapshots should be expired before this time.
    String instantTime = Instant.now().toString();
    wait1MS();
    String table1s2 = newSnapshot(table1);
    String table1s3 = newSnapshot(table1);
    setNessieGCDefaultCutOffPolicy(instantTime);
    Reference commitPoint2 = getNessieApi().getReference().refName("main").get();
    wait1MS();

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThatThrownBy(
        () -> selectOnSnapshotAtRef(table1, table1s1, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s2, commitPoint2.getHash())); // collected
    assertDoesNotThrow(
        () -> selectOnSnapshotAtRef(table1, table1s3, commitPoint2.getHash())); //  collected

    setNessieGCDefaultCutOffPolicy("PT0S");

    cleanupSilently(table1);
  }

  @Test
  public void testWithFolder() throws Exception {
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Create table under the subfolder
    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    cleanupSilently(mainTablePath);
  }

  @Test
  public void testV2TableWithPositionalDeletes() throws Exception {
    Map<String, String> tableProps =
        ImmutableMap.of(
            NESSIE_GC_ENABLED, "true", FORMAT_VERSION, "2", DELETE_MODE, "merge-on-read");
    String tableName = "table_with_positional_deletes";
    Table table = createTable(String.format("%s@main", tableName), tableProps);

    DataFile dataFile = appendDataFile(table, "data.parquet");
    DeleteFile deleteFile = addDeleteFile(table, "positional_delete.parquet");
    table
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(COMMIT_NUM_RETRIES, "2")
        .commit(); // Reset GC enabled as it is defaulted in NessieCatalog

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, dataFile.path().toString()))
        .isTrue();
    assertThat(getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, deleteFile.path().toString()))
        .isTrue();

    cleanupSilently(tablePathWithFolders(tableName));
  }

  @Test
  public void testMissingMetadataError() throws Exception {
    final List<String> table1 = createTable(); // Missing metadata

    newSnapshot(table1);

    List<String> table1SurvivingFiles = getFiles(table1, s -> true);
    List<String> table1MetadataFiles = getFiles(table1, s -> s.endsWith(".json"));
    table1SurvivingFiles.removeIf(path -> path.endsWith(".json"));
    table1MetadataFiles.forEach(
        metadataFile -> getDataplaneStorage().deleteObject(PRIMARY_BUCKET, metadataFile));

    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);

    assertThat(table1SurvivingFiles)
        .allMatch(file -> getDataplaneStorage().doesObjectExist(PRIMARY_BUCKET, file));

    testBuilder()
        .sqlQuery("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME)
        .unOrdered()
        .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
        .baselineValues(0L, 0L)
        .go();

    cleanupSilently(table1);
  }

  @Test
  public void testMissingManifestListError() throws Exception {
    final List<String> table = createTable(); // Missing manifest list

    newSnapshot(table);

    List<String> tableSurvivingFiles = getFiles(table, s -> true);
    List<String> tableManifestLists = getFiles(table, k -> manifestListsCriteria(table).test(k));
    tableSurvivingFiles.removeAll(tableManifestLists);

    //     Respective manifests and data files are unreachable, hence will be considered orphan and
    // deleted
    tableManifestLists.forEach(
        manifestListFile -> getDataplaneStorage().deleteObject(PRIMARY_BUCKET, manifestListFile));

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));

    cleanupSilently(table);
  }

  @Test
  public void testMissingManifestError() throws Exception {
    final List<String> table = createTable(); // Missing manifest

    newSnapshot(table);

    List<String> tableManifestFiles = getFiles(table, k -> manifestsCriteria(table).test(k));
    tableManifestFiles.forEach(
        manifestFile -> getDataplaneStorage().deleteObject(PRIMARY_BUCKET, manifestFile));

    assertThatThrownBy(() -> runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void testWithMultipleBatches() throws Exception {
    final int numOfTables = 5;
    final int numOfSnapshots = 5;
    List<List<String>> tables =
        IntStream.range(0, numOfTables).mapToObj(i -> createTable()).collect(Collectors.toList());
    IntStream.range(0, numOfSnapshots).forEach(i -> tables.forEach(this::newSnapshot));

    setTargetBatchSize(2);
    runSQL("VACUUM CATALOG " + DATAPLANE_PLUGIN_NAME);
    resetTargetBatchSize();

    tables.forEach(t -> assertDoesNotThrow(() -> selectQuery(t, "BRANCH main")));
    tables.forEach(t -> assertThat(snapshots(t, "main")).hasSize(1));

    tables.forEach(super::cleanupSilently);
  }
}
