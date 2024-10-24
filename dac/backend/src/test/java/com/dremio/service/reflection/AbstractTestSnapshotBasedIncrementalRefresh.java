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
package com.dremio.service.reflection;

import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;
import static com.dremio.service.reflection.ReflectionOptions.ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.accelerator.BaseTestReflection;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.Transform;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractTestSnapshotBasedIncrementalRefresh extends BaseTestReflection {
  @FunctionalInterface
  public interface CheckedFunction<T> {
    void apply(T t) throws IOException;
  }

  @FunctionalInterface
  public interface CheckedCallable {
    void apply() throws IOException;
  }

  protected static final AtomicInteger TABLE_NUMBER = new AtomicInteger(0);
  private final String testTableName =
      "test_snapshot_based_incremental_reflections" + TABLE_NUMBER.getAndIncrement();
  private BufferAllocator allocator;
  private final ReflectionMonitor monitor = newReflectionMonitor();

  protected abstract String getTestTablePath();

  protected abstract String getTestTableFullPath();

  protected abstract NamespaceKey getTestTableNamespaceKey();

  protected abstract void dropTestTable() throws IOException;

  protected void refreshTableMetadata() {}
  ;

  protected ReflectionMonitor getMonitor() {
    return monitor;
  }

  @BeforeClass
  public static void prepare() throws Exception {
    assumeFalse(isMultinode());
    setDeletionGracePeriod();
    setManagerRefreshDelay(5);
    setMaterializationCacheSettings(false);
  }

  @Before
  public void before() throws Exception {
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    setSystemOption(ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS, false);
  }

  @After
  public void after() throws Exception {
    resetSystemOption(ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS);
    setDeletionGracePeriod();
    getReflectionService().clearAll();
    monitor.waitUntilNoMaterializationsAvailable();
    resetSettings();
    allocator.close();
  }

  protected String getTestTableName() {
    return testTableName;
  }

  protected void runSQL(String sql) {
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder().setSqlQuery(getQueryFromSQL(sql.toString())).build());
  }

  protected String getBaseTableCurrentSnapshotId() throws NamespaceException {
    return getCurrentSnapshotId(getTestTableNamespaceKey()).orElse("0");
  }

  @Override
  protected List<Refresh> getRefreshes(ReflectionId id) {
    final Materialization m =
        Preconditions.checkNotNull(
            getMaterializationStore().getLastMaterializationDone(id),
            "No materialization found for reflection %s",
            id.getId());
    final FluentIterable<Refresh> refreshes = getMaterializationStore().getRefreshes(m);
    assertFalse(
        String.format("no refreshes found for reflection %s", id.getId()),
        Iterables.isEmpty(refreshes));
    return refreshes.stream()
        .sorted((Comparator.comparingInt(Refresh::getSeriesOrdinal)))
        .collect(Collectors.toList());
  }

  protected boolean wasQueryAcceleratedWith(
      final String query, final Materialization... materializations) throws JobNotFoundException {
    try (final JobDataFragment data =
        submitJobAndGetData(
            l(JobsService.class),
            JobRequest.newBuilder()
                .setSqlQuery(new SqlQuery("explain plan for " + query, SYSTEM_USERNAME))
                .setQueryType(QueryType.JDBC)
                .build(),
            0,
            1,
            allocator)) {
      String text = data.extractValue("text", 0).toString();
      for (Materialization m : materializations) {
        if (text.contains(
            String.format(
                "\"__accelerator\".\"%s\".\"%s\"",
                m.getReflectionId().getId(), m.getId().getId()))) {
          return true;
        }
      }
      return false;
    }
  }

  protected void validateFullRefresh(
      final Materialization materialization, final Pair<String, String>... snapshotIdPairs)
      throws JobNotFoundException {
    final String planReflection = getReflectionPlan(materialization);
    SnapshotBasedIncrementalRefreshTestUtils.validateFullRefresh(
        planReflection, getTestTableFullPath(), snapshotIdPairs);
  }

  protected void validateSnapshotBasedIncrementalRefresh(
      final Materialization materialization, final Pair<String, String>... snapshotIdPairs)
      throws Exception {
    final String planReflection = getReflectionPlan(materialization);
    SnapshotBasedIncrementalRefreshTestUtils.validateSnapshotBasedIncrementalRefresh(
        planReflection, getTestTableFullPath(), snapshotIdPairs);
  }

  /** Test basic incremental refresh. - Single raw reflection on VDS */
  protected void testIncrementalRefresh(
      CheckedCallable createTestTable, CheckedFunction<Integer> insertRows) throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertNotNull("Missing join analysis", m.getJoinAnalysis().getJoinTablesList());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      // Ensure the Refresh Decision was logged to the profile phases.
      QueryProfileRequest refreshRequest =
          QueryProfileRequest.newBuilder()
              .setJobId(JobProtobuf.JobId.newBuilder().setId(m.getInitRefreshJobId()).build())
              .setAttempt(0)
              .setUserName(SYSTEM_USERNAME)
              .build();
      UserBitShared.QueryProfile refreshProfile = getJobsService().getProfile(refreshRequest);
      List<UserBitShared.PlanPhaseProfile> refreshDecision =
          refreshProfile.getPlanPhasesList().stream()
              .filter(p -> p.getPhaseName().equals(PlannerPhase.PLAN_REFRESH_DECISION))
              .collect(Collectors.toList());
      assertEquals(1, refreshDecision.size());

      final Refresh refresh0 = refreshes.get(0);
      final long reflectionSize0 = getReflectionStatusService().getTotalReflectionSize(vdsRawId);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for it to refresh again
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Refresh shouldn't write new data", 1, refreshes.size());
      assertEquals("Refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh series id shouldn't change",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh series ordinal shouldn't change",
          refresh0.getSeriesOrdinal(),
          refreshes.get(0).getSeriesOrdinal());
      assertNotNull("Missing join analysis", m.getJoinAnalysis().getJoinTablesList());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      final long reflectionSize1 = getReflectionStatusService().getTotalReflectionSize(vdsRawId);
      assertEquals("No on disk change, sizes should be the same", reflectionSize0, reflectionSize1);

      // add new data
      insertRows.apply(200);
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh0.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(400),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      assertNotNull("missing join analysis", m.getJoinAnalysis().getJoinTablesList());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      // Ensure the Refresh Decision was logged to the profile phases.
      refreshRequest =
          QueryProfileRequest.newBuilder()
              .setJobId(JobProtobuf.JobId.newBuilder().setId(m.getInitRefreshJobId()).build())
              .setAttempt(0)
              .setUserName(SYSTEM_USERNAME)
              .build();
      refreshProfile = getJobsService().getProfile(refreshRequest);
      refreshDecision =
          refreshProfile.getPlanPhasesList().stream()
              .filter(p -> p.getPhaseName().equals(PlannerPhase.PLAN_REFRESH_DECISION))
              .collect(Collectors.toList());
      assertEquals(1, refreshDecision.size());

      final long reflectionSize2 = getReflectionStatusService().getTotalReflectionSize(vdsRawId);
      assertTrue("Disk size should increase", reflectionSize0 < reflectionSize2);

      DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      assertDependsOn(vdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 3L, data.extractValue("cnt_b", 0)); // 3 => insert200() 3 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 3L, data.extractValue("sum_b", 1)); // 3 => insert200() 3 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test multiple reflections for same base table. - One raw reflection on PDS - One raw reflection
   * on VDS ("select * from table_name") - Each reflection should refresh incrementally by scanning
   * base PDS
   */
  protected void testMultiRaws(CheckedCallable createTestTable, CheckedFunction<Integer> insertRows)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery = String.format("select * from %s", getTestTablePath());
      final String testQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id order by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection on PDS
      final DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      final ReflectionId pdsRawId =
          getReflectionService()
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.RAW)
                      .setDatasetId(dataset.getId().getId())
                      .setName("incremental-raw-pds")
                      .setDetails(
                          new ReflectionDetails()
                              .setDisplayFieldList(
                                  ImmutableList.<ReflectionField>builder()
                                      .add(new ReflectionField("id"))
                                      .add(new ReflectionField("b"))
                                      .add(new ReflectionField("c"))
                                      .build())));

      // wait for it to refresh
      Materialization pdsMat = monitor.waitUntilMaterialized(pdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(pdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(pdsRawId);
      assertEquals("No more than a single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh reflection id should match",
          pdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat));
      final Refresh pdsReflectionRefresh0 = refreshes.get(0);

      // create raw reflection of VDS
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("b").add("c").build(),
              "incremental-raw-vds");

      // wait for it to refresh
      Materialization vdsMat = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals(
          "Refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));
      final Refresh vdsReflectionRefresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      pdsMat = monitor.waitUntilMaterialized(pdsRawId, pdsMat);
      vdsMat = monitor.waitUntilMaterialized(vdsRawId, vdsMat);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(pdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          pdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          pdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(pdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(pdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          vdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(vdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(vdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));

      assertDependsOn(pdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));
      assertDependsOn(vdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              l(JobsService.class),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(testQuery, SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 2L, data.extractValue("cnt_b", 0)); // 2 => insert200() 2 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 2L, data.extractValue("sum_b", 1)); // 2 => insert200() 2 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test multiple reflections for same base table. - One raw reflection on PDS - One raw reflection
   * on VDS ("select id, count(b) as cnt_b, sum(b) as sum_b from table_name group by id order by
   * id") - Each reflection should refresh incrementally by scanning base PDS
   */
  protected void testMultiRawsWithAggInSql(
      CheckedCallable createTestTable, CheckedFunction<Integer> insertRows) throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());
      final String testQuery = vdsQuery + " order by id";

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection on PDS
      final DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      final ReflectionId pdsRawId =
          getReflectionService()
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.RAW)
                      .setDatasetId(dataset.getId().getId())
                      .setName("incremental-raw-pds")
                      .setDetails(
                          new ReflectionDetails()
                              .setDisplayFieldList(
                                  ImmutableList.<ReflectionField>builder()
                                      .add(new ReflectionField("id"))
                                      .add(new ReflectionField("b"))
                                      .add(new ReflectionField("c"))
                                      .build())));

      // wait for it to refresh
      Materialization pdsMat = monitor.waitUntilMaterialized(pdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(pdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(pdsRawId);
      assertEquals("No more than a single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh reflection id should match",
          pdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat));
      final Refresh pdsReflectionRefresh0 = refreshes.get(0);

      // create raw reflection of VDS
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "incremental-raw-vds");

      // wait for it to refresh
      Materialization vdsMat = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals(
          "Refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));
      final Refresh vdsReflectionRefresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      pdsMat = monitor.waitUntilMaterialized(pdsRawId, pdsMat);
      vdsMat = monitor.waitUntilMaterialized(vdsRawId, vdsMat);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(pdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          pdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          pdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(pdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(pdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          vdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(vdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(vdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat, vdsMat));

      assertDependsOn(pdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));
      assertDependsOn(vdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              l(JobsService.class),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(testQuery, SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 2L, data.extractValue("cnt_b", 0)); // 2 => insert200() 2 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 2L, data.extractValue("sum_b", 1)); // 2 => insert200() 2 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test multiple reflections for same base table. - One raw reflection on PDS - One agg reflection
   * on VDS - Each reflection should refresh incrementally by scanning base PDS
   */
  protected void testRawAndAgg(CheckedCallable createTestTable, CheckedFunction<Integer> insertRows)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery = String.format("select * from %s", getTestTablePath());
      final String testQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id order by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection on PDS
      final DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      final ReflectionId pdsRawId =
          getReflectionService()
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.RAW)
                      .setDatasetId(dataset.getId().getId())
                      .setName("incremental-raw-pds")
                      .setDetails(
                          new ReflectionDetails()
                              .setDisplayFieldList(
                                  ImmutableList.<ReflectionField>builder()
                                      .add(new ReflectionField("id"))
                                      .add(new ReflectionField("b"))
                                      .add(new ReflectionField("c"))
                                      .build())));

      // wait for it to refresh
      Materialization pdsMat = monitor.waitUntilMaterialized(pdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(pdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(pdsRawId);
      assertEquals("No more than a single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh reflection id should match",
          pdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, pdsMat));
      final Refresh pdsReflectionRefresh0 = refreshes.get(0);

      // create VDS
      final DatasetUI vds = createVdsFromQuery(vdsQuery, TEST_SPACE);

      // create agg reflection of VDS
      final ReflectionId vdsAggId =
          getReflectionService()
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.AGGREGATION)
                      .setDatasetId(vds.getId())
                      .setName("incremental-agg-vds")
                      .setDetails(
                          new ReflectionDetails()
                              .setDimensionFieldList(reflectionDimensionFields("id"))
                              .setMeasureFieldList(reflectionMeasureFields("b", "c"))));

      // wait for it to refresh
      Materialization vdsMat = monitor.waitUntilMaterialized(vdsAggId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsAggId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsAggId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsAggId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsAggId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals(
          "Refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsAggId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, vdsMat));
      final Refresh vdsReflectionRefresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      pdsMat = monitor.waitUntilMaterialized(pdsRawId, pdsMat);
      vdsMat = monitor.waitUntilMaterialized(vdsAggId, vdsMat);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(pdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(pdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(pdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          pdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          pdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          pdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(pdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(pdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, pdsMat));

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsAggId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsAggId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsAggId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change",
          vdsReflectionRefresh0.getId(),
          refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsAggId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          vdsReflectionRefresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(vdsReflectionRefresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(vdsMat, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, vdsMat));

      assertDependsOn(pdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));
      assertDependsOn(vdsAggId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              l(JobsService.class),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(testQuery, SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 2L, data.extractValue("cnt_b", 0)); // 2 => insert200() 2 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 2L, data.extractValue("sum_b", 1)); // 2 => insert200() 2 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test incremental reflection refreshes for many times. - Single raw reflection on VDS - 5
   * incremental changes on base table and 5 incremental refreshes
   */
  protected void testIncrementalRefreshMultiTimes(
      CheckedCallable createTestTable, CheckedFunction<Integer> insertRows) throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      final Refresh refresh0 = refreshes.get(0);

      final String[] snapshotIds = new String[11];
      snapshotIds[0] = snapshotId0;
      for (int i = 1; i < 5; i++) {
        // add new data
        insertRows.apply(200);
        refreshTableMetadata();
        snapshotIds[i] = getBaseTableCurrentSnapshotId();
        assertNotEquals(
            String.format("Base table current snapshot should change (i = %d)", i),
            snapshotIds[i - 1],
            snapshotIds[i]);

        // refresh the reflection
        requestRefresh(getTestTableNamespaceKey());

        // wait for the refresh
        m = monitor.waitUntilMaterialized(vdsRawId, m);

        assertEquals(
            String.format("Reflection materialization failed (i = %d)", i),
            0,
            (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
        assertEquals(
            "Refresh method should be incremental",
            RefreshMethod.INCREMENTAL,
            getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
        validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotIds[i - 1], snapshotIds[i]));
      }

      refreshes = getRefreshes(vdsRawId);

      assertEquals("Materialization should have written data", 5, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());

      for (int i = 1; i < 5; i++) {
        assertNotEquals(
            String.format("New refresh entry id should change (i = %d)", i),
            refresh0.getId(),
            refreshes.get(i).getId());
        assertEquals(
            String.format("New refresh reflection id shouldn't change (i = %d)", i),
            vdsRawId.getId(),
            refreshes.get(i).getReflectionId().getId());
        assertEquals(
            String.format("New refresh should have the same seriesId (i = %d)", i),
            refresh0.getSeriesId(),
            refreshes.get(i).getSeriesId());
        assertEquals(
            String.format("New refresh series ordinal should increase by 1 (i = %d)", i),
            Integer.valueOf(refresh0.getSeriesOrdinal() + i),
            refreshes.get(i).getSeriesOrdinal());
        assertEquals(
            String.format("Base table snapshot id should be saved in refresh (i = %d)", i),
            snapshotIds[i],
            refreshes.get(i).getUpdateId().getStringUpdateId());
        assertEquals(
            String.format("Incorrect input records (i = %d)", i),
            Long.valueOf(200),
            refreshes.get(i).getJob().getInputRecords());
        assertEquals(
            String.format("Incorrect output records (i = %d)", i),
            Long.valueOf(2),
            refreshes.get(i).getJob().getOutputRecords());
      }
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 5L, data.extractValue("cnt_b", 0)); // 5 => insert200() 5 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 5L, data.extractValue("sum_b", 1)); // 5 => insert200() 5 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test update on base table. - Single raw reflection on VDS - Insert -> R0 -> Insert -> R1 ->
   * Update -> R2 -> Insert -> R3 - R0: initial full refresh - R1: incremental refresh - R2: initial
   * full refresh (new series id) - R3: incremental refresh
   */
  protected void testUpdate(
      CheckedCallable createTestTable,
      CheckedFunction<Integer> insertRows,
      CheckedCallable updateTable)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      final Refresh refresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh0.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // update base table
      updateTable.apply();
      refreshTableMetadata();
      final String snapshotId2 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId1, snapshotId2);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals("Refresh entry id should change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertNotEquals(
          "Refresh should have the new seriesId",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId2,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(400),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      validateFullRefresh(m, Pair.of(snapshotId1, snapshotId2));
      final Refresh refresh2 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId3 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh2.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh2.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh2.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh2.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId3,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId2, snapshotId3));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 3L, data.extractValue("cnt_b", 0)); // 3 => insert200() 3 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 3L, data.extractValue("sum_b", 1)); // insert200() 3 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test rollback on base table. - Single raw reflection on VDS - Insert(S0) -> R0 -> Insert -> R1
   * -> Rollback(to S0) -> R2 -> Insert -> R3 - R0: initial full refresh - R1: incremental refresh -
   * R2: initial full refresh (new series id) - R3: incremental refresh
   */
  protected void testRollback(CheckedCallable createTestTable, CheckedFunction<Integer> insertRows)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      insertRows.apply(200);
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      final Refresh refresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh0.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId0, snapshotId1));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // rollback base table to snapshot0
      runSQL(String.format("ROLLBACK TABLE %s TO SNAPSHOT '%s'", getTestTablePath(), snapshotId0));
      final String snapshotId2 = getBaseTableCurrentSnapshotId();
      assertEquals(
          "Base table current snapshot should change to snapshotId0", snapshotId0, snapshotId2);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals("Refresh entry id should change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertNotEquals(
          "Refresh should have the new seriesId",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId2,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      validateFullRefresh(m, Pair.of(snapshotId1, snapshotId2));
      final Refresh refresh2 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      final String snapshotId3 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh2.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh2.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh2.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh2.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId3,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId2, snapshotId3));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 2L, data.extractValue("cnt_b", 0)); // 2 => insert200() 2 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 2L, data.extractValue("sum_b", 1)); // 2 => insert200() 2 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test vacuum on base table. - Single raw reflection on VDS - Insert(S0) -> R0 -> Insert(S1) ->
   * Expire S0 -> R1 -> Insert(S2) -> R2 - R0: initial full refresh - R1: initial full refresh (new
   * series id) - R2: incremental refresh
   */
  protected void testVacuum(CheckedCallable createTestTable, CheckedFunction<Integer> insertRows)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      insertRows.apply(200);
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      final Refresh refresh0 = refreshes.get(0);

      // add new data
      final long timestampMillisToExpire = System.currentTimeMillis();
      insertRows.apply(200);
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      runSQL(
          String.format(
              "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
              getTestTablePath(), getTimestampFromMillis(timestampMillisToExpire)));

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertNotEquals("Refresh entry id should change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertNotEquals(
          "Refresh should have the new seriesId",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved",
          snapshotId1,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(400),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      validateFullRefresh(m, Pair.of(snapshotId0, snapshotId1));
      final Refresh refresh1 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      final String snapshotId2 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId1, snapshotId2);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh1.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh1.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh1.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh1.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId2,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId1, snapshotId2));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 3L, data.extractValue("cnt_b", 0)); // 2 => insert200() 3 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 3L, data.extractValue("sum_b", 1)); // 2 => insert200() 3 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test compactions on base table. - Single raw reflection on VDS - Insert(S0) -> R0 -> Insert(S1)
   * -> Insert(S2) -> Compact(S3) -> Insert(S4) -> Compact(S5) -> Insert(S6) -> R1 - R0: initial
   * full refresh - R1: incremental refresh - Expected snapshot diff ranges: [(S0, S2), (S3, S4),
   * (S5, S6)]
   */
  protected void testOptimize(CheckedCallable createTestTable, CheckedFunction<Integer> insertRows)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      insertRows.apply(200);
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      final Refresh refresh0 = refreshes.get(0);

      // add new data
      insertRows.apply(200);
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);
      // add new data
      insertRows.apply(200);
      final String snapshotId2 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId1, snapshotId2);
      // compact base table
      runSQL(
          String.format(
              "OPTIMIZE TABLE %s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
              getTestTablePath()));
      final String snapshotId3 = getBaseTableCurrentSnapshotId();
      assertNotEquals(
          "Base table current snapshot should change to snapshotId0", snapshotId2, snapshotId3);
      // add new data
      insertRows.apply(200);
      final String snapshotId4 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId3, snapshotId4);
      // compact base table
      runSQL(
          String.format(
              "OPTIMIZE TABLE %s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
              getTestTablePath()));
      final String snapshotId5 = getBaseTableCurrentSnapshotId();
      assertNotEquals(
          "Base table current snapshot should change to snapshotId0", snapshotId4, snapshotId5);
      // add new data
      insertRows.apply(200);
      final String snapshotId6 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId5, snapshotId6);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh0.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId6,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records",
          Long.valueOf(200 * 4),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(
          m,
          Pair.of(snapshotId0, snapshotId2),
          Pair.of(snapshotId3, snapshotId4),
          Pair.of(snapshotId5, snapshotId6));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(100 * 5L, data.extractValue("cnt_b", 0)); // 5 => insert200() 5 times
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(200 * 5L, data.extractValue("sum_b", 1)); // 5 => insert200() 5 times
      }
    } finally {
      dropTestTable();
    }
  }

  /**
   * Test dataset reflection refresh setting changes are ignored by snapshot based incremental
   * refresh
   */
  protected void testRefreshSettingsChange(
      CheckedCallable createTestTable, CheckedFunction<Integer> insertRows) throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      final Refresh refresh0 = refreshes.get(0);

      // change the refresh settings, setting a refresh field
      setDatasetAccelerationSettings(
          getTestTableNamespaceKey(), 0, MINUTES.toMillis(10), true, "value");

      // request a dataset refresh
      requestRefresh(getTestTableNamespaceKey());

      // wait for it to refresh again
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Refresh shouldn't write new data", 1, refreshes.size());
      assertEquals("Refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh series id shouldn't change",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh series ordinal shouldn't change",
          refresh0.getSeriesOrdinal(),
          refreshes.get(0).getSeriesOrdinal());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));

      // change the refresh settings, settings to full
      setDatasetAccelerationSettings(getTestTableNamespaceKey(), 0, MINUTES.toMillis(10));

      // request a dataset refresh
      requestRefresh(getTestTableNamespaceKey());

      // wait for it to refresh again
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Refresh shouldn't write new data", 1, refreshes.size());
      assertEquals("Refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertEquals(
          "Refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh series id shouldn't change",
          refresh0.getSeriesId(),
          refreshes.get(0).getSeriesId());
      assertEquals(
          "Refresh series ordinal shouldn't change",
          refresh0.getSeriesOrdinal(),
          refreshes.get(0).getSeriesOrdinal());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(vdsQuery, m));
      DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      assertDependsOn(vdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));
    } finally {
      dropTestTable();
    }
  }

  protected void testDatasetHashChangeHelper(
      CheckedCallable createTestTable,
      CheckedFunction<Integer> insertRows,
      CheckedCallable alterTestTable,
      CheckedCallable insertRows1)
      throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery =
          String.format(
              "select id, count(b) as cnt_b, sum(b) as sum_b from %s group by id",
              getTestTablePath());

      // insert initial data
      refreshTableMetadata();
      insertRows.apply(200);
      refreshTableMetadata();
      final String snapshotId0 = getBaseTableCurrentSnapshotId();

      // create raw reflection
      final ReflectionId vdsRawId =
          createRawFromQuery(
              vdsQuery,
              TEST_SPACE,
              ImmutableList.<String>builder().add("id").add("cnt_b").add("sum_b").build(),
              "vds-raw");

      // wait until raw can accelerate
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      // change pds schema
      alterTestTable.apply();
      refreshTableMetadata();

      // insert 100 records
      //      StringBuilder sql = new StringBuilder(String.format("INSERT INTO %s VALUES",
      // getTestTablePath()));
      //      sql.append(String.format("('a', 1, %d, 'd1'),('b',2, %d, 'd2')", idx++, idx++));
      //      for (int i = 0; i < 49; i++) {
      //        sql.append(String.format(",('a', 1, %d, 'd1'),('b',2, %d, 'd2')", idx++, idx++));
      //      }
      //      runSQL(sql.toString());
      // insertRows.apply(100);
      insertRows1.apply();
      refreshTableMetadata();
      final String snapshotId1 = getBaseTableCurrentSnapshotId();
      assertNotEquals("Base table current snapshot should change", snapshotId0, snapshotId1);

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for it to refresh again
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);

      assertEquals("There should be two refreshes", 2, refreshes.size());
      assertEquals(
          "New refresh should have the same seriesId",
          refreshes.get(0).getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "1st refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "2nd refresh series ordinal should increase by 1",
          Integer.valueOf(1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in 1st refresh",
          snapshotId0,
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Base table snapshot id should be saved in 2nd refresh",
          snapshotId1,
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Initial refresh should have 200 input records",
          Long.valueOf(200),
          refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Initial refresh should output 2 records",
          Long.valueOf(2),
          refreshes.get(0).getJob().getOutputRecords());
      assertEquals(
          "Incremental refresh should have 100 input records",
          Long.valueOf(100),
          refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incremental refresh should output 2 records",
          Long.valueOf(2),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId0, snapshotId1));

      DatasetConfig dataset = getNamespaceService().getDataset(getTestTableNamespaceKey());
      assertDependsOn(vdsRawId, dependency(dataset.getId().getId(), getTestTableNamespaceKey()));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(vdsQuery + " order by id", SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              10,
              allocator)) {
        assertEquals(2, data.getReturnedRowCount());
        assertEquals(new Text("a"), data.extractValue("id", 0));
        assertEquals(150L, data.extractValue("cnt_b", 0)); // insert200 + insert100
        assertEquals(new Text("b"), data.extractValue("id", 1));
        assertEquals(300L, data.extractValue("sum_b", 1)); // insert200 + insert100
      }
    } finally {
      dropTestTable();
    }
  }

  /** Test partitioned reflection - Single raw reflection on VDS - Partitioned on IDENTITY */
  protected void testPartitionedReflection(
      CheckedCallable createTestTable, CheckedCallable insertRows) throws Exception {
    try {
      createTestTable.apply();
      final String vdsQuery = String.format("select * from %s", getTestTablePath());
      final String testQuery = String.format("select rev from %s order by 1", getTestTablePath());

      refreshTableMetadata();
      insertRows.apply();
      refreshTableMetadata();
      final Optional<String> snapshotId0 = getCurrentSnapshotId(getTestTableNamespaceKey());
      assertTrue("snapshot id not found", snapshotId0.isPresent());
      // create VDS
      final DatasetUI vds = createVdsFromQuery(vdsQuery, TEST_SPACE);

      final ReflectionId vdsRawId =
          getReflectionService()
              .create(
                  new ReflectionGoal()
                      .setType(ReflectionType.RAW)
                      .setDatasetId(vds.getId())
                      .setName("vds-raw")
                      .setDetails(
                          new ReflectionDetails()
                              .setDisplayFieldList(
                                  ImmutableList.<ReflectionField>builder()
                                      .add(new ReflectionField("store_id"))
                                      .add(new ReflectionField("sale_time"))
                                      .add(new ReflectionField("rev"))
                                      .build())
                              .setPartitionFieldList(
                                  ImmutableList.<ReflectionPartitionField>builder()
                                      .add(
                                          new ReflectionPartitionField("sale_time")
                                              .setTransform(
                                                  new Transform()
                                                      .setType(Transform.Type.valueOf("IDENTITY"))))
                                      .build())));

      // wait for it to refresh
      Materialization m = monitor.waitUntilMaterialized(vdsRawId);

      assertTrue(
          "Reflection entry not found", getReflectionService().getEntry(vdsRawId).isPresent());
      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      List<Refresh> refreshes = getRefreshes(vdsRawId);
      assertEquals("Single refresh expected", 1, refreshes.size());
      assertEquals(
          "Refresh reflection id should match",
          vdsRawId.getId(),
          refreshes.get(0).getReflectionId().getId());
      assertEquals(
          "Refresh should be initial", Integer.valueOf(0), refreshes.get(0).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId0.get(),
          refreshes.get(0).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records", Long.valueOf(5), refreshes.get(0).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(5),
          refreshes.get(0).getJob().getOutputRecords());
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, m));
      final Refresh refresh0 = refreshes.get(0);

      // add new data
      insertRows.apply();
      refreshTableMetadata();
      final Optional<String> snapshotId1 = getCurrentSnapshotId(getTestTableNamespaceKey());
      assertTrue("snapshot id not found", snapshotId1.isPresent());
      assertNotEquals(
          "Base table current snapshot should change", snapshotId0.get(), snapshotId1.get());

      // refresh the reflection
      requestRefresh(getTestTableNamespaceKey());

      // wait for the refresh
      m = monitor.waitUntilMaterialized(vdsRawId, m);

      assertEquals(
          "Reflection materialization failed",
          0,
          (int) getReflectionService().getEntry(vdsRawId).get().getNumFailures());
      assertEquals(
          "Refresh method should be incremental",
          RefreshMethod.INCREMENTAL,
          getReflectionService().getEntry(vdsRawId).get().getRefreshMethod());
      refreshes = getRefreshes(vdsRawId);
      assertEquals("Materialization should have written data", 2, refreshes.size());
      assertEquals(
          "1st refresh entry id shouldn't change", refresh0.getId(), refreshes.get(0).getId());
      assertNotEquals(
          "New refresh entry id should change", refresh0.getId(), refreshes.get(1).getId());
      assertEquals(
          "New refresh reflection id shouldn't change",
          vdsRawId.getId(),
          refreshes.get(1).getReflectionId().getId());
      assertEquals(
          "New refresh should have the same seriesId",
          refresh0.getSeriesId(),
          refreshes.get(1).getSeriesId());
      assertEquals(
          "New refresh series ordinal should increase by 1",
          Integer.valueOf(refresh0.getSeriesOrdinal() + 1),
          refreshes.get(1).getSeriesOrdinal());
      assertEquals(
          "Base table snapshot id should be saved in refresh",
          snapshotId1.get(),
          refreshes.get(1).getUpdateId().getStringUpdateId());
      assertEquals(
          "Incorrect input records", Long.valueOf(5), refreshes.get(1).getJob().getInputRecords());
      assertEquals(
          "Incorrect output records",
          Long.valueOf(5),
          refreshes.get(1).getJob().getOutputRecords());
      validateSnapshotBasedIncrementalRefresh(m, Pair.of(snapshotId0.get(), snapshotId1.get()));
      assertTrue("Query not accelerated", wasQueryAcceleratedWith(testQuery, m));

      // confirm that we are indeed reading all the new data
      try (final JobDataFragment data =
          submitJobAndGetData(
              getJobsService(),
              JobRequest.newBuilder()
                  .setSqlQuery(new SqlQuery(testQuery, SYSTEM_USERNAME))
                  .setQueryType(QueryType.JDBC)
                  .build(),
              0,
              20,
              allocator)) {
        assertEquals(10, data.getReturnedRowCount());
        assertEquals(10.0, data.extractValue("rev", 0));
        assertEquals(10.0, data.extractValue("rev", 1));
        assertEquals(20.0, data.extractValue("rev", 2));
        assertEquals(20.0, data.extractValue("rev", 3));
        assertEquals(30.0, data.extractValue("rev", 4));
        assertEquals(30.0, data.extractValue("rev", 5));
        assertEquals(40.0, data.extractValue("rev", 6));
        assertEquals(40.0, data.extractValue("rev", 7));
        assertEquals(50.0, data.extractValue("rev", 8));
        assertEquals(50.0, data.extractValue("rev", 9));
      }
    } finally {
      dropTestTable();
    }
  }
}
