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
package com.dremio.service.accelerator;

import static com.dremio.common.utils.PathUtils.parseFullPath;
import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.accelerator.proto.SubstitutionState.CHOSEN;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_PERIODIC_WAKEUP_ONLY;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.api.Space;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionValue;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.LogicalPlanCaptureListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.DependencyUtils;
import com.dremio.service.reflection.ReflectionMonitor;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/** Basic integration tests to ensure the reflection services and its components work as expected */
public class BaseTestReflection extends BaseTestServer {

  private static AtomicInteger queryNumber = new AtomicInteger(0);

  protected static final String TEST_SPACE = "refl_test";
  protected static final String TEMP_SCHEMA = "dfs_test";

  private static MaterializationStore materializationStore;
  private static DependenciesStore dependenciesStore;
  private static MaterializationPlanStore materializationPlanStore;
  private static ReflectionEntriesStore entriesStore;

  @ClassRule public static final TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    BaseTestServer.getPopulator().populateTestUsers();
    setupAdditionalServices();
  }

  protected static void setupAdditionalServices() throws NamespaceException {
    final NamespaceService nsService = getNamespaceService();
    final SpaceConfig config = new SpaceConfig().setName(TEST_SPACE);
    nsService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);

    materializationStore = new MaterializationStore(p(LegacyKVStoreProvider.class));
    materializationPlanStore = new MaterializationPlanStore(p(LegacyKVStoreProvider.class));
    dependenciesStore = new DependenciesStore(p(LegacyKVStoreProvider.class));
    entriesStore = new ReflectionEntriesStore(p(LegacyKVStoreProvider.class));
    setSystemOption(PlannerSettings.QUERY_PLAN_CACHE_ENABLED.getOptionName(), "false");
  }

  @AfterClass
  public static void resetSettings() {
    // reset deletion grace period
    setDeletionGracePeriod(HOURS.toSeconds(4));
    setManagerRefreshDelay(10);
    // Clear reflections to prevent scheduled reflection
    // maintenance in background during test shutdown
    clearReflections();
  }

  private static void clearReflections() {
    final ReflectionService reflectionService = getReflectionService();
    final ReflectionMonitor reflectionMonitor =
        newReflectionMonitor(TimeUnit.SECONDS.toMillis(5), TimeUnit.MINUTES.toMillis(2));
    reflectionService.clearAll();
    reflectionMonitor.waitUntilNoMaterializationsAvailable();
  }

  protected static MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  protected static DependenciesStore getDependenciesStore() {
    return dependenciesStore;
  }

  protected static MaterializationPlanStore getMaterializationPlanStore() {
    return materializationPlanStore;
  }

  protected static ReflectionEntriesStore getReflectionEntriesStore() {
    return entriesStore;
  }

  protected Catalog cat() {
    return l(CatalogService.class)
        .getCatalog(
            MetadataRequestOptions.of(
                SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()));
  }

  protected static NamespaceService getNamespaceService() {
    return p(NamespaceService.class).get();
  }

  protected static ReflectionServiceImpl getReflectionService() {
    return (ReflectionServiceImpl) p(ReflectionService.class).get();
  }

  protected static ReflectionStatusService getReflectionStatusService() {
    return p(ReflectionStatusService.class).get();
  }

  protected static MaterializationDescriptorProvider getMaterializationDescriptorProvider() {
    return p(MaterializationDescriptorProvider.class).get();
  }

  protected static long requestRefresh(NamespaceKey datasetKey) throws NamespaceException {
    final long requestTime = System.currentTimeMillis();
    DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    getReflectionService().requestRefresh(dataset.getId().getId());
    return requestTime;
  }

  protected static long requestRefreshWithRetry(
      NamespaceKey datasetKey, ReflectionMonitor monitor, ReflectionId rawId, Materialization m) {
    int retry = 3;
    long requestTime;
    while (true) {
      try {
        requestTime = requestRefresh(datasetKey);
        monitor.waitUntilMaterialized(rawId, m);
        break;
      } catch (Throwable t) {
        if (retry == 0) {
          Throwables.propagate(t);
        } else {
          retry--;
        }
      }
    }
    return requestTime;
  }

  protected static ReflectionMonitor newReflectionMonitor(long delay, long maxWait) {
    final MaterializationStore materializationStore =
        new MaterializationStore(p(LegacyKVStoreProvider.class));
    return new ReflectionMonitor(
        getReflectionService(),
        getReflectionStatusService(),
        getMaterializationDescriptorProvider(),
        getJobsService(),
        materializationStore,
        delay,
        maxWait);
  }

  protected static JobsService getJobsService() {
    return p(JobsService.class).get();
  }

  protected static DatasetConfig addJson(DatasetPath path) throws Exception {
    final DatasetConfig dataset =
        new DatasetConfig()
            .setId(new EntityId(UUID.randomUUID().toString()))
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
            .setFullPathList(path.toPathList())
            .setName(path.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return nsService.getDataset(path.toNamespaceKey());
  }

  protected String getQueryPlan(final String query) {
    return getQueryPlan(query, false);
  }

  protected String getQueryPlan(final String query, boolean asSystemUser) {
    final LogicalPlanCaptureListener capturePlanListener = new LogicalPlanCaptureListener();
    JobRequest jobRequest =
        JobRequest.newBuilder()
            .setSqlQuery(
                new SqlQuery(query, asSystemUser ? SystemUser.SYSTEM_USERNAME : DEFAULT_USERNAME))
            .setQueryType(QueryType.UI_RUN)
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .build();
    JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
        l(LocalJobsService.class), jobRequest, capturePlanListener);
    return capturePlanListener.getPlan();
  }

  protected static List<ReflectionField> reflectionFields(String... fields) {
    ImmutableList.Builder<ReflectionField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionPartitionField> reflectionPartitionFields(String... fields) {
    ImmutableList.Builder<ReflectionPartitionField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionPartitionField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionDimensionField> reflectionDimensionFields(String... fields) {
    ImmutableList.Builder<ReflectionDimensionField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionDimensionField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionMeasureField> reflectionMeasureFields(String... fields) {
    ImmutableList.Builder<ReflectionMeasureField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionMeasureField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionRelationship> getChosen(
      List<ReflectionRelationship> relationships) {
    if (relationships == null) {
      return Collections.emptyList();
    }

    return relationships.stream()
        .filter((r) -> r.getState() == CHOSEN)
        .collect(Collectors.toList());
  }

  protected Materialization getMaterializationFor(final ReflectionId rId) {
    final Iterable<MaterializationId> mIds =
        getMaterializationDescriptorProvider().get().stream()
            .filter(input -> input.getLayoutId().equals(rId.getId()))
            .map(descriptor -> new MaterializationId(descriptor.getMaterializationId()))
            .collect(Collectors.toList());
    assertEquals(
        "only one materialization expected, but got " + mIds.toString(), 1, Iterables.size(mIds));

    final MaterializationId mId = mIds.iterator().next();
    final Optional<Materialization> m = getReflectionService().getMaterialization(mId);
    assertTrue("materialization not found", m.isPresent());
    return m.get();
  }

  protected DatasetUI createVdsFromQuery(String query, String space, String dataset) {
    final DatasetPath datasetPath = new DatasetPath(ImmutableList.of(space, dataset));
    return createDatasetFromSQLAndSave(datasetPath, query, Collections.emptyList());
  }

  protected DatasetUI createVdsFromQuery(String query, String testSpace) {
    final String datasetName = "query" + queryNumber.getAndIncrement();
    return createVdsFromQuery(query, testSpace, datasetName);
  }

  protected ReflectionId createRawOnVds(
      String datasetId, String reflectionName, List<String> rawFields) throws Exception {
    return getReflectionService()
        .create(
            new ReflectionGoal()
                .setType(ReflectionType.RAW)
                .setDatasetId(datasetId)
                .setName(reflectionName)
                .setDetails(
                    new ReflectionDetails()
                        .setDisplayFieldList(
                            rawFields.stream()
                                .map(ReflectionField::new)
                                .collect(Collectors.toList()))));
  }

  protected void onlyAllowPeriodicWakeup(boolean periodicOnly) {
    getSabotContext()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                SYSTEM, REFLECTION_PERIODIC_WAKEUP_ONLY.getOptionName(), periodicOnly));
  }

  protected ReflectionId createRawFromQuery(
      String query, String testSpace, List<String> rawFields, String reflectionName)
      throws Exception {
    final DatasetUI datasetUI = createVdsFromQuery(query, testSpace);
    return createRawOnVds(datasetUI.getId(), reflectionName, rawFields);
  }

  protected static void setMaterializationCacheSettings(
      boolean enabled, long refreshDelayInSeconds) {
    l(ContextService.class)
        .get()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                SYSTEM, MATERIALIZATION_CACHE_ENABLED.getOptionName(), enabled));
    l(ContextService.class)
        .get()
        .getOptionManager()
        .setOption(
            OptionValue.createLong(
                SYSTEM,
                ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS.getOptionName(),
                refreshDelayInSeconds * 1000));
  }

  protected static void setEnableReAttempts(boolean enableReAttempts) {
    l(ContextService.class)
        .get()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                SYSTEM, ExecConstants.ENABLE_REATTEMPTS.getOptionName(), enableReAttempts));
  }

  protected static void setManagerRefreshDelay(long delayInSeconds) {
    setManagerRefreshDelayMs(delayInSeconds * 1000);
  }

  protected static void setManagerRefreshDelayMs(long delayInMillis) {
    l(ContextService.class)
        .get()
        .getOptionManager()
        .setOption(
            OptionValue.createLong(
                SYSTEM, REFLECTION_MANAGER_REFRESH_DELAY_MILLIS.getOptionName(), delayInMillis));
  }

  protected static void setDeletionGracePeriod(long periodInSeconds) {
    l(ContextService.class)
        .get()
        .getOptionManager()
        .setOption(
            OptionValue.createLong(
                SYSTEM, REFLECTION_DELETION_GRACE_PERIOD.getOptionName(), periodInSeconds));
  }

  protected void setDatasetAccelerationSettings(
      NamespaceKey key, long refreshPeriod, long gracePeriod) {
    setDatasetAccelerationSettings(
        key, refreshPeriod, gracePeriod, false, null, false, false, RefreshPolicyType.PERIOD);
  }

  protected void setDatasetAccelerationSettings(
      NamespaceKey key, long refreshPeriod, long gracePeriod, boolean neverExpire) {
    setDatasetAccelerationSettings(
        key, refreshPeriod, gracePeriod, false, null, neverExpire, false, RefreshPolicyType.PERIOD);
  }

  protected void setDatasetAccelerationSettings(
      NamespaceKey key,
      long refreshPeriod,
      long gracePeriod,
      boolean neverExpire,
      boolean neverRefresh,
      RefreshPolicyType refreshPolicyType) {
    setDatasetAccelerationSettings(
        key, refreshPeriod, gracePeriod, false, null, neverExpire, neverRefresh, refreshPolicyType);
  }

  protected void setDatasetAccelerationSettings(
      NamespaceKey key,
      long refreshPeriod,
      long gracePeriod,
      boolean incremental,
      String refreshField) {
    setDatasetAccelerationSettings(
        key,
        refreshPeriod,
        gracePeriod,
        incremental,
        refreshField,
        false,
        false,
        RefreshPolicyType.PERIOD);
  }

  protected void setDatasetAccelerationSettings(
      NamespaceKey key,
      long refreshPeriod,
      long gracePeriod,
      boolean incremental,
      String refreshField,
      boolean neverExpire,
      boolean neverRefresh,
      RefreshPolicyType refreshPolicyType) {
    // update dataset refresh/grace period
    getReflectionService()
        .getReflectionSettings()
        .setReflectionSettings(
            key,
            new AccelerationSettings()
                .setMethod(incremental ? RefreshMethod.INCREMENTAL : RefreshMethod.FULL)
                .setRefreshPeriod(refreshPeriod)
                .setGracePeriod(gracePeriod)
                .setRefreshField(refreshField)
                .setNeverExpire(neverExpire)
                .setNeverRefresh(neverRefresh)
                .setRefreshPolicyType(refreshPolicyType));
  }

  protected DatasetDependency dependency(
      final String datasetId, final NamespaceKey datasetKey, long snapshotId) {
    return DependencyEntry.of(datasetId, datasetKey.getPathComponents(), snapshotId, null);
  }

  protected ReflectionDependency dependency(final ReflectionId reflectionId, long snapshotId) {
    return DependencyEntry.of(reflectionId, snapshotId);
  }

  protected DatasetDependency dependency(final String datasetId, final NamespaceKey datasetKey)
      throws NamespaceException {
    long snapshotId = Long.parseLong(getCurrentSnapshotId(datasetKey).orElse("0"));
    return DependencyEntry.of(datasetId, datasetKey.getPathComponents(), snapshotId, null);
  }

  protected ReflectionDependency dependency(final ReflectionId reflectionId)
      throws NamespaceException {
    Materialization m = materializationStore.getLastMaterializationDone(reflectionId);
    NamespaceKey key =
        new NamespaceKey(
            ImmutableList.of(
                ACCELERATOR_STORAGEPLUGIN_NAME, reflectionId.getId(), m.getId().getId()));
    long snapshotId = Long.parseLong(getCurrentSnapshotId(key).orElse("0"));
    return DependencyEntry.of(reflectionId, snapshotId);
  }

  protected boolean dependsOn(ReflectionId rId, final DependencyEntry... entries) {
    final Iterable<DependencyEntry> dependencies = getReflectionService().getDependencies(rId);
    if (isEmpty(dependencies)) {
      return false;
    }
    for (DependencyEntry entry : entries) {
      if (!Iterables.contains(dependencies, entry)) {
        return false;
      }
    }
    return true;
  }

  protected void assertDependsOn(ReflectionId rId, final DependencyEntry... entries) {
    assertTrue(
        String.format(
            "Unexpected state %s",
            DependencyUtils.describeDependencies(rId, getReflectionService().getDependencies(rId))),
        dependsOn(rId, entries));
  }

  protected void assertNotDependsOn(ReflectionId rId, final DependencyEntry... entries) {
    assertFalse(
        String.format(
            "Unexpected state %s",
            DependencyUtils.describeDependencies(rId, getReflectionService().getDependencies(rId))),
        dependsOn(rId, entries));
  }

  protected String dumpState(final Materialization m) {
    return String.format(
        "%s %s",
        m,
        DependencyUtils.describeDependencies(
            m.getReflectionId(), getReflectionService().getDependencies(m.getReflectionId())));
  }

  protected void createSpace(String name) {
    Space newSpace = new Space(null, name, null, null, null);
    expectSuccess(
        getBuilder(getPublicAPI(3).path("/catalog/")).buildPost(Entity.json(newSpace)),
        new GenericType<Space>() {});
  }

  protected void deleteSpace(String name) throws NamespaceException {
    NamespaceKey key = new SpacePath(name).toNamespaceKey();
    getNamespaceService().deleteSpace(key, getNamespaceService().getSpace(key).getTag());
  }

  /**
   * Get data record of a reflection
   *
   * @param reflectionId id of a reflection
   * @return data record of the reflection
   */
  protected JobDataFragment getReflectionsData(
      JobsService jobsService, ReflectionId reflectionId, BufferAllocator allocator)
      throws Exception {
    return submitJobAndGetData(
        jobsService,
        JobRequest.newBuilder()
            .setSqlQuery(
                getQueryFromSQL(
                    "select * from sys.reflections where reflection_id = '"
                        + reflectionId.getId()
                        + "'"))
            .build(),
        0,
        1,
        allocator);
  }

  /**
   * Get materialization data of a reflection
   *
   * @param reflectionId id of a reflection
   * @return materialization data of the reflection
   */
  protected JobDataFragment getMaterializationsData(
      JobsService jobsService, ReflectionId reflectionId, BufferAllocator allocator)
      throws Exception {
    return submitJobAndGetData(
        jobsService,
        JobRequest.newBuilder()
            .setSqlQuery(
                getQueryFromSQL(
                    "select * from sys.materializations where reflection_id = '"
                        + reflectionId.getId()
                        + "'"))
            .build(),
        0,
        1,
        allocator);
  }

  /**
   * Get refresh data of a reflection
   *
   * @param reflectionId id of a reflection
   * @return refresh data of the reflection
   */
  protected JobDataFragment getRefreshesData(
      JobsService jobsService, ReflectionId reflectionId, BufferAllocator allocator)
      throws Exception {
    return submitJobAndGetData(
        jobsService,
        JobRequest.newBuilder()
            .setSqlQuery(
                getQueryFromSQL(
                    "select * from sys.materializations where reflection_id = '"
                        + reflectionId.getId()
                        + "'"))
            .build(),
        0,
        100,
        allocator);
  }

  /**
   * Get the number of written records of a materialization, which is output records shown in its
   * refresh job details
   *
   * @param m materialization of a reflection
   * @return the number of written records
   */
  protected static long getNumWrittenRecords(Materialization m) throws JobNotFoundException {
    final JobId refreshJobId = new JobId(m.getInitRefreshJobId());
    JobDetailsRequest request =
        JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(refreshJobId))
            .setUserName(SYSTEM_USERNAME)
            .build();
    final com.dremio.service.job.JobDetails refreshJob = getJobsService().getJobDetails(request);
    final JobDetails jobDetails = JobsProtoUtil.getLastAttempt(refreshJob).getDetails();
    return jobDetails.getOutputRecords();
  }

  /**
   * Ensures child materialization depends properly on parent materialization:
   *
   * <ol>
   *   <li>child materialization started after parent materialization was done
   *   <li>child reflection depends on parent materialization
   * </ol>
   */
  protected void checkReflectionDependency(Materialization parent, Materialization child)
      throws Exception {
    // child reflection should depend on its parent
    assertDependsOn(child.getReflectionId(), dependency(parent.getReflectionId()));

    JobDetailsRequest parentRefreshReflectionRequest =
        JobDetailsRequest.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId(parent.getInitRefreshJobId()).build())
            .build();
    JobDetailsRequest childRefreshReflectionRequest =
        JobDetailsRequest.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId(child.getInitRefreshJobId()).build())
            .build();

    final com.dremio.service.job.JobDetails parentRefreshReflectionJobDetails =
        getJobsService().getJobDetails(parentRefreshReflectionRequest);
    final com.dremio.service.job.JobDetails childRefreshReflectionJobDetails =
        getJobsService().getJobDetails(childRefreshReflectionRequest);

    // make sure child has been accelerated with parent's latest materialization
    AccelerationDetails details =
        AccelerationDetailsUtils.deserialize(
            JobsProtoUtil.getLastAttempt(childRefreshReflectionJobDetails)
                .getAccelerationDetails());
    List<ReflectionRelationship> chosen = getChosen(details.getReflectionRelationshipsList());
    assertTrue(
        "child refresh wasn't accelerated with parent's latest materialization",
        chosen.stream()
            .anyMatch(r -> r.getMaterialization().getId().equals(parent.getId().getId())));

    assertTrue(
        "child refresh started before parent load materialization job finished",
        JobsProtoUtil.getLastAttempt(childRefreshReflectionJobDetails).getInfo().getStartTime()
            >= JobsProtoUtil.getLastAttempt(parentRefreshReflectionJobDetails)
                .getInfo()
                .getFinishTime());
  }

  /**
   * Refresh metadata of a dataset
   *
   * @param datasetPath dataset path
   */
  protected void refreshMetadata(final String datasetPath) {
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(getQueryFromSQL("ALTER TABLE " + datasetPath + " REFRESH METADATA"))
            .build());
  }

  /**
   * Get query data with max rows
   *
   * @param query sql query
   * @param maxRows max rows of the results
   * @return results of the query
   */
  protected JobDataFragment getQueryData(
      JobsService jobsService, String query, int maxRows, BufferAllocator allocator)
      throws JobNotFoundException {
    return submitJobAndGetData(
        jobsService,
        JobRequest.newBuilder().setSqlQuery(getQueryFromSQL(query)).build(),
        0,
        maxRows,
        allocator);
  }

  /**
   * Add an external reflection
   *
   * @param spaceName space name
   * @return external reflection id
   * @throws Exception
   */
  protected ReflectionId addExternalReflection(String spaceName) throws Exception {
    final String sourceName = "src_external_reflection";

    // add a NAS source
    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(1000L);

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config1 = new NASConf();
    config1.path = folder.getRoot().getAbsolutePath();
    source.setConfig(config1);

    // create json file for PDS
    File srcFolder = folder.getRoot();

    try (PrintStream f1 = new PrintStream(new File(srcFolder.getAbsolutePath(), "file1.json"))) {
      for (int i = 0; i < 10; i++) {
        f1.println(String.format("{id:%d, colour:\"red\", price:20.0}", i));
      }
      for (int i = 10; i < 15; i++) {
        f1.println(String.format("{id:%d, colour:\"green\", price:10.0}", i));
      }
    }

    // create json file for external reflection
    try (PrintStream f2 = new PrintStream(new File(srcFolder.getAbsolutePath(), "file2.json"))) {
      f2.println("{colour:\"red\", sum_price:200.0, count_price:10}");
      f2.println("{colour:\"green\", sum_price:50.0, count_price:5}");
    }

    newSourceService().registerSourceWithRuntime(source);

    // create PDS for json file
    final DatasetPath path1 = new DatasetPath(ImmutableList.of(sourceName, "file1.json"));
    final DatasetConfig dataset1 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path1.toPathList())
            .setName(path1.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    getNamespaceService().addOrUpdateDataset(path1.toNamespaceKey(), dataset1);

    final DatasetPath path2 = new DatasetPath(ImmutableList.of(sourceName, "file2.json"));
    final DatasetConfig dataset2 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
            .setFullPathList(path2.toPathList())
            .setName(path2.getLeaf().getName())
            .setCreatedAt(System.currentTimeMillis())
            .setTag(null)
            .setOwner(DEFAULT_USERNAME)
            .setPhysicalDataset(
                new PhysicalDataset().setFormatSettings(new FileConfig().setType(FileType.JSON)));
    getNamespaceService().addOrUpdateDataset(path2.toNamespaceKey(), dataset2);
    getPreview(path2); // Promote PDS so that PDS can be found through reflection service's catalog.

    DatasetPath vdsPath = new DatasetPath(ImmutableList.of(spaceName, "vds"));
    createDatasetFromSQLAndSave(
        vdsPath,
        "SELECT colour, sum(price) as sum_price, count(price) as count_price FROM "
            + sourceName
            + ".\"file1.json\" group by colour",
        Collections.<String>emptyList());

    return getReflectionService()
        .createExternalReflection(
            "external_reflection",
            ImmutableList.of(spaceName, "vds"),
            ImmutableList.of(sourceName, "file2.json"));
  }

  protected static JobId runQuery(final String query) {
    final CompletableFuture<JobId> future = new CompletableFuture<>();
    final Thread thread =
        new Thread(
            () -> {
              final JobId jobId =
                  submitJobAndWaitUntilCompletion(
                      JobRequest.newBuilder()
                          .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
                          .setQueryType(QueryType.UI_INTERNAL_RUN)
                          .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                          .build());
              future.complete(jobId);
            });
    thread.start();

    try {
      return future.get(10, SECONDS);
    } catch (final InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail(query);
      return null;
    }
  }

  protected List<Refresh> getRefreshes(final ReflectionId id) {
    final Materialization m =
        Preconditions.checkNotNull(
            getMaterializationStore().getLastMaterializationDone(id),
            "No materialization found for reflection %s",
            id.getId());
    final FluentIterable<Refresh> refreshes = getMaterializationStore().getRefreshes(m);
    assertFalse(
        String.format("no refreshes found for reflection %s", id.getId()),
        Iterables.isEmpty(refreshes));
    return refreshes.toList();
  }

  /**
   * Get the plan for a reflection
   *
   * @param materialization materialization for the reflection
   * @return the query plan as a string
   * @throws JobNotFoundException possible exception thrown
   */
  protected static String getReflectionPlan(final Materialization materialization)
      throws JobNotFoundException {
    final QueryProfileRequest request =
        QueryProfileRequest.newBuilder()
            .setJobId(
                JobProtobuf.JobId.newBuilder().setId(materialization.getInitRefreshJobId()).build())
            .setAttempt(0)
            .setUserName(SYSTEM_USERNAME)
            .build();
    final UserBitShared.QueryProfile profile = getJobsService().getProfile(request);
    return profile.getPlan();
  }

  /**
   * Get the current latest snapshot of tableNamespaceKey
   *
   * @param tableNamespaceKey Name space key to find the table by
   * @return the latest snapshot
   * @throws NamespaceException
   */
  protected Optional<String> getCurrentSnapshotId(NamespaceKey tableNamespaceKey)
      throws NamespaceException {
    DatasetConfig dataset = getNamespaceService().getDataset(tableNamespaceKey);
    return IcebergUtils.getCurrentSnapshotId(dataset);
  }

  /** Returns the namespace key for a fully qualified table name */
  public NamespaceKey getNamespaceKey(final String fullTableName) {
    return new NamespaceKey(parseFullPath(fullTableName));
  }
}
