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

import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.accelerator.proto.SubstitutionState.CHOSEN;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_PERIODIC_WAKEUP_ONLY;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.ws.rs.client.Entity;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionValue;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.dremio.service.jobs.NoOpJobStatusListener;
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
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
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
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Basic integration tests to ensure the reflection services and its components work as expected
 */
public class BaseTestReflection extends BaseTestServer {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestReflection.class);

  private static AtomicInteger queryNumber = new AtomicInteger(0);

  protected static final String TEST_SPACE = "refl_test";

  private static MaterializationStore materializationStore;
  private static ReflectionEntriesStore entriesStore;

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();

    final NamespaceService nsService = getNamespaceService();
    final SpaceConfig config = new SpaceConfig().setName(TEST_SPACE);
    nsService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);

    materializationStore = new MaterializationStore(p(KVStoreProvider.class));
    entriesStore = new ReflectionEntriesStore(p(KVStoreProvider.class));
  }

  @AfterClass
  public static void resetSettings()  {
    // reset deletion grace period
    setDeletionGracePeriod(HOURS.toSeconds(4));
    setManagerRefreshDelay(10);
  }

  protected static MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  protected static ReflectionEntriesStore getReflectionEntriesStore() {
    return entriesStore;
  }

  protected Catalog cat() {
    return l(CatalogService.class).getCatalog(MetadataRequestOptions.of(
        SchemaConfig.newBuilder(SYSTEM_USERNAME).build()));
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

  protected static ReflectionMonitor newReflectionMonitor(long delay, long maxWait) {
    final MaterializationStore materializationStore = new MaterializationStore(p(KVStoreProvider.class));
    return new ReflectionMonitor(getReflectionService(), getReflectionStatusService(), getMaterializationDescriptorProvider(),
      materializationStore, delay, maxWait);
  }

  protected static JobsService getJobsService() {
    return p(JobsService.class).get();
  }

  protected static DatasetConfig addJson(DatasetPath path) throws Exception {
    final DatasetConfig dataset = new DatasetConfig()
      .setId(new EntityId(UUID.randomUUID().toString()))
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON))
      );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return nsService.getDataset(path.toNamespaceKey());
  }

  protected void setSystemOption(String optionName, String optionValue) {
    final String query = String.format("ALTER SYSTEM SET \"%s\"=%s", optionName, optionValue);
    JobsServiceUtil.waitForJobCompletion(
      getJobsService().submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
          .setQueryType(QueryType.UI_INTERNAL_RUN)
          .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
          .build(), NoOpJobStatusListener.INSTANCE)
    );
  }

  protected String getQueryPlan(final String query) {
    return getQueryPlan(query, false);
  }

  protected String getQueryPlan(final String query, boolean asSystemUser) {
    final AtomicReference<String> plan = new AtomicReference<>("");
    final JobStatusListener capturePlanListener = new NoOpJobStatusListener() {
      @Override
      public void planRelTransform(final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
        if (!Strings.isNullOrEmpty(plan.get())) {
          return;
        }

        if (phase == PlannerPhase.LOGICAL) {
          plan.set(RelOptUtil.dumpPlan("", after, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
        }
      }
    };

    JobsServiceUtil.waitForJobCompletion(
      getJobsService().submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(query, asSystemUser ? SystemUser.SYSTEM_USERNAME : DEFAULT_USERNAME))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
          .build(), capturePlanListener)
    );

    return plan.get();
  }

  protected static List<ReflectionField> reflectionFields(String ... fields) {
    ImmutableList.Builder<ReflectionField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionDimensionField> reflectionDimensionFields(String ... fields) {
    ImmutableList.Builder<ReflectionDimensionField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionDimensionField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionMeasureField> reflectionMeasureFields(String ... fields) {
    ImmutableList.Builder<ReflectionMeasureField> builder = new ImmutableList.Builder<>();
    for (String field : fields) {
      builder.add(new ReflectionMeasureField(field));
    }
    return builder.build();
  }

  protected static List<ReflectionRelationship> getChosen(List<ReflectionRelationship> relationships) {
    if (relationships == null) {
      return Collections.emptyList();
    }

    return relationships.stream()
      .filter((r) -> r.getState() == CHOSEN)
      .collect(Collectors.toList());
  }

  protected Materialization getMaterializationFor(final ReflectionId rId) {
    final Iterable<MaterializationId> mIds = FluentIterable.from(getMaterializationDescriptorProvider().get())
      .filter(new Predicate<MaterializationDescriptor>() {
        @Override
        public boolean apply(MaterializationDescriptor input) {
          return input.getLayoutId().equals(rId.getId());
        }
      })
      .transform(new Function<MaterializationDescriptor, MaterializationId>() {
        @Override
        public MaterializationId apply(MaterializationDescriptor descriptor) {
          return new MaterializationId(descriptor.getMaterializationId());
        }
      });
    assertEquals("only one materialization expected, but got " + mIds.toString(), 1, Iterables.size(mIds));

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

  protected ReflectionId createRawOnVds(String datasetId, String reflectionName, List<String> rawFields) throws Exception {
    return getReflectionService().create(new ReflectionGoal()
      .setType(ReflectionType.RAW)
      .setDatasetId(datasetId)
      .setName(reflectionName)
      .setDetails(new ReflectionDetails()
        .setDisplayFieldList(rawFields.stream().map(ReflectionField::new).collect(Collectors.toList()))
      )
    );
  }

  protected void onlyAllowPeriodicWakeup(boolean periodicOnly) {
    getSabotContext().getOptionManager()
      .setOption( OptionValue.createBoolean(SYSTEM, REFLECTION_PERIODIC_WAKEUP_ONLY.getOptionName(), periodicOnly));
  }

  protected ReflectionId createRawFromQuery(String query, String testSpace, List<String> rawFields, String reflectionName) throws Exception {
    final DatasetUI datasetUI = createVdsFromQuery(query, testSpace);
    return createRawOnVds(datasetUI.getId(), reflectionName, rawFields);
  }

  protected static void setMaterializationCacheSettings(boolean enabled, long refreshDelayInSeconds) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createBoolean(SYSTEM, MATERIALIZATION_CACHE_ENABLED.getOptionName(), enabled));
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS.getOptionName(), refreshDelayInSeconds*1000));
  }

  protected static void setEnableReAttempts(boolean enableReAttempts) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createBoolean(SYSTEM, ExecConstants.ENABLE_REATTEMPTS.getOptionName(), enableReAttempts));
  }

  protected static void setManagerRefreshDelay(long delayInSeconds) {
    setManagerRefreshDelayMs(delayInSeconds*1000);
  }

  protected static void setManagerRefreshDelayMs(long delayInMillis) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, REFLECTION_MANAGER_REFRESH_DELAY_MILLIS.getOptionName(), delayInMillis));
  }

  protected static void setDeletionGracePeriod(long periodInSeconds) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, REFLECTION_DELETION_GRACE_PERIOD.getOptionName(), periodInSeconds));
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod) {
    setDatasetAccelerationSettings(key, refreshPeriod, gracePeriod, false, null, false, false);
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod, boolean neverExpire) {
    setDatasetAccelerationSettings(key, refreshPeriod, gracePeriod, false, null, neverExpire, false);
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod, boolean neverExpire, boolean neverRefresh) {
    setDatasetAccelerationSettings(key, refreshPeriod, gracePeriod, false, null, neverExpire, neverRefresh);
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod,
                                                boolean incremental, String refreshField) {
    setDatasetAccelerationSettings(key, refreshPeriod, gracePeriod, incremental, refreshField, false, false);
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod,
                                                boolean incremental, String refreshField, boolean neverExpire, boolean neverRefresh) {
    // update dataset refresh/grace period
    getReflectionService().getReflectionSettings().setReflectionSettings(key, new AccelerationSettings()
      .setMethod(incremental ? RefreshMethod.INCREMENTAL : RefreshMethod.FULL)
      .setRefreshPeriod(refreshPeriod)
      .setGracePeriod(gracePeriod)
      .setRefreshField(refreshField)
      .setNeverExpire(neverExpire)
      .setNeverRefresh(neverRefresh)
    );
  }

  protected DatasetDependency dependency(final String datasetId, final NamespaceKey datasetKey) {
    return DependencyEntry.of(datasetId, datasetKey.getPathComponents());
  }

  protected ReflectionDependency dependency(final ReflectionId reflectionId) {
    return DependencyEntry.of(reflectionId);
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

  protected void createSpace(String name) {
    expectSuccess(getBuilder(getAPIv2().path("space/" + name)).buildPut(Entity.json(new Space(null, name, null, null, null, 0, null))), Space.class);
  }

  /**
   * Get data record of a reflection
   * @param reflectionId    id of a reflection
   * @return  data record of the reflection
   * @throws Exception
   */
  protected JobDataFragment getReflectionsData(ReflectionId reflectionId) throws Exception {
    SqlQuery reflectionsQuery = getQueryFromSQL(
      String.format("select * from sys.reflections where reflection_id = '" + reflectionId.getId() + "'"));
    final JobDataFragment reflectionsData = JobUI.getJobData(
      l(JobsService.class).submitJob(JobRequest.newBuilder().setSqlQuery(reflectionsQuery).build(),
        NoOpJobStatusListener.INSTANCE)
    ).truncate(1);
    return reflectionsData;
  }

  /**
   * Get materialization data of a reflection
   * @param reflectionId    id of a reflection
   * @return  materialization data of the reflection
   * @throws Exception
   */
  protected JobDataFragment getMaterializationsData(ReflectionId reflectionId) throws Exception {
    SqlQuery materializationsQuery = getQueryFromSQL(
      String.format("select * from sys.materializations where reflection_id = '" + reflectionId.getId() + "'"));
    final JobDataFragment materializationsData = JobUI.getJobData(
      l(JobsService.class).submitJob(JobRequest.newBuilder().setSqlQuery(materializationsQuery).build(),
        NoOpJobStatusListener.INSTANCE)
    ).truncate(1);
    return materializationsData;
  }

  /**
   * Get refresh data of a reflection
   * @param reflectionId    id of a reflection
   * @return  refresh data of the reflection
   * @throws Exception
   */
  protected JobDataFragment getRefreshesData(ReflectionId reflectionId) throws Exception {
    SqlQuery refreshesQuery = getQueryFromSQL(
      String.format("select * from sys.materializations where reflection_id = '" + reflectionId.getId() + "'"));
    final JobDataFragment refreshesData = JobUI.getJobData(
      l(JobsService.class).submitJob(JobRequest.newBuilder().setSqlQuery(refreshesQuery).build(),
        NoOpJobStatusListener.INSTANCE)
    ).truncate(100);
    return refreshesData;
  }
}
