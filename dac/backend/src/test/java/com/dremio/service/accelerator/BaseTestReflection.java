/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.exec.server.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_DELAY_MILLIS;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

/**
 * Basic integration tests to ensure the reflection services and its components work as expected
 */
public class BaseTestReflection extends BaseTestServer {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestReflection.class);

  private static AtomicInteger queryNumber = new AtomicInteger(0);

  protected static final String TEST_SPACE = "refl_test";

  private static MaterializationStore materializationStore;

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    Assert.assertFalse(isMultinode()); // we need to connect to the master's node reflectionService to access the dependencies
    BaseTestServer.init();

    final NamespaceService nsService = getNamespaceService();
    final SpaceConfig config = new SpaceConfig().setName(TEST_SPACE);
    nsService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);

    materializationStore = new MaterializationStore(p(KVStoreProvider.class));
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

  protected Catalog cat() {
    return l(CatalogService.class).getCatalog(SchemaConfig.newBuilder(SYSTEM_USERNAME).build());
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

  protected static void requestRefresh(NamespaceKey datasetKey) throws NamespaceException {
    DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    getReflectionService().requestRefresh(dataset.getId().getId());
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
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setVersion(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON))
      );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return nsService.getDataset(path.toNamespaceKey());
  }

  protected String getQueryPlan(final String query) {
    final AtomicReference<String> plan = new AtomicReference<>("");
    final Job job = getJobsService().submitJob(JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
      .setQueryType(QueryType.UI_INTERNAL_RUN)
      .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.NONE)
      .build(), new NoOpJobStatusListener() {
      @Override
      public void planRelTransform(final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
        if (!Strings.isNullOrEmpty(plan.get())) {
          return;
        }

        if (phase == PlannerPhase.LOGICAL) {
          plan.set(RelOptUtil.dumpPlan("", after, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
        }
      }
    });

    job.getData().loadIfNecessary();
    return plan.get();
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

  protected DatasetPath createVdsFromQuery(String query, String testSpace) {
    final String datasetName = "query" + queryNumber.getAndIncrement();
    final List<String> path = Arrays.asList(testSpace, datasetName);
    final DatasetPath datasetPath = new DatasetPath(path);
    createDatasetFromSQLAndSave(datasetPath, query, Collections.<String>emptyList());
    return datasetPath;
  }

  protected ReflectionId createRawOnVds(DatasetPath datasetPath, String reflectionName, List<String> rawFields) throws Exception {
    final DatasetConfig dataset = getNamespaceService().getDataset(datasetPath.toNamespaceKey());
    return getReflectionService().create(new ReflectionGoal()
      .setType(ReflectionType.RAW)
      .setDatasetId(dataset.getId().getId())
      .setName(reflectionName)
      .setDetails(new ReflectionDetails()
        .setDisplayFieldList(
          FluentIterable.from(rawFields)
            .transform(new Function<String, ReflectionField>() {
              @Override
              public ReflectionField apply(String field) {
                return new ReflectionField(field);
              }
            }).toList()
        )
      )
    );
  }

  protected ReflectionId createRawFromQuery(String query, String testSpace, List<String> rawFields, String reflectionName) throws Exception {
    final DatasetPath datasetPath = createVdsFromQuery(query, testSpace);
    return createRawOnVds(datasetPath, reflectionName, rawFields);
  }

  protected static void setMaterializationCacheSettings(boolean enabled, long refreshDelayInSeconds) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createBoolean(SYSTEM, MATERIALIZATION_CACHE_ENABLED.getOptionName(), enabled));
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS.getOptionName(), refreshDelayInSeconds*1000));
  }

  protected static void setManagerRefreshDelay(long delayInSeconds) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, REFLECTION_MANAGER_REFRESH_DELAY_MILLIS.getOptionName(), delayInSeconds*1000));
  }

  protected static void setDeletionGracePeriod(long periodInSeconds) {
    l(ContextService.class).get().getOptionManager().setOption(
      OptionValue.createLong(SYSTEM, REFLECTION_DELETION_GRACE_PERIOD.getOptionName(), periodInSeconds));
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod) {
    setDatasetAccelerationSettings(key, refreshPeriod, gracePeriod, false, null);
  }

  protected void setDatasetAccelerationSettings(NamespaceKey key, long refreshPeriod, long gracePeriod,
                                                boolean incremental, String refreshField) {
    // update dataset refresh/grace period
    getReflectionService().getReflectionSettings().setReflectionSettings(key, new AccelerationSettings()
      .setMethod(incremental ? RefreshMethod.INCREMENTAL : RefreshMethod.FULL)
      .setRefreshPeriod(refreshPeriod)
      .setGracePeriod(gracePeriod)
      .setRefreshField(refreshField)
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



}
