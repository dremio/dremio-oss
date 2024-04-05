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

import static com.dremio.sabot.rpc.user.UserSession.MAX_METADATA_COUNT;
import static com.dremio.service.reflection.ReflectionOptions.AUTO_REBUILD_PLAN;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionServiceImpl.DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationExpander.RebuildPlanException;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.options.OptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.service.job.DeleteJobCountsRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.JobDetails;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.MaterializationPlan;
import com.dremio.service.reflection.proto.MaterializationPlanId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.protostuff.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestReflectionService {
  @Mock private SabotContext sabotContext;

  @Mock private JobsService jobsService;

  @Mock private CatalogService catalogService;

  @Mock private BufferAllocator allocator;

  @Mock private SabotConfig config;

  @Mock private OptionManager optionManager;

  @Mock private DremioConfig dremioConfig;

  @Mock private LegacyKVStoreProvider kvStoreProvider;

  @Mock private LegacyIndexedStore<ReflectionId, ReflectionGoal> reflectionGoalStore;

  @Mock private LegacyKVStore<ReflectionId, ReflectionEntry> reflectionEntryStore;

  @Mock private LegacyIndexedStore<MaterializationId, Materialization> materializationStore;

  @Mock
  private LegacyIndexedStore<MaterializationPlanId, MaterializationPlan> materializationPlanStore;

  @Mock private LegacyIndexedStore<RefreshId, Refresh> refreshStore;

  @Mock private LegacyIndexedStore<ReflectionId, ExternalReflection> externalReflectionStore;

  @Mock private ReflectionStatusService reflectionStatusService;

  @Mock private SqlConverter converter;

  @Mock private MaterializationCache.CacheHelper cacheHelper;

  private ReflectionServiceImpl service;

  @Mock private MaterializationDescriptor descriptor;

  @Mock private DremioMaterialization dremioMaterialization;

  @Mock private Catalog catalog;

  private Materialization materialization;
  private ReflectionEntry entry;
  private ReflectionGoal goal;
  private ReflectionServiceImpl.ExpansionHelper expansionHelper;
  private ReflectionServiceImpl.PlanCacheInvalidationHelper planCacheInvalidationHelper;

  @Before
  public void setup() {
    when(sabotContext.getDremioConfig()).thenReturn(dremioConfig);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    when(config.getInstance(
            "dremio.reflection.materialization.descriptor.factory",
            MaterializationDescriptorFactory.class,
            DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY))
        .thenReturn(DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY);

    when(optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)).thenReturn(true);
    when(optionManager.getOption(AUTO_REBUILD_PLAN)).thenReturn(true);
    when(optionManager.getOption(MAX_METADATA_COUNT.getOptionName()))
        .thenReturn(MAX_METADATA_COUNT.getDefault());

    service =
        new TestableReflectionServiceImpl(
            config,
            DirectProvider.wrap(kvStoreProvider),
            DirectProvider.wrap(Mockito.mock(SchedulerService.class)),
            DirectProvider.wrap(jobsService),
            DirectProvider.wrap(catalogService),
            DirectProvider.wrap(sabotContext),
            DirectProvider.wrap(reflectionStatusService),
            Mockito.mock(ExecutorService.class),
            DirectProvider.wrap(Mockito.mock(ForemenWorkManager.class)),
            false,
            allocator);

    // Test KV store objects
    ReflectionId rId = new ReflectionId("r1");
    MaterializationId mId = new MaterializationId("m1");
    materialization =
        new Materialization()
            .setId(mId)
            .setReflectionId(rId)
            .setReflectionGoalVersion("v1")
            .setState(MaterializationState.DONE)
            .setSeriesId(5L)
            .setSeriesOrdinal(0)
            .setExpiration(0L)
            .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
            .setInitRefreshSubmit(0L);
    entry = new ReflectionEntry();
    entry.setId(rId);
    ReflectionDetails details = new ReflectionDetails();
    details.setDisplayFieldList(ImmutableList.of());
    details.setDistributionFieldList(ImmutableList.of());
    details.setPartitionFieldList(ImmutableList.of());
    details.setSortFieldList(ImmutableList.of());
    goal =
        new ReflectionGoal()
            .setId(rId)
            .setTag(materialization.getReflectionGoalVersion())
            .setType(ReflectionType.RAW)
            .setDetails(details);

    when(kvStoreProvider.getStore(ReflectionGoalsStore.StoreCreator.class))
        .thenReturn(reflectionGoalStore);
    when(reflectionGoalStore.get(rId)).thenReturn(goal);
    when(kvStoreProvider.getStore(ReflectionEntriesStore.StoreCreator.class))
        .thenReturn(reflectionEntryStore);
    when(reflectionEntryStore.get(rId)).thenReturn(entry);
    when(kvStoreProvider.getStore(MaterializationStore.MaterializationStoreCreator.class))
        .thenReturn(materializationStore);
    when(materializationStore.get(mId)).thenReturn(materialization);
    when(materializationStore.find(any(LegacyIndexedStore.LegacyFindByCondition.class)))
        .thenReturn(ImmutableList.of(Maps.immutableEntry(mId, materialization)));
    when(kvStoreProvider.getStore(MaterializationStore.RefreshStoreCreator.class))
        .thenReturn(refreshStore);
    Refresh refresh = new Refresh();
    refresh.setId(new RefreshId("refresh1"));
    MaterializationMetrics metrics = new MaterializationMetrics();
    metrics.setFootprint(0L);
    metrics.setOriginalCost(0d);
    metrics.setFootprint(0L);
    refresh.setMetrics(metrics);
    JobDetails jobDetails = new JobDetails();
    jobDetails.setOutputRecords(0L);
    refresh.setJob(jobDetails);
    when(refreshStore.find(Mockito.any(LegacyIndexedStore.LegacyFindByCondition.class)))
        .thenReturn(ImmutableMap.of(refresh.getId(), refresh).entrySet());
    when(materializationStore.find(Mockito.any(LegacyIndexedStore.LegacyFindByCondition.class)))
        .thenReturn(ImmutableMap.of(materialization.getId(), materialization).entrySet());
    when(kvStoreProvider.getStore(ExternalReflectionStore.StoreCreator.class))
        .thenReturn(externalReflectionStore);

    when(kvStoreProvider.getStore(MaterializationPlanStore.MaterializationPlanStoreCreator.class))
        .thenReturn(materializationPlanStore);

    // Setup Materialization expansion helper
    expansionHelper = Mockito.mock(ReflectionServiceImpl.ExpansionHelper.class);
    planCacheInvalidationHelper =
        Mockito.mock(ReflectionServiceImpl.PlanCacheInvalidationHelper.class);
    converter = Mockito.mock(SqlConverter.class);
    when(converter.getPlannerCatalog()).thenReturn(Mockito.mock(PlannerCatalog.class));
    when(expansionHelper.getConverter()).thenReturn(converter);

    when(descriptor.getMaterializationFor(any())).thenReturn(dremioMaterialization);
    when(descriptor.getMaterializationId()).thenReturn(mId.getId());
    when(descriptor.getLayoutInfo()).thenReturn(ReflectionUtils.toReflectionInfo(goal));
    when(descriptor.getPath()).thenReturn(ImmutableList.of());

    when(catalogService.getCatalog(any())).thenReturn(catalog);
    when(catalog.getAllRequestedTables())
        .thenReturn(ImmutableList.of(Mockito.mock(DremioTable.class)));
    when(cacheHelper.getValidMaterializations()).thenReturn(ImmutableList.of(materialization));
  }

  /** Verifies that materialization cache initialization doesn't happen during start */
  @Test
  public void testStartAndMaterializationCacheInit() {
    TestableReflectionServiceImpl serviceWithMockDescriptor =
        new TestableReflectionServiceImpl(
            config,
            DirectProvider.wrap(kvStoreProvider),
            DirectProvider.wrap(Mockito.mock(SchedulerService.class)),
            DirectProvider.wrap(jobsService),
            DirectProvider.wrap(catalogService),
            DirectProvider.wrap(sabotContext),
            DirectProvider.wrap(Mockito.mock(ReflectionStatusService.class)),
            Mockito.mock(ExecutorService.class),
            DirectProvider.wrap(Mockito.mock(ForemenWorkManager.class)),
            false,
            allocator) {
          @Override
          MaterializationDescriptor getDescriptor(
              Materialization materialization,
              Catalog catalog,
              ReflectionPlanGeneratorProvider provider)
              throws MaterializationCache.CacheException {
            return TestReflectionService.this.descriptor;
          }
        };

    serviceWithMockDescriptor.start();
    assertFalse(serviceWithMockDescriptor.getCacheViewerProvider().get().isInitialized());
    verify(expansionHelper, times(0)).close();
    assertFalse(
        serviceWithMockDescriptor.getCacheViewerProvider().get().isCached(materialization.getId()));
    serviceWithMockDescriptor.refreshCache();
    assertTrue(serviceWithMockDescriptor.getCacheViewerProvider().get().isInitialized());
    assertTrue(
        serviceWithMockDescriptor.getCacheViewerProvider().get().isCached(materialization.getId()));
    verify(expansionHelper, times(1)).close();
    // Verify shared catalog cache is cleared
    verify(catalog, times(1)).clearDatasetCache(any(), any());
  }

  @Test
  public void testMaterializationCacheDisabled() {
    MaterializationCache mockCache = Mockito.mock(MaterializationCache.class);
    TestableReflectionServiceImpl serviceWithMockCache =
        new TestableReflectionServiceImpl(
            config,
            DirectProvider.wrap(kvStoreProvider),
            DirectProvider.wrap(Mockito.mock(SchedulerService.class)),
            DirectProvider.wrap(jobsService),
            DirectProvider.wrap(catalogService),
            DirectProvider.wrap(sabotContext),
            DirectProvider.wrap(Mockito.mock(ReflectionStatusService.class)),
            Mockito.mock(ExecutorService.class),
            DirectProvider.wrap(Mockito.mock(ForemenWorkManager.class)),
            false,
            allocator) {
          @Override
          MaterializationCache createMaterializationCache() {
            return mockCache;
          }
        };
    serviceWithMockCache.start();
    when(optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)).thenReturn(false);
    try {
      verify(mockCache, times(0)).resetCache();
      serviceWithMockCache.refreshCache();
      verify(mockCache, times(1)).resetCache();
      serviceWithMockCache.refreshCache();
      verify(mockCache, times(2)).resetCache();
    } finally {
      // get back to previous state
      when(optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)).thenReturn(true);
    }
  }

  @Test
  public void testDescriptorUpgradeSuccess() throws MaterializationCache.CacheException {
    ReflectionServiceImpl.ReflectionPlanGeneratorProvider provider =
        (expansionHelper,
            catalogService,
            config,
            goal,
            entry,
            materialization,
            reflectionSettings,
            materializationStore,
            dependenciesStore,
            forceFullUpdate,
            isRebuildPlan) -> {
          ReflectionPlanGenerator generator = Mockito.mock(ReflectionPlanGenerator.class);
          when(generator.getMatchingPlanBytes()).thenReturn(ByteString.bytesDefaultValue("abc"));
          return generator;
        };

    MaterializationDescriptor descriptorWithPlan =
        service.getDescriptor(materialization, catalog, provider);
    assertTrue(Arrays.equals(new byte[] {'a', 'b', 'c'}, descriptorWithPlan.getPlan()));
    verify(materializationPlanStore, times(1))
        .put(
            eq(MaterializationPlanStore.createMaterializationPlanId(materialization.getId())),
            any(MaterializationPlan.class));
    when(cacheHelper.getValidMaterializations()).thenReturn(ImmutableList.of(materialization));
    verify(expansionHelper, times(1)).close();
  }

  /**
   * Verifies that when a descriptor can't be upgraded, we mark the materialization as failed and
   * stop attempting upgrades.
   */
  @Test
  public void testDescriptorUpgradeFailure() {
    ReflectionServiceImpl.ReflectionPlanGeneratorProvider provider =
        (expansionHelper,
            catalogService,
            config,
            goal,
            entry,
            materialization,
            reflectionSettings,
            materializationStore,
            dependenciesStore,
            forceFullUpdate,
            isRebuildPlan) -> {
          ReflectionPlanGenerator generator = Mockito.mock(ReflectionPlanGenerator.class);
          when(generator.generateNormalizedPlan()).thenThrow(new RuntimeException("Boom!"));
          return generator;
        };

    assertEquals(MaterializationState.DONE, materialization.getState());
    assertRedbuildException(() -> service.getDescriptor(materialization, catalog, provider), false);
    assertEquals(MaterializationState.FAILED, materialization.getState());
    assertTrue(
        materialization
            .getFailure()
            .getMessage()
            .contains("Materialization plan upgrade: Failed to rebuild plan for"));
    verify(materializationStore, times(1)).put(any(), any());
  }

  private void assertRedbuildException(ThrowingRunnable runnable, boolean sourceDown) {
    try {
      runnable.run();
    } catch (Throwable e) {
      assertEquals(RebuildPlanException.class, e.getClass());
      assertEquals(sourceDown, ReflectionUtils.isSourceDown(e));
    }
  }

  /**
   * Verifies that when a descriptor can't be upgraded because of a down source, we continue to
   * retry upgrade.
   */
  @Test
  public void testDescriptorUpgradeWithSourceDown() {

    ReflectionServiceImpl.ReflectionPlanGeneratorProvider provider =
        (expansionHelper,
            catalogService,
            config,
            goal,
            entry,
            materialization,
            reflectionSettings,
            materializationStore,
            dependenciesStore,
            forceFullUpdate,
            isRebuildPlan) -> {
          ReflectionPlanGenerator generator = Mockito.mock(ReflectionPlanGenerator.class);
          when(generator.generateNormalizedPlan())
              .thenThrow(new RuntimeException(UserException.sourceInBadState().buildSilently()));
          return generator;
        };

    assertEquals(MaterializationState.DONE, materialization.getState());
    assertRedbuildException(() -> service.getDescriptor(materialization, catalog, provider), true);
    assertEquals(MaterializationState.DONE, materialization.getState());
    verify(materializationStore, times(0)).put(any(), any());
  }

  @Test
  public void testRemoveReflectionDeleteJobCounts() {
    service.start();
    ReflectionId reflectionId = new ReflectionId("rId");
    ReflectionDetails details = new ReflectionDetails();
    details.setDisplayFieldList(ImmutableList.of());
    details.setDistributionFieldList(ImmutableList.of());
    details.setPartitionFieldList(ImmutableList.of());
    details.setSortFieldList(ImmutableList.of());
    final ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setName("rName")
            .setTag(materialization.getReflectionGoalVersion())
            .setType(ReflectionType.RAW)
            .setState(ReflectionGoalState.ENABLED)
            .setDetails(details)
            .setDatasetId("datasetId");
    when(reflectionGoalStore.get(reflectionId))
        .thenReturn(
            new ReflectionGoal()
                .setId(reflectionId)
                .setName("rName")
                .setTag(materialization.getReflectionGoalVersion())
                .setType(ReflectionType.RAW)
                .setState(ReflectionGoalState.ENABLED)
                .setDetails(details)
                .setDatasetId("datasetId"));
    ArgumentCaptor<DeleteJobCountsRequest> deleteJobCountsRequestArgumentCaptor =
        ArgumentCaptor.forClass(DeleteJobCountsRequest.class);
    service.remove(reflectionGoal);
    verify(jobsService).deleteJobCounts(deleteJobCountsRequestArgumentCaptor.capture());

    // ASSERT
    // deleteJobCounts get the request to delete reflection when remove is called
    List<String> deleteReflectionIds =
        deleteJobCountsRequestArgumentCaptor
            .getValue()
            .getReflectionsOrBuilder()
            .getReflectionIdsList();
    assertEquals(deleteReflectionIds, Collections.singletonList(reflectionGoal.getId().getId()));
  }

  @Test
  public void testDropExternalReflectionDeleteJobCounts() {
    service.start();
    String externalReflectionId = "externalReflectionId";
    ArgumentCaptor<DeleteJobCountsRequest> deleteJobCountsRequestArgumentCaptor =
        ArgumentCaptor.forClass(DeleteJobCountsRequest.class);
    service.dropExternalReflection(externalReflectionId);
    verify(jobsService).deleteJobCounts(deleteJobCountsRequestArgumentCaptor.capture());

    // ASSERT
    // deleteJobCounts get the request to delete externalReflection when dropExternalReflection is
    // called
    List<String> deleteExternalReflectionIds =
        deleteJobCountsRequestArgumentCaptor
            .getValue()
            .getReflectionsOrBuilder()
            .getReflectionIdsList();
    assertEquals(deleteExternalReflectionIds, Collections.singletonList(externalReflectionId));
  }

  /**
   * In order to make ReflectionServiceImpl testable, we need to override methods instead of using a
   * Mockito spy. The issue with Mockito spy (which uses the decorator design pattern) is that
   * ReflectionServiceImpl inner classes can't see the spy.
   */
  private class TestableReflectionServiceImpl extends ReflectionServiceImpl {

    public TestableReflectionServiceImpl(
        SabotConfig config,
        Provider<LegacyKVStoreProvider> storeProvider,
        Provider<SchedulerService> schedulerService,
        Provider<JobsService> jobsService,
        Provider<CatalogService> catalogService,
        Provider<SabotContext> sabotContext,
        Provider<ReflectionStatusService> reflectionStatusService,
        ExecutorService executorService,
        Provider<ForemenWorkManager> foremenWorkManagerProvider,
        boolean isMaster,
        BufferAllocator allocator) {
      super(
          config,
          storeProvider,
          schedulerService,
          jobsService,
          catalogService,
          sabotContext,
          reflectionStatusService,
          executorService,
          foremenWorkManagerProvider,
          isMaster,
          allocator,
          null);
    }

    @Override
    Function<Catalog, ExpansionHelper> getExpansionHelper() {
      return new Function<Catalog, ExpansionHelper>() {
        @Override
        public ExpansionHelper apply(Catalog input) {
          return expansionHelper;
        }
      };
    }

    @Override
    Supplier<PlanCacheInvalidationHelper> getPlanCacheInvalidationHelper() {
      return new Supplier<PlanCacheInvalidationHelper>() {
        @Override
        public PlanCacheInvalidationHelper get() {
          doNothing()
              .when(planCacheInvalidationHelper)
              .invalidateReflectionAssociatedPlanCache(anyString());
          return planCacheInvalidationHelper;
        }
      };
    }

    @Override
    public Iterable<ExternalReflection> getAllExternalReflections() {
      return ImmutableList.of();
    }
  }
}
