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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.options.OptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestReflectionService {
  @Mock
  private SabotContext sabotContext;

  @Mock
  private JobsService jobsService;

  @Mock
  private CatalogService catalogService;

  @Mock
  private BufferAllocator allocator;

  @Mock
  private SabotConfig config;

  @Mock
  private OptionManager optionManager;

  @Mock
  private DremioConfig dremioConfig;

  @Mock
  private LegacyKVStoreProvider kvStoreProvider;

  @Mock
  private LegacyIndexedStore<ReflectionId, ReflectionGoal> reflectionGoalStore;

  @Mock
  private LegacyKVStore<ReflectionId, ReflectionEntry> reflectionEntryStore;

  @Mock
  private LegacyIndexedStore<MaterializationId, Materialization> materializationStore;

  private ReflectionServiceImpl service;

  private MaterializationDescriptor descriptor;

  private boolean isCoordinatorStarting;

  private Materialization materialization;
  private ReflectionEntry entry;
  private ReflectionGoal goal;
  private ReflectionServiceImpl.ExpansionHelper expansionHelper;


  @Before
  public void setup() {
    when(sabotContext.getDremioConfig()).thenReturn(dremioConfig);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    when(optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)).thenReturn(true);
    when(optionManager.getOption(AUTO_REBUILD_PLAN)).thenReturn(true);
    when(optionManager.getOption(MAX_METADATA_COUNT.getOptionName())).thenReturn(MAX_METADATA_COUNT.getDefault());

    service = new TestableReflectionServiceImpl(config,
      DirectProvider.wrap(kvStoreProvider),
      DirectProvider.wrap(Mockito.mock(SchedulerService.class)),
      DirectProvider.wrap(jobsService),
      DirectProvider.wrap(catalogService),
      DirectProvider.wrap(sabotContext),
      DirectProvider.wrap(Mockito.mock(ReflectionStatusService.class)),
      Mockito.mock(ExecutorService.class),
      DirectProvider.wrap(Mockito.mock(ForemenWorkManager.class)),
      false,
      allocator);

    // Test KV store objects
    ReflectionId rId = new ReflectionId("r1");
    MaterializationId mId = new MaterializationId("m1");
    materialization = new Materialization()
      .setId(mId)
      .setReflectionId(rId)
      .setReflectionGoalVersion("v1");
    entry = new ReflectionEntry();
    entry.setId(rId);
    ReflectionDetails details = new ReflectionDetails();
    details.setDisplayFieldList(ImmutableList.of());
    details.setDistributionFieldList(ImmutableList.of());
    details.setPartitionFieldList(ImmutableList.of());
    details.setSortFieldList(ImmutableList.of());
    goal = new ReflectionGoal()
      .setId(rId)
      .setTag(materialization.getReflectionGoalVersion())
      .setType(ReflectionType.RAW)
      .setDetails(details);

    when(kvStoreProvider.getStore(ReflectionGoalsStore.StoreCreator.class)).thenReturn(reflectionGoalStore);
    when(reflectionGoalStore.get(rId)).thenReturn(goal);
    when(kvStoreProvider.getStore(ReflectionEntriesStore.StoreCreator.class)).thenReturn(reflectionEntryStore);
    when(reflectionEntryStore.get(rId)).thenReturn(entry);
    when(kvStoreProvider.getStore(MaterializationStore.MaterializationStoreCreator.class)).thenReturn(materializationStore);
    when(materializationStore.get(mId)).thenReturn(materialization);

    // Setup Materialization expansion helper
    expansionHelper = Mockito.mock(ReflectionServiceImpl.ExpansionHelper.class);
    SqlConverter converter = Mockito.mock(SqlConverter.class);
    when(converter.getCatalog()).thenReturn(Mockito.mock(Catalog.class));
    when(converter.getTypeFactory()).thenReturn(Mockito.mock(JavaTypeFactory.class));
    when(converter.getSerializerFactory()).thenThrow(new DeserializationException(new RuntimeException("Boom!")));
    when(expansionHelper.getConverter()).thenReturn(converter);

    // Test descriptor
    descriptor = new MaterializationDescriptor(ReflectionUtils.toReflectionInfo(goal), mId.getId(), "tag",
      0, null, ImmutableList.of(), 0.0, 0, ImmutableList.of(), null,
      null, 0L, 0, catalogService);

  }

  /**
   * Verifies that reflection service won't trigger an inline metadata refresh on startup
   */
  @Test
  public void testNoInlineMetadataRefreshOnStart() {
      isCoordinatorStarting = true;
      service.start();
      verify(expansionHelper, times(2)).close();
      isCoordinatorStarting = false;
      service.refreshCache();
      verify(expansionHelper, times(4)).close();
  }

  /**
   * In order to make ReflectionServiceImpl testable, we need to override methods instead of using a Mockito spy.
   * The issue with Mockito spy (which uses the decorator design pattern) is that ReflectionServiceImpl inner classes
   * can't see the spy.
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
    Supplier<QueryContext> getQueryContext() {
      return new Supplier<QueryContext>() {
        @Override
        public QueryContext get() {
          /**
           * Validates that we set the {@link com.dremio.exec.catalog.MetadataRequestOptions#neverPromote}
           */
          Assert.assertEquals(isCoordinatorStarting, isReflectionServiceStarting);
          QueryContext context = Mockito.mock(QueryContext.class);
          return context;
        }
      };
    }

    @Override
    Supplier<ExpansionHelper> getExpansionHelper() {
      return new Supplier<ExpansionHelper>() {
        @Override
        public ExpansionHelper get() {
          when(expansionHelper.getContext()).thenReturn(getQueryContext().get());
          return expansionHelper;
        }
      };
    }

    @Override
    Iterable<Materialization> getValidMaterializations() {
      return ImmutableList.of(materialization);
    }

    @Override
    public Iterable<ExternalReflection> getAllExternalReflections() {
      return ImmutableList.of();
    }

    @Override
    MaterializationDescriptor getDescriptor(Materialization materialization) throws MaterializationCache.CacheException {
      return TestReflectionService.this.descriptor;
    }


  }
}
