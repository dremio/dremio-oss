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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.ReflectionStatus.COMBINED_STATUS;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/** Tests {@link ReflectionStatusService} */
@RunWith(Parameterized.class)
public class TestReflectionStatusService {

  private final ReflectionId reflectionId;
  private final ReflectionStatusService statusService;

  private final COMBINED_STATUS expected;

  static class ConstantCacheViewer implements CacheViewer {
    private final boolean isCached;

    ConstantCacheViewer(boolean isCached) {
      this.isCached = isCached;
    }

    @Override
    public boolean isCached(MaterializationId id) {
      return isCached;
    }

    @Override
    public boolean isInitialized() {
      return true;
    }
  }

  public TestReflectionStatusService(
      String name,
      COMBINED_STATUS expected,
      boolean enabled,
      boolean manualRefresh,
      boolean isValid,
      ReflectionEntry entry,
      Materialization lastMaterialization,
      boolean isMaterializationCached) {

    final NamespaceService namespaceService = mock(NamespaceService.class);
    final SabotContext sabotContext = mock(SabotContext.class);
    final ReflectionGoalsStore goalsStore = mock(ReflectionGoalsStore.class);
    final ReflectionEntriesStore entriesStore = mock(ReflectionEntriesStore.class);
    final MaterializationStore materializationStore = mock(MaterializationStore.class);
    final ExternalReflectionStore externalReflectionStore = mock(ExternalReflectionStore.class);
    final ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    final ReflectionValidator validator = mock(ReflectionValidator.class);
    final CatalogService catalogService = mock(CatalogService.class);
    final JobsService jobsService = mock(JobsService.class);
    final Catalog entityExplorer = mock(Catalog.class);
    final OptionManager optionManager = mock(OptionManager.class);

    statusService =
        new ReflectionStatusServiceImpl(
            sabotContext::getExecutors,
            DirectProvider.<CacheViewer>wrap(new ConstantCacheViewer(isMaterializationCached)),
            goalsStore,
            entriesStore,
            materializationStore,
            externalReflectionStore,
            validator,
            DirectProvider.wrap(catalogService),
            DirectProvider.wrap(jobsService),
            DirectProvider.wrap(optionManager));

    reflectionId = new ReflectionId(UUID.randomUUID().toString());

    final String datasetId = UUID.randomUUID().toString();
    final List<String> dataPath = Arrays.asList("source", "folder", "dataset");
    final DatasetConfig dataset =
        new DatasetConfig().setId(new EntityId(datasetId)).setFullPathList(dataPath);
    final DremioTable table = mock(DremioTable.class);
    when(table.getDatasetConfig()).thenReturn(dataset);
    when(entityExplorer.getTable(datasetId)).thenReturn(table);
    when(catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(entityExplorer);

    final ReflectionGoal goal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setDatasetId(datasetId)
            .setState(enabled ? ReflectionGoalState.ENABLED : ReflectionGoalState.DISABLED)
            .setCreatedAt("reflection manager down".equals(name) ? 0 : System.currentTimeMillis());
    when(goalsStore.get(reflectionId)).thenReturn(goal);

    if (entry != null) {
      entry.setId(reflectionId);
      entry.setDontGiveUp(manualRefresh);
      when(entriesStore.get(reflectionId)).thenReturn(entry);
    }

    when(materializationStore.getLastMaterializationDone(reflectionId))
        .thenReturn(lastMaterialization);
    when(materializationStore.getAllDone(eq(reflectionId), Mockito.anyLong()))
        .thenReturn(Collections.singleton(lastMaterialization));

    when(validator.isValid(goal, table)).thenReturn(isValid);
    this.expected = expected;
  }

  @Test
  public void testStatus() {
    assertEquals(expected, statusService.getReflectionStatus(reflectionId).getCombinedStatus());
  }

  enum MATERIALIZATION_STATE {
    NOT_FOUND, // no last materialization
    INCOMPLETE, // last materialization has missing data partitions
    EXPIRED, // last materialization expired
    NOT_CACHED, // last materialization valid but not cached
    VALID
  }

  private static Object[] newTestCase(
      String name,
      COMBINED_STATUS expected,
      boolean enabled,
      boolean manualRefresh,
      boolean invalid,
      ReflectionState entryState,
      int numFailures,
      MATERIALIZATION_STATE materializationState,
      boolean hasCachedMaterialization) {

    // expected, enabled, manualRefresh, isValid, entry, lastMaterialization,
    // isMaterializationCached
    ReflectionEntry entry = null;
    if (entryState != null) {
      entry = new ReflectionEntry().setState(entryState).setNumFailures(numFailures);
    }

    Materialization materialization =
        new Materialization().setLastRefreshFromPds(0L).setLastRefreshDurationMillis(0L);

    if ("null materialization expiration".equals(name)) {
      materialization.setExpiration(null);
    } else {
      materialization.setExpiration(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1L));
    }

    switch (materializationState) {
      case NOT_FOUND:
        materialization = null;
        break;
      case INCOMPLETE:
        materialization.setPartitionList(
            Collections.singletonList(new DataPartition("some_address")));
        break;
      case EXPIRED:
        materialization.setExpiration(0L);
        break;
      default:
        break;
    }

    // TODO move materializationState handling to the test itself
    return new Object[] {
      name,
      expected,
      enabled,
      manualRefresh,
      !invalid,
      entry,
      materialization,
      hasCachedMaterialization
    };
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        newTestCase(
            "disabled",
            COMBINED_STATUS.DISABLED,
            false,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "disabled with manual refresh",
            COMBINED_STATUS.DISABLED,
            false,
            true,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "invalid",
            COMBINED_STATUS.INVALID,
            true,
            false,
            true,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "invalid with manual refresh",
            COMBINED_STATUS.INVALID,
            true,
            true,
            true,
            ReflectionState.REFRESHING,
            2,
            MATERIALIZATION_STATE.VALID,
            true),
        newTestCase(
            "invalid with failures",
            COMBINED_STATUS.INVALID,
            true,
            false,
            true,
            ReflectionState.FAILED,
            3,
            MATERIALIZATION_STATE.EXPIRED,
            false),
        newTestCase(
            "given up, incomplete, can accelerate",
            COMBINED_STATUS.FAILED,
            true,
            false,
            false,
            ReflectionState.FAILED,
            3,
            MATERIALIZATION_STATE.INCOMPLETE,
            true),
        newTestCase(
            "given up, expired, cannot accelerate",
            COMBINED_STATUS.FAILED,
            true,
            false,
            false,
            ReflectionState.FAILED,
            3,
            MATERIALIZATION_STATE.EXPIRED,
            false),
        newTestCase(
            "incomplete, no failures, can accelerate",
            COMBINED_STATUS.INCOMPLETE,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.INCOMPLETE,
            true),
        newTestCase(
            "incomplete, some failures, cannot accelerate",
            COMBINED_STATUS.INCOMPLETE,
            true,
            true,
            false,
            ReflectionState.ACTIVE,
            2,
            MATERIALIZATION_STATE.INCOMPLETE,
            false),
        newTestCase(
            "expired, no failures, can accelerate",
            COMBINED_STATUS.EXPIRED,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.EXPIRED,
            true),
        newTestCase(
            "expired, some failures, cannot accelerate",
            COMBINED_STATUS.EXPIRED,
            true,
            true,
            false,
            ReflectionState.ACTIVE,
            2,
            MATERIALIZATION_STATE.EXPIRED,
            false),
        newTestCase(
            "refreshing, no materialization done",
            COMBINED_STATUS.REFRESHING,
            true,
            false,
            false,
            ReflectionState.REFRESHING,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "refreshing",
            COMBINED_STATUS.REFRESHING,
            true,
            false,
            false,
            ReflectionState.REFRESHING,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "metadata refreshing",
            COMBINED_STATUS.REFRESHING,
            true,
            false,
            false,
            ReflectionState.METADATA_REFRESH,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "can accelerate, no failures",
            COMBINED_STATUS.CAN_ACCELERATE,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.VALID,
            true),
        newTestCase(
            "can accelerate, with failures",
            COMBINED_STATUS.CAN_ACCELERATE_WITH_FAILURES,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            2,
            MATERIALIZATION_STATE.VALID,
            true),
        newTestCase(
            "can accelerate, manual refresh",
            COMBINED_STATUS.CAN_ACCELERATE,
            true,
            true,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.VALID,
            true),
        newTestCase(
            "cannot accelerate, not cached",
            COMBINED_STATUS.CANNOT_ACCELERATE_SCHEDULED,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.VALID,
            false),
        newTestCase(
            "cannot accelerate, with failures",
            COMBINED_STATUS.CANNOT_ACCELERATE_SCHEDULED,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            1,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "cannot accelerate manual, no failures",
            COMBINED_STATUS.CANNOT_ACCELERATE_MANUAL,
            true,
            true,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "cannot accelerate manual, failures",
            COMBINED_STATUS.CANNOT_ACCELERATE_MANUAL,
            true,
            true,
            false,
            ReflectionState.ACTIVE,
            1,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "new goal, no entry",
            COMBINED_STATUS.CANNOT_ACCELERATE_SCHEDULED,
            true,
            false,
            false,
            null,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        newTestCase(
            "new goal, no entry, manual refresh",
            COMBINED_STATUS.CANNOT_ACCELERATE_SCHEDULED,
            true,
            true,
            false,
            null,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false),
        // These test cases will also parameterize something based on the testcase name
        newTestCase(
            "null materialization expiration",
            COMBINED_STATUS.EXPIRED,
            true,
            false,
            false,
            ReflectionState.ACTIVE,
            0,
            MATERIALIZATION_STATE.VALID,
            false),
        newTestCase(
            "reflection manager down",
            COMBINED_STATUS.FAILED,
            true,
            false,
            false,
            null,
            0,
            MATERIALIZATION_STATE.NOT_FOUND,
            false));
  }

  @Test
  public void testGetExternalReflectionStatus() throws Exception {
    final NamespaceService namespaceService = mock(NamespaceService.class);
    final SabotContext sabotContext = mock(SabotContext.class);
    final ReflectionGoalsStore goalsStore = mock(ReflectionGoalsStore.class);
    final ReflectionEntriesStore entriesStore = mock(ReflectionEntriesStore.class);
    final MaterializationStore materializationStore = mock(MaterializationStore.class);
    final ExternalReflectionStore externalReflectionStore = mock(ExternalReflectionStore.class);
    final ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    final ReflectionValidator validator = mock(ReflectionValidator.class);
    final CatalogService catalogService = mock(CatalogService.class);
    final JobsService jobsService = mock(JobsService.class);
    final OptionManager optionManager = mock(OptionManager.class);

    ReflectionStatusServiceImpl reflectionStatusService =
        new ReflectionStatusServiceImpl(
            sabotContext::getExecutors,
            DirectProvider.<CacheViewer>wrap(new ConstantCacheViewer(false)),
            goalsStore,
            entriesStore,
            materializationStore,
            externalReflectionStore,
            validator,
            DirectProvider.wrap(catalogService),
            DirectProvider.wrap(jobsService),
            DirectProvider.wrap(optionManager));

    final Catalog catalog = mock(Catalog.class);
    when(catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);
    // mock query dataset
    DatasetConfig queryDatasetConfig = new DatasetConfig();
    queryDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    Integer queryHash = DatasetHashUtils.computeDatasetHash(queryDatasetConfig, catalog, false);
    String queryDatasetId = UUID.randomUUID().toString();
    final DremioTable queryTable = mock(DremioTable.class);
    when(queryTable.getDatasetConfig()).thenReturn(queryDatasetConfig);
    when(catalog.getTable(queryDatasetId)).thenReturn(queryTable);

    // mock target dataset
    DatasetConfig targetDatasetConfig = new DatasetConfig();
    targetDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    Integer targetHash = DatasetHashUtils.computeDatasetHash(targetDatasetConfig, catalog, false);
    String targetDatasetId = UUID.randomUUID().toString();
    final DremioTable targetTable = mock(DremioTable.class);
    when(targetTable.getDatasetConfig()).thenReturn(targetDatasetConfig);
    when(catalog.getTable(targetDatasetId)).thenReturn(targetTable);

    // mock external reflection
    ReflectionId reflectionId = new ReflectionId(UUID.randomUUID().toString());
    ExternalReflection externalReflection = new ExternalReflection();
    externalReflection.setId(reflectionId.getId());
    externalReflection.setQueryDatasetId(queryDatasetId);
    externalReflection.setQueryDatasetHash(queryHash);
    externalReflection.setTargetDatasetId(targetDatasetId);
    // make the hashes not match
    externalReflection.setTargetDatasetHash(targetHash + 1);

    when(externalReflectionStore.get(reflectionId.getId())).thenReturn(externalReflection);
    // since the hashes don't match, should return OUT_OF_SYNC
    ExternalReflectionStatus externalReflectionStatus =
        reflectionStatusService.getExternalReflectionStatus(reflectionId);
    assertEquals(
        externalReflectionStatus.getConfigStatus(), ExternalReflectionStatus.STATUS.OUT_OF_SYNC);
  }
}
