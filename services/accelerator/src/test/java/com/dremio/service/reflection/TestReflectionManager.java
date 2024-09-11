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

import static com.dremio.service.reflection.proto.ReflectionState.ACTIVE;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESH;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESHING;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.types.MinorType;
import com.dremio.exec.catalog.CachingCatalog;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.plancache.PlanCacheInvalidationHelper;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.CoordinatorModeInfo;
import com.dremio.service.coordinator.SoftwareCoordinatorModeInfo;
import com.dremio.service.job.DeleteJobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.reflection.materialization.AccelerationStoragePlugin;
import com.dremio.service.reflection.materialization.AccelerationStoragePluginConfig;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalHash;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.RefreshDoneHandler;
import com.dremio.service.reflection.refresh.RefreshHandler;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test Reflection Manager */
public class TestReflectionManager {
  @Test
  public void handleGoalWithNoEntry() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    String tag = "rgTag";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setName(reflectionGoalName)
            .setTag(tag)
            .setType(ReflectionType.EXTERNAL)
            .setDatasetId(dataSetId);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(null);
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal))
        .thenReturn(reflectionGoalHash);

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).calculateReflectionGoalVersion(reflectionGoal);

    verify(subject.reflectionStore)
        .save(
            new ReflectionEntry()
                .setId(reflectionId)
                .setReflectionGoalHash(reflectionGoalHash)
                .setDatasetId(dataSetId)
                .setState(REFRESH)
                .setGoalVersion(tag)
                .setType(ReflectionType.EXTERNAL)
                .setName(reflectionGoalName)
                .setArrowCachingEnabled(true));
    verifyNoMoreInteractions(subject.reflectionStore);
  }

  @Test
  public void handleGoalWithEntryButNothingHasChanged() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setName(reflectionGoalName)
            .setType(ReflectionType.EXTERNAL)
            .setDatasetId(dataSetId)
            .setArrowCachingEnabled(true);
    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setArrowCachingEnabled(true)
            .setId(reflectionId)
            .setReflectionGoalHash(reflectionGoalHash);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry)).thenReturn(true);

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).isEqual(reflectionGoal, reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);
  }

  @Test
  public void handleGoalWithEntryButHashHasNotChangedAndBoostHasChanged() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String goalTag = "goal_tag";
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setId(reflectionId)
            .setName(reflectionGoalName)
            .setTag(goalTag)
            .setType(ReflectionType.EXTERNAL);
    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setArrowCachingEnabled(false)
            .setId(reflectionId)
            .setName("oldName")
            .setGoalVersion("old_tag")
            .setReflectionGoalHash(reflectionGoalHash)
            .setState(ReflectionState.ACTIVE);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry)).thenReturn(true);
    when(subject.materializationStore.find(reflectionId)).thenReturn(Collections.emptyList());
    when(subject.materializationStore.getLastMaterializationDone(reflectionId))
        .thenReturn(Mockito.mock(Materialization.class));

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.reflectionStore).save(reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);
    assertEquals(ReflectionState.ACTIVE, reflectionEntry.getState());
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(reflectionGoalName, reflectionEntry.getName());
    assertEquals(goalTag, reflectionEntry.getGoalVersion());
  }

  @Test
  public void handleGoalTagChangedAndEntryIsInFailedState() {
    // We currently use a noop change as a work around for getting reflections out of a failed state
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String goalTag = "goal_tag";
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setId(reflectionId)
            .setName(reflectionGoalName)
            .setTag(goalTag)
            .setState(ReflectionGoalState.ENABLED)
            .setType(ReflectionType.EXTERNAL);
    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setArrowCachingEnabled(false)
            .setId(reflectionId)
            .setName("oldName")
            .setGoalVersion("old_tag")
            .setReflectionGoalHash(reflectionGoalHash)
            .setState(ReflectionState.FAILED)
            .setNumFailures(3);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry)).thenReturn(true);
    when(subject.materializationStore.find(reflectionId)).thenReturn(Collections.emptyList());

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.reflectionStore).save(reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);
    assertEquals(ReflectionState.UPDATE, reflectionEntry.getState());
    assertEquals(Integer.valueOf(0), reflectionEntry.getNumFailures());
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(reflectionGoalName, reflectionEntry.getName());
    assertEquals(goalTag, reflectionEntry.getGoalVersion());
  }

  @Test
  public void handleGoalWithEntryButHashHasChanged() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String goalTag = "goalTag";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setId(reflectionId)
            .setName(reflectionGoalName)
            .setTag(goalTag)
            .setType(ReflectionType.EXTERNAL);
    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setArrowCachingEnabled(false)
            .setId(reflectionId)
            .setGoalVersion("old tag")
            .setReflectionGoalHash(new ReflectionGoalHash("xxx"))
            .setState(ReflectionState.ACTIVE);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry)).thenReturn(false);
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
        .thenReturn(false);
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal))
        .thenReturn(reflectionGoalHash);
    when(subject.materializationStore.find(reflectionId)).thenReturn(Collections.emptyList());

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.reflectionStore).save(reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);

    // Ensure the entry is updated appropriately
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(goalTag, reflectionEntry.getGoalVersion());
    assertEquals(reflectionGoalName, reflectionEntry.getName());
    assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    assertEquals(ReflectionState.UPDATE, reflectionEntry.getState());
  }

  @Test
  public void testSyncDoesNotUpdateReflectionWhenOnlyBoostIsToggle() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.ENABLED);

    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setReflectionGoalHash(reflectionGoalHash)
            .setArrowCachingEnabled(false)
            .setState(ReflectionState.ACTIVE);

    Materialization materialization = new Materialization().setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();

    final Catalog catalog = mock(Catalog.class);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(dataSetId)).thenReturn(dremioTable);
    Subject subject = new Subject();

    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.dependencyManager.shouldRefresh(
            reflectionEntry, 5555L, subject.dependencyResolutionContext))
        .thenReturn(false); // assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt()))
        .thenReturn(emptyList());
    when(subject.materializationStore.getLastMaterializationDone(reflectionId))
        .thenReturn(materialization);

    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);
    when(subject.materializationStore.getAllMaterializations()).thenReturn(emptyList());
    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));

    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
        .thenReturn(false); // it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
        .thenReturn(true); // but only fields not used to update the reflection

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));

    subject.reflectionManager.sync();

    // ASSERT
    verify(subject.materializationStore).save(materialization);

    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionStore).find();
    verify(subject.reflectionStore).save(reflectionEntry);
    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);
    verifyNoMoreInteractions(subject.refreshStartHandler);
    assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(true, materialization.getArrowCachingEnabled());
    assertEquals(ReflectionState.ACTIVE, reflectionEntry.getState());
  }

  @Test
  public void testSyncDoesUpdateReflectionWhenChanged() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    JobId materializationJobId = new JobId("m_job_id");
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.ENABLED);

    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setReflectionGoalHash(reflectionGoalHash)
            .setArrowCachingEnabled(false)
            .setState(ReflectionState.ACTIVE);

    Materialization materialization =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();
    final Catalog catalog = mock(Catalog.class);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(dataSetId)).thenReturn(dremioTable);
    Subject subject = new Subject();

    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.dependencyManager.shouldRefresh(
            reflectionEntry, 5555L, subject.dependencyResolutionContext))
        .thenReturn(false); // assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt()))
        .thenReturn(emptyList());
    when(subject.materializationStore.getAllDone(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllMaterializations()).thenReturn(emptyList());

    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));

    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
        .thenReturn(false); // it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
        .thenReturn(false); // but only fields not used to update the reflection
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal))
        .thenReturn(reflectionGoalHash);

    when(subject.refreshStartHandler.startJob(any(), anyLong(), any()))
        .thenReturn(materializationJobId);

    ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
    when(clusterCoordinator.getExecutorEndpoints()).thenReturn(singletonList(null));
    when(subject.sabotContext.getClusterCoordinator()).thenReturn(clusterCoordinator);

    SoftwareCoordinatorModeInfo softwareCoordinatorModeInfo = new SoftwareCoordinatorModeInfo();
    when(subject.sabotContext.getCoordinatorModeInfoProvider())
        .thenReturn(() -> softwareCoordinatorModeInfo);

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));
    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);

    subject.reflectionManager.sync();

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionStore).find();
    verify(subject.reflectionStore, times(2)).save(reflectionEntry);

    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.descriptorCache).invalidate(materialization);

    verify(subject.refreshStartHandler)
        .startJob(
            any(), anyLong(), any()); // Mockito does not support using a mix of any matchers....

    verifyNoMoreInteractions(subject.reflectionStore);
    verifyNoMoreInteractions(subject.refreshStartHandler);
    assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(false, materialization.getArrowCachingEnabled());
    assertEquals(0, reflectionEntry.getNumFailures().intValue());
    assertEquals(ReflectionState.REFRESHING, reflectionEntry.getState());
    assertEquals(MaterializationState.DEPRECATED, materialization.getState());
  }

  @Test
  public void testSyncDoesNotDeleteReflectionWhenException() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    JobId materializationJobId = new JobId("m_job_id");
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.ENABLED);

    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setReflectionGoalHash(reflectionGoalHash)
            .setArrowCachingEnabled(false)
            .setState(ReflectionState.ACTIVE);

    Materialization materialization =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();
    final Catalog catalog = mock(Catalog.class);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(dataSetId)).thenThrow(RuntimeException.class);
    Subject subject = new Subject();

    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.dependencyManager.shouldRefresh(
            reflectionEntry, 5555L, subject.dependencyResolutionContext))
        .thenReturn(false); // assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt()))
        .thenReturn(emptyList());
    when(subject.materializationStore.getAllDone(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllMaterializations()).thenReturn(emptyList());

    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));

    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
        .thenReturn(false); // it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
        .thenReturn(false); // but only fields not used to update the reflection
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal))
        .thenReturn(reflectionGoalHash);

    when(subject.refreshStartHandler.startJob(any(), anyLong(), any()))
        .thenReturn(materializationJobId);

    ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
    when(clusterCoordinator.getExecutorEndpoints()).thenReturn(singletonList(null));
    when(subject.sabotContext.getClusterCoordinator()).thenReturn(clusterCoordinator);

    SoftwareCoordinatorModeInfo softwareCoordinatorModeInfo = new SoftwareCoordinatorModeInfo();
    when(subject.sabotContext.getCoordinatorModeInfoProvider())
        .thenReturn(() -> softwareCoordinatorModeInfo);

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));
    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);

    subject.reflectionManager.sync();

    // ASSERT
    assertTrue((subject.reflectionStore).get(reflectionId).getId().equals(reflectionId));
  }

  @Test
  public void testSyncDeletesReflectionWhenNull() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    JobId materializationJobId = new JobId("m_job_id");
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.ENABLED);

    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setReflectionGoalHash(reflectionGoalHash)
            .setArrowCachingEnabled(false)
            .setState(ReflectionState.ACTIVE);

    Materialization materialization =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();
    final Catalog catalog = mock(Catalog.class);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(dataSetId)).thenReturn(null);
    Subject subject = new Subject();

    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.dependencyManager.shouldRefresh(
            reflectionEntry, 5555L, subject.dependencyResolutionContext))
        .thenReturn(false); // assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt()))
        .thenReturn(emptyList());
    when(subject.materializationStore.getAllDone(reflectionId))
        .thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllMaterializations()).thenReturn(emptyList());

    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));

    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
        .thenReturn(false); // it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
        .thenReturn(false); // but only fields not used to update the reflection
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal))
        .thenReturn(reflectionGoalHash);

    when(subject.refreshStartHandler.startJob(any(), anyLong(), any()))
        .thenReturn(materializationJobId);

    ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
    when(clusterCoordinator.getExecutorEndpoints()).thenReturn(singletonList(null));
    when(subject.sabotContext.getClusterCoordinator()).thenReturn(clusterCoordinator);

    SoftwareCoordinatorModeInfo softwareCoordinatorModeInfo = new SoftwareCoordinatorModeInfo();
    when(subject.sabotContext.getCoordinatorModeInfoProvider())
        .thenReturn(() -> softwareCoordinatorModeInfo);

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.get(reflectionGoal.getId())).thenReturn(reflectionGoal);
    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);

    subject.reflectionManager.sync();

    // ASSERT
    assertTrue(
        (subject.userStore).get(reflectionId).getState().equals(ReflectionGoalState.DELETED));
  }

  // Checking if the deleteMaterializationOrphans removes Orphan(materialization which does not have
  // the corresponding parent entry in reflectionStore)
  @Test
  public void testSyncDoesPurgeOrphans() {
    // Materialization for an existing reflection whose DROP TABLE failed
    ReflectionId existingReflectionId = new ReflectionId("r1");
    Materialization materialization =
        new Materialization()
            .setId(new MaterializationId("r1-m1"))
            .setReflectionId(existingReflectionId)
            .setArrowCachingEnabled(false)
            .setReflectionGoalVersion("test")
            .setState(MaterializationState.DELETED)
            .setModifiedAt(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));

    // Orphaned materialization whose reflection no longer exists
    ReflectionId deletedReflectionId = new ReflectionId("r2");
    Materialization materialization1 =
        new Materialization()
            .setId(new MaterializationId("r2-m1"))
            .setReflectionId(deletedReflectionId)
            .setArrowCachingEnabled(false)
            .setReflectionGoalVersion("test")
            .setState(MaterializationState.DONE);

    Materialization materialization2 =
        new Materialization()
            .setId(new MaterializationId("r2-m2"))
            .setReflectionId(deletedReflectionId)
            .setArrowCachingEnabled(false)
            .setReflectionGoalVersion("test")
            .setState(MaterializationState.DEPRECATED)
            .setModifiedAt(0L);

    Materialization materialization3 =
        new Materialization()
            .setId(new MaterializationId("r2-m3"))
            .setReflectionId(deletedReflectionId)
            .setArrowCachingEnabled(false)
            .setReflectionGoalVersion("test")
            .setState(MaterializationState.DELETED);

    Subject subject = new Subject();

    JobSubmission submission = Mockito.mock(JobSubmission.class);
    when(submission.getJobId()).thenReturn(new JobId());
    when(subject.jobsService.submitJob(any(), any())).thenReturn(submission);

    when(subject.optionManager.getOption(ReflectionOptions.MATERIALIZATION_ORPHAN_REFRESH))
        .thenReturn(TimeUnit.HOURS.toSeconds(4));
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD))
        .thenReturn(TimeUnit.HOURS.toSeconds(4));
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_DELETION_NUM_ENTRIES))
        .thenReturn(3L);
    when(subject.optionManager.getOption(
            ReflectionOptions.SKIP_DROP_TABLE_JOB_FOR_INCREMENTAL_REFRESH))
        .thenReturn(true);

    when(subject.userStore.getAllNotDeleted()).thenReturn(emptyList());
    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());
    when(subject.reflectionStore.find()).thenReturn(emptyList());
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong())).thenReturn(emptyList());

    // Used only by orphan purger
    when(subject.materializationStore.getAllMaterializations())
        .thenReturn(
            Arrays.asList(materialization, materialization1, materialization2, materialization3));

    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt()))
        .thenReturn(emptyList());
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);

    DependencyManager dependencyManager =
        new DependencyManager(
            subject.materializationStore,
            subject.reflectionStore,
            subject.optionManager,
            new DependencyGraph(subject.dependenciesStore));
    subject.dependencyManager = dependencyManager;

    ReflectionManager reflectionManager = subject.newReflectionManager();

    // Test
    reflectionManager.sync();

    // ASSERT
    verify(subject.materializationStore).getAllMaterializations();
    verify(subject.materializationStore, times(3)).save(any(Materialization.class));
    verify(subject.reflectionStore, times(1)).find();
    verify(subject.reflectionStore, times(0)).contains(deletedReflectionId);
    verify(subject.reflectionStore, times(3)).get(any(ReflectionId.class));
    verify(subject.materializationStore, times(3))
        .getRefreshesExclusivelyOwnedBy(any(Materialization.class));
    verify(subject.jobsService, times(3)).submitJob(any(), any());
    verifyNoMoreInteractions(subject.reflectionStore);
    assertEquals(MaterializationState.DELETED, materialization1.getState());
    assertEquals(MaterializationState.DELETED, materialization2.getState());
    assertEquals(MaterializationState.DELETED, materialization3.getState());
  }

  @Test
  public void testIcebergIncrementalRefreshJobFailedAfterCommit() throws Exception {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setState(MaterializationState.RUNNING)
            .setIsIcebergDataset(true)
            .setBasePath("/base/path")
            .setPreviousIcebergSnapshot(1L);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setType(ReflectionType.RAW);

    JobDetails job = createJobDetails();

    JobId jobId = new JobId().setId("m_job_id");
    entry.setRefreshJobId(jobId);

    IcebergModel icebergModel = mock(IcebergModel.class);
    FileSelection fileSelection = mock(FileSelection.class);
    Table icebergTable = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    ManageSnapshots manageSnapshots = mock(ManageSnapshots.class);
    long newSnapshotId = 2; // committed new snapshot

    Subject subject = new Subject();

    when(subject.materializationStore.getLastMaterialization(reflectionId)).thenReturn(m);
    when(subject.jobsService.getJobDetails(any())).thenReturn(job);
    when(subject.dependencyManager.reflectionHasKnownDependencies(any())).thenReturn(false);
    when(subject.accelerationPlugin.getIcebergModel()).thenReturn(icebergModel);
    when(subject.accelerationPlugin.getIcebergFileSelection(anyString())).thenReturn(fileSelection);
    when(icebergModel.getIcebergTable(any())).thenReturn(icebergTable);
    when(icebergTable.currentSnapshot()).thenReturn(snapshot);
    when(snapshot.snapshotId()).thenReturn(newSnapshotId);
    when(icebergTable.manageSnapshots()).thenReturn(manageSnapshots);
    when(manageSnapshots.rollbackTo(anyLong())).thenReturn(manageSnapshots);
    AccelerationStoragePluginConfig pluginConfig =
        Mockito.mock(AccelerationStoragePluginConfig.class);
    when(pluginConfig.getPath()).thenReturn(Path.of("."));
    when(subject.accelerationPlugin.getConfig()).thenReturn(pluginConfig);
    when(subject.catalogService.getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME))
        .thenReturn(subject.accelerationPlugin);

    // Test
    subject.reflectionManager.handleEntry(
        entry, 0, new ReflectionManager.EntryCounts(), subject.dependencyResolutionContext);

    assertEquals(MaterializationState.FAILED, m.getState());
    assertEquals(
        "Reflection Job failed without reporting an error message", m.getFailure().getMessage());

    verify(icebergTable, times(1)).manageSnapshots();
    verify(manageSnapshots, times(1)).rollbackTo(1L);
    verify(manageSnapshots, times(1)).commit();
  }

  private JobDetails createJobDetails() {
    RefreshDecision refreshDecision = new RefreshDecision().setInitialRefresh(false);

    JobProtobuf.ExtraInfo extraInfo =
        JobProtobuf.ExtraInfo.newBuilder()
            .setName(RefreshDecision.class.getName())
            .setData(
                ByteString.copyFrom(RefreshHandler.ABSTRACT_SERIALIZER.serialize(refreshDecision)))
            .build();

    JobProtobuf.JobInfo jobInfo =
        JobProtobuf.JobInfo.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setSql("sql")
            .setDatasetVersion("version")
            .setQueryType(JobProtobuf.QueryType.ACCELERATOR_CREATE)
            .build();

    JobProtobuf.JobAttempt jobAttempt =
        JobProtobuf.JobAttempt.newBuilder()
            .setState(JobProtobuf.JobState.FAILED)
            .setInfo(jobInfo)
            .addExtraInfo(extraInfo)
            .build();

    JobDetails job =
        JobDetails.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setCompleted(true)
            .addAttempts(jobAttempt)
            .build();
    return job;
  }

  @Test
  public void testIcebergIncrementalRefreshJobFailedBeforeCommit() throws Exception {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setState(MaterializationState.RUNNING)
            .setIsIcebergDataset(true)
            .setBasePath("/base/path")
            .setPreviousIcebergSnapshot(1L);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setType(ReflectionType.RAW)
            .setState(ReflectionState.REFRESHING);

    JobDetails job = createJobDetails();

    JobId jobId = new JobId().setId("jobid");
    entry.setRefreshJobId(jobId);

    IcebergModel icebergModel = mock(IcebergModel.class);
    FileSelection fileSelection = mock(FileSelection.class);
    Table icebergTable = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    ManageSnapshots manageSnapshots = mock(ManageSnapshots.class);
    long newSnapshotId = 1; // didn't commit

    Subject subject = new Subject();

    when(subject.materializationStore.getLastMaterialization(reflectionId)).thenReturn(m);
    when(subject.jobsService.getJobDetails(any())).thenReturn(job);
    when(subject.dependencyManager.reflectionHasKnownDependencies(any())).thenReturn(false);
    when(subject.accelerationPlugin.getIcebergModel()).thenReturn(icebergModel);
    when(subject.accelerationPlugin.getIcebergFileSelection(anyString())).thenReturn(fileSelection);
    when(icebergModel.getIcebergTable(any())).thenReturn(icebergTable);
    when(icebergTable.currentSnapshot()).thenReturn(snapshot);
    when(snapshot.snapshotId()).thenReturn(newSnapshotId);
    when(icebergTable.manageSnapshots()).thenReturn(manageSnapshots);
    when(manageSnapshots.rollbackTo(anyLong())).thenReturn(manageSnapshots);
    AccelerationStoragePluginConfig pluginConfig =
        Mockito.mock(AccelerationStoragePluginConfig.class);
    when(pluginConfig.getPath()).thenReturn(Path.of("."));
    when(subject.accelerationPlugin.getConfig()).thenReturn(pluginConfig);
    when(subject.catalogService.getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME))
        .thenReturn(subject.accelerationPlugin);

    // Test
    subject.reflectionManager.handleEntry(
        entry, 0, new ReflectionManager.EntryCounts(), subject.dependencyResolutionContext);

    assertEquals(MaterializationState.FAILED, m.getState());
    assertEquals(
        "Reflection Job failed without reporting an error message", m.getFailure().getMessage());
    verify(icebergTable, times(0)).manageSnapshots();
    verify(manageSnapshots, times(0)).rollbackTo(1L);
    verify(manageSnapshots, times(0)).commit();
  }

  @Test
  public void testStartRefreshForIcebergReflection() throws Exception {
    ReflectionId reflectionId = new ReflectionId("r_id");

    ReflectionEntry reflectionEntry = new ReflectionEntry().setId(reflectionId).setState(REFRESH);

    Refresh refresh = new Refresh().setIsIcebergRefresh(true).setBasePath("/basepath");

    JobId jobId = new JobId().setId("jobid");

    IcebergModel icebergModel = mock(IcebergModel.class);
    FileSelection fileSelection = mock(FileSelection.class);
    Table icebergTable = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    long snapshotId = new Random().nextLong();

    CoordinatorModeInfo coordinatorModeInfo = mock(CoordinatorModeInfo.class);

    Subject subject = new Subject();

    when(subject.materializationStore.getRefreshesByReflectionId(reflectionId))
        .thenReturn(FluentIterable.from(Collections.singletonList(refresh)));
    when(subject.accelerationPlugin.getIcebergModel()).thenReturn(icebergModel);
    when(subject.accelerationPlugin.getIcebergFileSelection(anyString())).thenReturn(fileSelection);
    when(subject.sabotContext.getCoordinatorModeInfoProvider())
        .thenReturn(() -> coordinatorModeInfo);
    when(subject.refreshStartHandler.startJob(eq(reflectionEntry), anyLong(), eq(snapshotId)))
        .thenReturn(jobId);
    when(icebergModel.getIcebergTable(any())).thenReturn(icebergTable);
    when(icebergTable.currentSnapshot()).thenReturn(snapshot);
    when(snapshot.snapshotId()).thenReturn(snapshotId);
    AccelerationStoragePluginConfig pluginConfig =
        Mockito.mock(AccelerationStoragePluginConfig.class);
    when(pluginConfig.getPath()).thenReturn(Path.of("."));
    when(subject.accelerationPlugin.getConfig()).thenReturn(pluginConfig);
    when(subject.catalogService.getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME))
        .thenReturn(subject.accelerationPlugin);

    // Test
    subject.reflectionManager.handleEntry(
        reflectionEntry,
        0,
        new ReflectionManager.EntryCounts(),
        subject.dependencyResolutionContext);

    assertEquals(REFRESHING, reflectionEntry.getState());
    verify(icebergTable, times(1)).currentSnapshot();
    verify(subject.refreshStartHandler, times(1))
        .startJob(eq(reflectionEntry), anyLong(), eq(snapshotId));
  }

  @Test
  public void testStartRefreshForNonIcebergReflection() throws Exception {
    ReflectionId reflectionId = new ReflectionId("r_id");

    ReflectionEntry reflectionEntry = new ReflectionEntry().setId(reflectionId).setState(REFRESH);

    Refresh refresh = new Refresh().setIsIcebergRefresh(false);

    JobId jobId = new JobId().setId("jobid");

    CoordinatorModeInfo coordinatorModeInfo = mock(CoordinatorModeInfo.class);

    Subject subject = new Subject();

    when(subject.materializationStore.getRefreshesByReflectionId(reflectionId))
        .thenReturn(FluentIterable.from(Collections.singletonList(refresh)));
    when(subject.sabotContext.getCoordinatorModeInfoProvider())
        .thenReturn(() -> coordinatorModeInfo);
    when(subject.refreshStartHandler.startJob(eq(reflectionEntry), anyLong(), any()))
        .thenReturn(jobId);

    // Test
    subject.reflectionManager.handleEntry(
        reflectionEntry,
        0,
        new ReflectionManager.EntryCounts(),
        subject.dependencyResolutionContext);

    assertEquals(REFRESHING, reflectionEntry.getState());
    verify(subject.refreshStartHandler, times(1))
        .startJob(eq(reflectionEntry), anyLong(), eq(null));
  }

  @Test
  public void testJobNotFoundException() throws Exception {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setState(MaterializationState.RUNNING);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setType(ReflectionType.RAW)
            .setRefreshJobId(new JobId().setId("m_job_id"));

    Subject subject = new Subject();
    when(subject.jobsService.getJobDetails(any()))
        .thenThrow(new JobNotFoundException(new JobId("m_job_id"), "something bad"));
    when(subject.materializationStore.getLastMaterialization(reflectionId)).thenReturn(m);

    // Test
    subject.reflectionManager.handleEntry(
        entry, 0, new ReflectionManager.EntryCounts(), subject.dependencyResolutionContext);

    assertEquals(MaterializationState.FAILED, m.getState());
    assertEquals("Unable to retrieve job m_job_id: something bad", m.getFailure().getMessage());
  }

  @Test
  public void testRefreshDoneHandlerException() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setState(MaterializationState.RUNNING);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setRefreshJobId(new JobId().setId("j_id"));

    Subject subject = new Subject();

    JobDetails job = createJobDetails();

    PlanCacheInvalidationHelper helper = Mockito.mock(PlanCacheInvalidationHelper.class);
    RefreshDoneHandler handler =
        new RefreshDoneHandler(
            entry,
            m,
            job,
            subject.jobsService,
            subject.materializationStore,
            subject.materializationPlanStore,
            subject.dependencyManager,
            subject.expansionHelper,
            Path.of("."),
            subject.allocator,
            subject.catalogService,
            subject.dependencyResolutionContext,
            subject.sabotConfig);

    when(subject.planCacheInvalidationHelper.get()).thenReturn(helper);
    subject.reflectionManager.handleSuccessfulJob(entry, m, job, handler);

    assertEquals(MaterializationState.FAILED, m.getState());
    assertTrue(
        m.getFailure()
            .getMessage()
            .contains(
                "Failed to handle successful REFLECTION REFRESH job m_job_id: "
                    + "Cannot handle job with non completed state FAILED"));
  }

  @Test
  public void testCachedReflectionSettings() {

    Subject subject = new Subject();
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_SYNC_CACHE))
        .thenReturn(true);
    ReflectionSettings settings = Mockito.mock(ReflectionSettings.class);
    RefreshRequestsStore requestsStore = Mockito.mock(RefreshRequestsStore.class);
    DependencyResolutionContextFactory factory =
        new DependencyResolutionContextFactory(settings, requestsStore, subject.optionManager);
    DependencyResolutionContext context = factory.create();
    final CatalogEntityKey key =
        CatalogEntityKey.fromNamespaceKey(new NamespaceKey(ImmutableList.of("root", "path")));
    final AccelerationSettings testSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL);
    when(settings.getReflectionSettings(key)).thenReturn(testSettings);

    // Test
    AccelerationSettings accelerationSettings = context.getReflectionSettings(key);
    assertEquals(testSettings.getMethod(), accelerationSettings.getMethod());
    verify(settings, times(1)).getReflectionSettings(key);
    accelerationSettings = context.getReflectionSettings(key);
    assertEquals(testSettings.getMethod(), accelerationSettings.getMethod());
    verify(settings, times(1)).getReflectionSettings(key); // Retrieved from cache
    assertTrue(context.hasAccelerationSettingsChanged());

    // Verify hasAccelerationSettingsChanged
    context = factory.create();
    assertFalse(context.hasAccelerationSettingsChanged());
    when(settings.getAllHash()).thenReturn(123);
    context = factory.create();
    assertTrue(context.hasAccelerationSettingsChanged());
  }

  /**
   * Verifies reflection settings cache on two different datasets with the same path but different
   * version context
   */
  @Test
  public void testCachedVersionedReflectionSettings() {

    Subject subject = new Subject();
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_SYNC_CACHE))
        .thenReturn(true);
    ReflectionSettings settings = Mockito.mock(ReflectionSettings.class);
    RefreshRequestsStore requestsStore = Mockito.mock(RefreshRequestsStore.class);
    DependencyResolutionContextFactory factory =
        new DependencyResolutionContextFactory(settings, requestsStore, subject.optionManager);
    DependencyResolutionContext context = factory.create();

    final CatalogEntityKey keyAtProd =
        CatalogEntityKey.newBuilder()
            .keyComponents(ImmutableList.of("root", "path"))
            .tableVersionContext(new TableVersionContext(TableVersionType.BRANCH, "prod"))
            .build();
    final AccelerationSettings testSettingsAtProd =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshPeriod(11111L);
    when(settings.getReflectionSettings(keyAtProd)).thenReturn(testSettingsAtProd);

    final CatalogEntityKey keyAtStaging =
        CatalogEntityKey.newBuilder()
            .keyComponents(ImmutableList.of("root", "path"))
            .tableVersionContext(new TableVersionContext(TableVersionType.BRANCH, "staging"))
            .build();
    final AccelerationSettings testSettingsAtStaging =
        new AccelerationSettings().setMethod(RefreshMethod.FULL).setRefreshPeriod(222222L);
    when(settings.getReflectionSettings(keyAtStaging)).thenReturn(testSettingsAtStaging);

    // Test
    AccelerationSettings accelerationSettings = context.getReflectionSettings(keyAtProd);
    assertEquals(testSettingsAtProd.getMethod(), accelerationSettings.getMethod());
    assertEquals(testSettingsAtProd.getRefreshPeriod(), accelerationSettings.getRefreshPeriod());
    verify(settings, times(1)).getReflectionSettings(keyAtProd);
    accelerationSettings = context.getReflectionSettings(keyAtProd);
    assertEquals(testSettingsAtProd.getMethod(), accelerationSettings.getMethod());
    assertEquals(testSettingsAtProd.getRefreshPeriod(), accelerationSettings.getRefreshPeriod());
    verify(settings, times(1)).getReflectionSettings(keyAtProd); // Retrieved from cache
    assertTrue(context.hasAccelerationSettingsChanged());

    accelerationSettings = context.getReflectionSettings(keyAtStaging);
    assertEquals(testSettingsAtStaging.getMethod(), accelerationSettings.getMethod());
    assertEquals(testSettingsAtStaging.getRefreshPeriod(), accelerationSettings.getRefreshPeriod());
    verify(settings, times(1)).getReflectionSettings(keyAtStaging);
    accelerationSettings = context.getReflectionSettings(keyAtStaging);
    assertEquals(testSettingsAtStaging.getMethod(), accelerationSettings.getMethod());
    assertEquals(testSettingsAtStaging.getRefreshPeriod(), accelerationSettings.getRefreshPeriod());
    verify(settings, times(1)).getReflectionSettings(keyAtStaging); // Retrieved from cache

    // Verify hasAccelerationSettingsChanged
    context = factory.create();
    assertFalse(context.hasAccelerationSettingsChanged());
    when(settings.getAllHash()).thenReturn(123);
    context = factory.create();
    assertTrue(context.hasAccelerationSettingsChanged());
  }

  @Test
  public void testSyncWithDependencyResolutionContext() {

    Subject subject = new Subject();

    ReflectionSettings settings = Mockito.mock(ReflectionSettings.class);
    RefreshRequestsStore requestsStore = Mockito.mock(RefreshRequestsStore.class);
    DependencyResolutionContextFactory factory =
        new DependencyResolutionContextFactory(settings, requestsStore, subject.optionManager);

    subject.contextFactory = factory;

    ReflectionManager reflectionManager = subject.newReflectionManager();

    ReflectionId reflectionId = new ReflectionId("r_id");

    ReflectionEntry reflectionEntry =
        new ReflectionEntry().setId(reflectionId).setState(ACTIVE).setLastSuccessfulRefresh(7L);

    when(subject.reflectionStore.find())
        .thenReturn(FluentIterable.from(Collections.singletonList(reflectionEntry)));

    // Enable sync cache and sync
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_SYNC_CACHE))
        .thenReturn(true);
    reflectionManager.sync();
    DependencyResolutionContext context = factory.create(); // Caching context
    verify(subject.reflectionStore, times(0)).get(any());

    // Disable sync cache, add another reflection and sync
    when(subject.optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_SYNC_CACHE))
        .thenReturn(false);

    ReflectionId reflectionId2 = new ReflectionId("r_id2");
    ReflectionEntry reflectionEntry2 =
        new ReflectionEntry().setId(reflectionId).setState(ACTIVE).setLastSuccessfulRefresh(8L);
    when(subject.reflectionStore.find())
        .thenReturn(FluentIterable.from(Arrays.asList(reflectionEntry, reflectionEntry2)));
    reflectionManager.sync();
    context = factory.create(); // Non caching context
    when(subject.reflectionStore.get(reflectionId2)).thenReturn(reflectionEntry2);
  }

  @Test
  public void testRefreshDoneHandlerLastRefreshDuration()
      throws DependencyGraph.DependencyException, NamespaceException {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setInitRefreshSubmit(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1))
            .setState(MaterializationState.RUNNING)
            .setIsIcebergDataset(false);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setRefreshJobId(new JobId().setId("j_id"));

    final AccelerationSettings testSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);

    RefreshDecision refreshDecision =
        new RefreshDecision()
            .setInitialRefresh(false)
            .setAccelerationSettings(testSettings)
            .setSeriesId(0L)
            .setLogicalPlan(io.protostuff.ByteString.EMPTY);

    JobProtobuf.ExtraInfo extraInfo =
        JobProtobuf.ExtraInfo.newBuilder()
            .setName(RefreshDecision.class.getName())
            .setData(
                ByteString.copyFrom(RefreshHandler.ABSTRACT_SERIALIZER.serialize(refreshDecision)))
            .build();

    JobProtobuf.JobInfo jobInfo =
        JobProtobuf.JobInfo.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setSql("sql")
            .setDatasetVersion("version")
            .setQueryType(JobProtobuf.QueryType.ACCELERATOR_CREATE)
            .build();

    JobProtobuf.JobAttempt jobAttempt =
        JobProtobuf.JobAttempt.newBuilder()
            .setState(JobProtobuf.JobState.COMPLETED)
            .setInfo(jobInfo)
            .addExtraInfo(extraInfo)
            .setStats(JobProtobuf.JobStats.newBuilder().setOutputRecords(4L).build())
            .setAttemptId("1be6174f-8e89-9244-643a-565b81142700")
            .build();

    JobDetails job =
        JobDetails.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setCompleted(true)
            .addAttempts(jobAttempt)
            .build();

    Subject subject = new Subject();
    RefreshDoneHandler handler =
        new RefreshDoneHandler(
            entry,
            m,
            job,
            subject.jobsService,
            subject.materializationStore,
            subject.materializationPlanStore,
            subject.dependencyManager,
            subject.expansionHelper,
            Path.of("."),
            subject.allocator,
            subject.catalogService,
            subject.dependencyResolutionContext,
            subject.sabotConfig);

    com.dremio.service.reflection.proto.JobDetails jobd =
        new com.dremio.service.reflection.proto.JobDetails();
    jobd.setOutputRecords(4L);

    when(subject.dependencyManager.getOldestDependentMaterialization(any()))
        .thenReturn(Optional.of(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30)));

    when(subject.dependencyManager.getGracePeriod(any(), any()))
        .thenReturn(Optional.of(Long.MAX_VALUE));

    when(subject.materializationStore.getRefreshes(any()))
        .thenReturn(FluentIterable.from(ImmutableList.<Refresh>of()));

    try (MockedStatic<ReflectionUtils> mocked = Mockito.mockStatic(ReflectionUtils.class)) {
      mocked.when(() -> ReflectionUtils.computeJobDetails(any())).thenReturn(jobd);
      mocked
          .when(() -> ReflectionUtils.computeMetrics(any(), any(), any(), any()))
          .thenReturn(null);
      mocked
          .when(
              () ->
                  ReflectionUtils.createRefresh(
                      any(),
                      any(),
                      anyLong(),
                      anyInt(),
                      any(),
                      any(),
                      any(),
                      any(),
                      anyBoolean(),
                      any()))
          .thenReturn(null);
      handler.handle();
    }

    assertEquals(
        m.getLastRefreshFinished() - m.getInitRefreshSubmit(),
        m.getLastRefreshDurationMillis().longValue());
  }

  /**
   * Verifies that RefreshDecision.baseTableSnapshotIda is properly extracted and saved into
   * sys.refreshes
   */
  @Test
  public void testRefreshDoneHandlerCreateAndSaveRefreshSnapshotBased()
      throws DependencyGraph.DependencyException, NamespaceException {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setInitRefreshSubmit(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1))
            .setBasePath("abc")
            .setState(MaterializationState.RUNNING)
            .setIsIcebergDataset(true);

    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setRefreshJobId(new JobId().setId("j_id"));

    final AccelerationSettings testSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setSnapshotBased(true);

    RefreshDecision refreshDecision =
        new RefreshDecision()
            .setInitialRefresh(false)
            .setAccelerationSettings(testSettings)
            .setSeriesId(0L)
            .setLogicalPlan(io.protostuff.ByteString.EMPTY)
            .setOutputUpdateId(
                new UpdateId()
                    .setStringUpdateId("12345")
                    .setType(MinorType.VARCHAR)
                    .setUpdateIdType(UpdateId.IdType.SNAPSHOT));

    JobProtobuf.ExtraInfo extraInfo =
        JobProtobuf.ExtraInfo.newBuilder()
            .setName(RefreshDecision.class.getName())
            .setData(
                ByteString.copyFrom(RefreshHandler.ABSTRACT_SERIALIZER.serialize(refreshDecision)))
            .build();

    JobProtobuf.JobInfo jobInfo =
        JobProtobuf.JobInfo.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setSql("sql")
            .setDatasetVersion("version")
            .setQueryType(JobProtobuf.QueryType.ACCELERATOR_CREATE)
            .build();

    JobProtobuf.JobAttempt jobAttempt =
        JobProtobuf.JobAttempt.newBuilder()
            .setState(JobProtobuf.JobState.COMPLETED)
            .setInfo(jobInfo)
            .addExtraInfo(extraInfo)
            .setStats(JobProtobuf.JobStats.newBuilder().setOutputRecords(4L).build())
            .setAttemptId("1be6174f-8e89-9244-643a-565b81142700")
            .build();

    JobDetails job =
        JobDetails.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setCompleted(true)
            .addAttempts(jobAttempt)
            .build();

    Subject subject = new Subject();
    RefreshDoneHandler handler =
        new RefreshDoneHandler(
            entry,
            m,
            job,
            subject.jobsService,
            subject.materializationStore,
            subject.materializationPlanStore,
            subject.dependencyManager,
            subject.expansionHelper,
            Path.of("."),
            subject.allocator,
            subject.catalogService,
            subject.dependencyResolutionContext,
            subject.sabotConfig);

    com.dremio.service.reflection.proto.JobDetails jobd =
        new com.dremio.service.reflection.proto.JobDetails();
    jobd.setOutputRecords(4L);

    when(subject.dependencyManager.getOldestDependentMaterialization(any()))
        .thenReturn(Optional.of(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30)));

    when(subject.dependencyManager.getGracePeriod(any(), any()))
        .thenReturn(Optional.of(Long.MAX_VALUE));

    when(subject.materializationStore.getRefreshes(any()))
        .thenReturn(FluentIterable.from(ImmutableList.<Refresh>of()));

    try (MockedStatic<ReflectionUtils> mocked =
        Mockito.mockStatic(ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(() -> ReflectionUtils.computeJobDetails(any())).thenReturn(jobd);
      mocked
          .when(() -> ReflectionUtils.computeMetrics(any(), any(), any(), any()))
          .thenReturn(null);
      // TEST
      handler.handle();
    }
    // ASSERT
    ArgumentCaptor<Refresh> argument = ArgumentCaptor.forClass(Refresh.class);
    verify(subject.materializationStore).save(argument.capture());
    assertEquals("12345", argument.getValue().getUpdateId().getStringUpdateId());
    assertEquals(UpdateId.IdType.SNAPSHOT, argument.getValue().getUpdateId().getUpdateIdType());
  }

  /**
   * Verifies that the same caching catalog is used across all table lookups when handling deleted
   * datasets
   */
  @Test
  public void testHandleDeletedDatasetsUsesCachingCatalog() {
    final String datasetId = "d1";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(new ReflectionId("r_id"))
            .setDatasetId(datasetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.DISABLED);

    ReflectionGoal reflectionGoal2 =
        new ReflectionGoal()
            .setId(new ReflectionId("r_id2"))
            .setDatasetId(datasetId)
            .setType(ReflectionType.EXTERNAL)
            .setState(ReflectionGoalState.DISABLED);

    final Catalog catalog = mock(Catalog.class);
    CachingCatalog cachingCatalog = new CachingCatalog(catalog);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getPath()).thenReturn(new NamespaceKey("Nessie.Table"));
    when(catalog.getTable(datasetId)).thenReturn(dremioTable);

    Subject subject = new Subject();
    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());
    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);
    when(subject.reflectionStore.find()).thenReturn(emptyList());

    when(subject.userStore.getAllNotDeleted())
        .thenReturn(ImmutableList.of(reflectionGoal, reflectionGoal2));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));
    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class)))
        .thenReturn(cachingCatalog);

    subject.reflectionManager.sync();

    // ASSERT
    // Both reflections built on d1 but only 1 catalog getTable call was made.
    verify(catalog, times(1)).getTable(datasetId);
  }

  /** Verifies that the reflection job counts gets updated when handling deleted datasets */
  @Test
  public void testHandleDeletedDatasetsDeletesReflectionJobCounts() {
    final String datasetId = "d1";
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(new ReflectionId("reflectionId"))
            .setDatasetId(datasetId)
            .setType(ReflectionType.RAW)
            .setState(ReflectionGoalState.DISABLED);

    ExternalReflection externalReflection =
        new ExternalReflection()
            .setId("externalReflectionId")
            .setQueryDatasetId(datasetId)
            .setTargetDatasetId(datasetId);

    final Catalog catalog = mock(Catalog.class);
    CachingCatalog cachingCatalog = new CachingCatalog(catalog);
    final DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getPath()).thenReturn(new NamespaceKey("Nessie.Table"));
    when(catalog.getTable(datasetId)).thenReturn(null);

    Subject subject = new Subject();
    when(subject.contextFactory.create()).thenReturn(subject.dependencyResolutionContext);
    when(subject.externalReflectionStore.getExternalReflections())
        .thenReturn(Collections.singletonList(externalReflection));
    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS))
        .thenReturn(5555L);
    when(subject.reflectionStore.find()).thenReturn(emptyList());

    when(subject.userStore.getAllNotDeleted())
        .thenReturn(Collections.singletonList(reflectionGoal));
    when(subject.userStore.get(new ReflectionId(reflectionGoal.getId().getId())))
        .thenReturn(reflectionGoal);
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(singletonList(reflectionGoal));
    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class)))
        .thenReturn(cachingCatalog);

    ArgumentCaptor<DeleteJobCountsRequest> deleteJobCountsRequestArgumentCaptor =
        ArgumentCaptor.forClass(DeleteJobCountsRequest.class);
    subject.reflectionManager.sync();
    verify(subject.jobsService).deleteJobCounts(deleteJobCountsRequestArgumentCaptor.capture());

    // ASSERT
    // deleteJobCounts get the request to delete both the reflection goals when dataset is null
    List<String> deleteReflectionIds =
        deleteJobCountsRequestArgumentCaptor
            .getValue()
            .getReflectionsOrBuilder()
            .getReflectionIdsList();
    assertEquals(
        deleteReflectionIds,
        Arrays.asList(reflectionGoal.getId().getId(), externalReflection.getId()));
  }

  /**
   * Verifies that the reflection manager correctly updates materialization and snapshot ids in
   * dependencyManager.materializationInfoCache with handleMaterializationDone method.
   */
  @Test
  public void testDependencyManagerMaterializationInfoUpdates() throws Exception {
    String dataSetId = "dataSetId";
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionEntry entry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setState(ReflectionState.REFRESHING)
            .setType(ReflectionType.RAW)
            .setDatasetId(dataSetId)
            .setRefreshMethod(RefreshMethod.INCREMENTAL);

    MaterializationId materializationId = new MaterializationId("m_id");
    Materialization m =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setState(MaterializationState.RUNNING)
            .setIsIcebergDataset(true)
            .setBasePath("/base/path")
            .setPreviousIcebergSnapshot(1L)
            .setExpiration(Long.MAX_VALUE)
            .setSeriesOrdinal(1);

    NamespaceKey namespaceKey =
        new NamespaceKey(
            Lists.newArrayList(
                ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
                entry.getId().getId(),
                m.getId().getId()));

    JobProtobuf.JobInfo jobInfo =
        JobProtobuf.JobInfo.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setSql("sql")
            .setDatasetVersion("version")
            .setQueryType(JobProtobuf.QueryType.ACCELERATOR_CREATE)
            .setStartTime(100)
            .setFinishTime(200)
            .build();

    JobProtobuf.JobAttempt jobAttempt =
        JobProtobuf.JobAttempt.newBuilder()
            .setState(JobProtobuf.JobState.COMPLETED)
            .setInfo(jobInfo)
            .setStats(JobProtobuf.JobStats.newBuilder().setOutputRecords(4L).build())
            .setAttemptId("1be6174f-8e89-9244-643a-565b81142700")
            .build();

    JobDetails job =
        JobDetails.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId("m_job_id").build())
            .setCompleted(true)
            .addAttempts(jobAttempt)
            .build();

    JobId jobId = new JobId().setId("m_job_id");
    entry.setRefreshJobId(jobId);
    Subject subject = new Subject();

    final Catalog catalog = mock(Catalog.class);
    final DremioTable dremioTable = mock(DremioTable.class);
    final DatasetConfig datasetConfig = mock(DatasetConfig.class);
    final PhysicalDataset physicalDataset = mock(PhysicalDataset.class);
    final IcebergMetadata icebergMetadata = mock(IcebergMetadata.class);
    long testSnapshot = 2L;

    RefreshDoneHandler handler = mock(RefreshDoneHandler.class);
    AccelerationSettings accelerationSettings = new AccelerationSettings();
    accelerationSettings.setMethod(RefreshMethod.INCREMENTAL);
    accelerationSettings.setRefreshField("refreshField");
    accelerationSettings.setSnapshotBased(true);
    RefreshDecision decision = new RefreshDecision();
    decision.setAccelerationSettings(accelerationSettings);
    decision.setDatasetHash(111);
    doReturn(decision).when(handler).handle();

    when(subject.materializationStore.getRefreshes(m))
        .thenReturn(FluentIterable.from(new Refresh[0]));

    // required for maybeOptimizeIncrementalReflectionFiles to skip OPTIMIZE,
    // which is unrelated to this test
    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setArrowCachingEnabled(true)
            .setDatasetId(dataSetId)
            .setType(ReflectionType.RAW)
            .setDetails(new ReflectionDetails())
            .setState(ReflectionGoalState.ENABLED);
    when(subject.userStore.get(entry.getId())).thenReturn(reflectionGoal);
    when(subject.optionManager.getOption(
            ReflectionOptions.ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS))
        .thenReturn(false);

    when(subject.catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);
    when(catalog.getTable(namespaceKey)).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(datasetConfig.getPhysicalDataset()).thenReturn(physicalDataset);
    when(physicalDataset.getIcebergMetadata()).thenReturn(icebergMetadata);
    when(icebergMetadata.getSnapshotId()).thenReturn(testSnapshot);

    PlanCacheInvalidationHelper planCacheInvalidationHelper =
        mock(PlanCacheInvalidationHelper.class);
    when(subject.planCacheInvalidationHelper.get()).thenReturn(planCacheInvalidationHelper);
    doNothing()
        .when(planCacheInvalidationHelper)
        .invalidateReflectionAssociatedPlanCache(entry.getDatasetId());

    // TEST
    subject.reflectionManager.handleSuccessfulJob(entry, m, job, handler);

    // ASSERT
    // Verify the map was updated
    verify(subject.dependencyManager, times(1))
        .updateMaterializationInfo(reflectionId, materializationId, testSnapshot, false);
  }

  @Test
  public void testGoalDeletionWithFolderCleanup() throws NamespaceException {
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoal reflectionGoal = new ReflectionGoal().setId(reflectionId);
    Subject subject = new Subject();
    long now = System.currentTimeMillis();
    when(subject.userStore.getDeletedBefore(now - ReflectionManager.GOAL_DELETION_WAIT_MILLIS))
        .thenReturn(Collections.singletonList(reflectionGoal));
    NamespaceKey reflectionFolderKey =
        new NamespaceKey(
            Lists.newArrayList(
                ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME, reflectionId.getId()));

    FolderConfig folderConfig = Mockito.mock(FolderConfig.class);
    when(subject.namespaceService.getFolder(reflectionFolderKey)).thenReturn(folderConfig);

    // TEST
    subject.reflectionManager.deleteDeprecatedGoals(now);

    // ASSERT
    verify(subject.userStore).delete(reflectionId);
    verify(subject.namespaceService).deleteFolder(reflectionFolderKey, folderConfig.getTag());
  }
}

class Subject {
  @VisibleForTesting SabotContext sabotContext = Mockito.mock(SabotContext.class);
  @VisibleForTesting JobsService jobsService = Mockito.mock(JobsService.class);
  @VisibleForTesting NamespaceService namespaceService = Mockito.mock(NamespaceService.class);
  @VisibleForTesting CatalogService catalogService = Mockito.mock(CatalogService.class);
  @VisibleForTesting OptionManager optionManager = Mockito.mock(OptionManager.class);
  @VisibleForTesting ReflectionGoalsStore userStore = Mockito.mock(ReflectionGoalsStore.class);

  @VisibleForTesting
  ReflectionEntriesStore reflectionStore = Mockito.mock(ReflectionEntriesStore.class);

  @VisibleForTesting
  ExternalReflectionStore externalReflectionStore = Mockito.mock(ExternalReflectionStore.class);

  @VisibleForTesting
  MaterializationStore materializationStore = Mockito.mock(MaterializationStore.class);

  @VisibleForTesting
  MaterializationPlanStore materializationPlanStore = Mockito.mock(MaterializationPlanStore.class);

  @VisibleForTesting DependenciesStore dependenciesStore = Mockito.mock(DependenciesStore.class);
  @VisibleForTesting DependencyManager dependencyManager = Mockito.mock(DependencyManager.class);

  @VisibleForTesting
  ReflectionServiceImpl.DescriptorCache descriptorCache =
      Mockito.mock(ReflectionServiceImpl.DescriptorCache.class);

  @VisibleForTesting Set<ReflectionId> reflectionsToUpdate = Sets.newHashSet();

  @VisibleForTesting
  ReflectionManager.WakeUpCallback wakeUpCallback =
      Mockito.mock(ReflectionManager.WakeUpCallback.class);

  @VisibleForTesting
  Function<Catalog, ReflectionServiceImpl.ExpansionHelper> expansionHelper =
      Mockito.mock(Function.class);

  @VisibleForTesting
  Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper = Mockito.mock(Supplier.class);

  @VisibleForTesting BufferAllocator allocator = Mockito.mock(BufferAllocator.class);

  @VisibleForTesting
  ReflectionGoalChecker reflectionGoalChecker = Mockito.mock(ReflectionGoalChecker.class);

  @VisibleForTesting
  RefreshStartHandler refreshStartHandler = Mockito.mock(RefreshStartHandler.class);

  @VisibleForTesting
  AccelerationStoragePlugin accelerationPlugin = Mockito.mock(AccelerationStoragePlugin.class);

  @VisibleForTesting
  DependencyResolutionContextFactory contextFactory =
      Mockito.mock(DependencyResolutionContextFactory.class);

  @VisibleForTesting
  DependencyResolutionContext dependencyResolutionContext =
      Mockito.mock(DependencyResolutionContext.class);

  @VisibleForTesting SabotConfig sabotConfig = SabotConfig.create();
  @VisibleForTesting DatasetEventHub datasetEventHub = new DatasetEventHub();
  @VisibleForTesting ReflectionManager reflectionManager;

  public Subject() {
    when(sabotContext.getConfig()).thenReturn(sabotConfig);
    reflectionManager = newReflectionManager();
  }

  @VisibleForTesting
  ReflectionManager newReflectionManager() {
    return spy(
        new ReflectionManager(
            sabotContext,
            jobsService,
            catalogService,
            namespaceService,
            optionManager,
            userStore,
            reflectionStore,
            externalReflectionStore,
            materializationStore,
            materializationPlanStore,
            dependencyManager,
            descriptorCache,
            wakeUpCallback,
            expansionHelper,
            allocator,
            reflectionGoalChecker,
            refreshStartHandler,
            contextFactory,
            datasetEventHub));
  }
}
