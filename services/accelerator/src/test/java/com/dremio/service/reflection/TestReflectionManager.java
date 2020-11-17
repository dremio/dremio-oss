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

import static com.dremio.service.reflection.proto.ReflectionState.REFRESH;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalHash;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;

/**
 * Test Reflection Manager
 */
public class TestReflectionManager {
  @Test
  public void handleGoalWithNoEntry(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    String tag = "rgTag";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
      .setId(reflectionId)
      .setArrowCachingEnabled(true)
      .setName(reflectionGoalName)
      .setTag(tag)
      .setType(ReflectionType.EXTERNAL)
      .setDatasetId(dataSetId);

    Subject subject = new Subject();

    when(subject.reflectionStore.get(reflectionId)).thenReturn(null);
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal)).thenReturn(reflectionGoalHash);

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).calculateReflectionGoalVersion(reflectionGoal);

    verify(subject.reflectionStore).save(
      new ReflectionEntry()
        .setId(reflectionId)
        .setReflectionGoalHash(reflectionGoalHash)
        .setDatasetId(dataSetId)
        .setState(REFRESH)
        .setGoalVersion(tag)
        .setType(ReflectionType.EXTERNAL)
        .setName(reflectionGoalName)
        .setArrowCachingEnabled(true)
    );
    verifyNoMoreInteractions(subject.reflectionStore);
  }

  @Test
  public void handleGoalWithEntryButNothingHasChanged(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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
    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
      .thenReturn(true);

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).isEqual(reflectionGoal, reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);
  }

  @Test
  public void handleGoalWithEntryButHashHasNotChangedAndBoostHasChanged(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String goalTag = "goal_tag";
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
      .thenReturn(true);
    when(subject.materializationStore.find(reflectionId)).thenReturn(Collections.emptyList());

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
  public void handleGoalTagChangedAndEntryIsInFailedState(){
    // We currently use a noop change as a work around for getting reflections out of a failed state
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String goalTag = "goal_tag";
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
      .thenReturn(true);
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
  public void handleGoalWithEntryButHashHasChanged(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String goalTag = "goalTag";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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
    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry))
      .thenReturn(false);
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal, reflectionEntry))
      .thenReturn(false);
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal)).thenReturn(reflectionGoalHash);
    when(subject.materializationStore.find(reflectionId)).thenReturn(Collections.emptyList());

    // TEST
    subject.reflectionManager.handleGoal(reflectionGoal);

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.reflectionStore).save(reflectionEntry);

    verifyNoMoreInteractions(subject.reflectionStore);

    //Ensure the entry is updated appropriately
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(goalTag, reflectionEntry.getGoalVersion());
    assertEquals(reflectionGoalName, reflectionEntry.getName());
    assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    assertEquals(ReflectionState.UPDATE, reflectionEntry.getState());
  }

  @Test
  public void testSyncDoesNotUpdateReflectionWhenOnlyBoostIsToggle(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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

    Materialization materialization = new Materialization()
      .setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();

    Subject subject = new Subject();

    when(subject.dependencyManager.shouldRefresh(reflectionEntry, 5555L)).thenReturn(false); //assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId)).thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt())).thenReturn(emptyList());

    when(subject.namespaceService.findDatasetByUUID(dataSetId)).thenReturn(datasetConfig);

    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS)).thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));

    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry)).thenReturn(false);//it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal,reflectionEntry)).thenReturn(true);//but only fields not used to update the reflection

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong())).thenReturn(singletonList(reflectionGoal));

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
  public void testSyncDoesUpdateReflectionWhenChanged(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    JobId materializationJobId = new JobId("m_job_id");
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
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

    Materialization materialization = new Materialization()
      .setId(materializationId)
      .setReflectionId(reflectionId)
      .setArrowCachingEnabled(false);

    DatasetConfig datasetConfig = new DatasetConfig();

    Subject subject = new Subject();

    when(subject.dependencyManager.shouldRefresh(reflectionEntry, 5555L)).thenReturn(false); //assuming we do not need a refresh for other reasons

    when(subject.externalReflectionStore.getExternalReflections()).thenReturn(emptyList());

    when(subject.materializationStore.find(reflectionId)).thenReturn(singletonList(materialization));
    when(subject.materializationStore.getAllExpiredWhen(anyLong())).thenReturn(emptyList());
    when(subject.materializationStore.getDeletableEntriesModifiedBefore(anyLong(), anyInt())).thenReturn(emptyList());
    when(subject.materializationStore.getAllDone(reflectionId)).thenReturn(singletonList(materialization));

    when(subject.namespaceService.findDatasetByUUID(dataSetId)).thenReturn(datasetConfig);

    when(subject.optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS)).thenReturn(5555L);

    when(subject.reflectionStore.get(reflectionId)).thenReturn(reflectionEntry);
    when(subject.reflectionStore.find()).thenReturn(singletonList(reflectionEntry));


    when(subject.reflectionGoalChecker.isEqual(reflectionGoal, reflectionEntry)).thenReturn(false);//it has changed
    when(subject.reflectionGoalChecker.checkHash(reflectionGoal,reflectionEntry)).thenReturn(false);//but only fields not used to update the reflection
    when(subject.reflectionGoalChecker.calculateReflectionGoalVersion(reflectionGoal)).thenReturn(reflectionGoalHash);

    when(subject.refreshStartHandler.startJob(any(), anyLong(), any())).thenReturn(materializationJobId);

    when(subject.sabotContext.getExecutors()).thenReturn(singletonList(null));

    when(subject.userStore.getAllNotDeleted()).thenReturn(singletonList(reflectionGoal));
    when(subject.userStore.getDeletedBefore(anyLong())).thenReturn(emptyList());
    when(subject.userStore.getModifiedOrCreatedSince(anyLong())).thenReturn(singletonList(reflectionGoal));

    subject.reflectionManager.sync();

    // ASSERT
    verify(subject.reflectionStore).get(reflectionId);
    verify(subject.reflectionStore).find();
    verify(subject.reflectionStore, times(2)).save(reflectionEntry);

    verify(subject.reflectionGoalChecker).checkHash(reflectionGoal, reflectionEntry);
    verify(subject.descriptorCache).invalidate(materializationId);

    verify(subject.refreshStartHandler).startJob(any(), anyLong(), any()); //Mockito does not support using a mix of any matchers....

    verifyNoMoreInteractions(subject.reflectionStore);
    verifyNoMoreInteractions(subject.refreshStartHandler);
    assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    assertEquals(false, materialization.getArrowCachingEnabled());
    assertEquals(0 , reflectionEntry.getNumFailures().intValue());
    assertEquals(ReflectionState.REFRESHING, reflectionEntry.getState());
    assertEquals(MaterializationState.DEPRECATED, materialization.getState());
  }
}

class Subject {
  @VisibleForTesting SabotContext sabotContext = Mockito.mock(SabotContext.class);
  @VisibleForTesting JobsService jobsService = Mockito.mock(JobsService.class);
  @VisibleForTesting NamespaceService namespaceService = Mockito.mock(NamespaceService.class);
  @VisibleForTesting OptionManager optionManager = Mockito.mock(OptionManager.class);
  @VisibleForTesting ReflectionGoalsStore userStore = Mockito.mock(ReflectionGoalsStore.class);
  @VisibleForTesting ReflectionEntriesStore reflectionStore = Mockito.mock(ReflectionEntriesStore.class);
  @VisibleForTesting ExternalReflectionStore externalReflectionStore = Mockito.mock(ExternalReflectionStore.class);
  @VisibleForTesting MaterializationStore materializationStore = Mockito.mock(MaterializationStore.class);
  @VisibleForTesting DependencyManager dependencyManager = Mockito.mock(DependencyManager.class);
  @VisibleForTesting ReflectionServiceImpl.DescriptorCache descriptorCache = Mockito.mock(ReflectionServiceImpl.DescriptorCache.class);
  @VisibleForTesting Set<ReflectionId> reflectionsToUpdate = Sets.newHashSet();
  @VisibleForTesting ReflectionManager.WakeUpCallback wakeUpCallback = Mockito.mock(ReflectionManager.WakeUpCallback.class);
  @VisibleForTesting Supplier<ReflectionServiceImpl.ExpansionHelper> expansionHelper = Mockito.mock(Supplier.class);
  @VisibleForTesting BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
  @VisibleForTesting ReflectionGoalChecker reflectionGoalChecker = Mockito.mock(ReflectionGoalChecker.class);
  @VisibleForTesting RefreshStartHandler refreshStartHandler = Mockito.mock(RefreshStartHandler.class);
  @VisibleForTesting ReflectionManager reflectionManager = new ReflectionManager(
    sabotContext,
    jobsService,
    namespaceService,
    optionManager,
    userStore,
    reflectionStore,
    externalReflectionStore,
    materializationStore,
    dependencyManager,
    descriptorCache,
    reflectionsToUpdate,
    wakeUpCallback,
    expansionHelper,
    allocator,
    Path.of("."), //TODO maybe we want to use JIMFS here,
    reflectionGoalChecker,
    refreshStartHandler
  );
}
