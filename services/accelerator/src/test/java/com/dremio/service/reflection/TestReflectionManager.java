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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalHash;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
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
        .setId(reflectionId)
        .setReflectionGoalHash(reflectionGoalHash)
        .setArrowCachingEnabled(true);


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
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
      .setId(reflectionId)
      .setArrowCachingEnabled(true)
      .setName(reflectionGoalName)
      .setType(ReflectionType.EXTERNAL)
      .setDatasetId(dataSetId);
    ReflectionEntry reflectionEntry =
      new ReflectionEntry()
        .setId(reflectionId)
        .setReflectionGoalHash(reflectionGoalHash)
        .setArrowCachingEnabled(false)
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
    Assert.assertEquals(ReflectionState.ACTIVE, reflectionEntry.getState());
  }

  @Test
  public void handleGoalWithEntryButHashHasChanged(){
    ReflectionId reflectionId = new ReflectionId("r_id");
    ReflectionGoalHash reflectionGoalHash = new ReflectionGoalHash("MY_HASH");
    String reflectionGoalName = "name";
    String dataSetId = "dataSetId";
    ReflectionGoal reflectionGoal = new ReflectionGoal()
      .setId(reflectionId)
      .setArrowCachingEnabled(true)
      .setName(reflectionGoalName)
      .setType(ReflectionType.EXTERNAL)
      .setDatasetId(dataSetId);
    ReflectionEntry reflectionEntry =
      new ReflectionEntry()
        .setId(reflectionId)
        .setReflectionGoalHash(new ReflectionGoalHash("xxx"))
        .setArrowCachingEnabled(false)
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
    Assert.assertEquals(reflectionGoalHash, reflectionEntry.getReflectionGoalHash());
    Assert.assertEquals(true, reflectionEntry.getArrowCachingEnabled());
    Assert.assertEquals(ReflectionState.UPDATE, reflectionEntry.getState());
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
    reflectionGoalChecker
  );
}
