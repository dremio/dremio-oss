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

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.service.reflection.ReflectionServiceImpl.PlanCacheInvalidationHelper;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;

public class PlanCacheSynchronizerTest {
  private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

  private ReflectionGoalsStore goals = Mockito.mock(ReflectionGoalsStore.class);
  private ReflectionEntriesStore entries = Mockito.mock(ReflectionEntriesStore.class);
  private PlanCacheInvalidationHelper invalidationHelper = Mockito.mock(PlanCacheInvalidationHelper.class);
  private long yesterday = System.currentTimeMillis() - DAY_IN_MS;
  // when we return entries with this modified time, they should be part of the response
  private long tomorrow = System.currentTimeMillis() + DAY_IN_MS;

  @Test
  public void testChangedGoalsContributeToDatasetsProcessed() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(true);

    when(entries.find())
        .thenReturn(Collections.emptyList());

    when(goals.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(
            asList(createGoal("a"),
                createGoal("b"),
                createGoal("c"))
        );

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("a");
    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("b");
    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("c");
    verify(invalidationHelper, times(3)).invalidateReflectionAssociatedPlanCache(anyString());
  }

  @Test
  public void testOnlyChangedEntriesContributeToDatasetsProcessed() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(true);

    when(entries.find())
        .thenReturn(asList(
            createEntry("one", tomorrow),
            createEntry("two", tomorrow)
        ));

    when(goals.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(
            asList(createGoal("one"),
                createGoal("three")
        ));

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("one");
    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("two");
    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("three");
    verify(invalidationHelper, times(3)).invalidateReflectionAssociatedPlanCache(anyString());
  }

  @Test
  public void testDatasetProcessedOnceIfBothGoalAndEntryChanged() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(true);

    when(entries.find())
        .thenReturn(asList(
            createEntry("a", yesterday),
            createEntry("b", tomorrow),
            createEntry("c", yesterday),
            createEntry("d", tomorrow)
        ));

    when(goals.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(Collections.emptyList());

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("b");
    verify(invalidationHelper, times(1)).invalidateReflectionAssociatedPlanCache("d");
    verify(invalidationHelper, times(2)).invalidateReflectionAssociatedPlanCache(anyString());
  }

  @Test
  public void testShouldNotSyncIfPlanCacheDisabled() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(false);

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, never()).invalidateReflectionAssociatedPlanCache(anyString());
    verify(entries, never()).find();
    verify(goals, never()).getModifiedOrCreatedSince(anyLong());
  }

  @Test
  public void testFailuresDuringExecutionNotPropagated() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(true);

    when(entries.find())
        .thenThrow(new RuntimeException());

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, never()).invalidateReflectionAssociatedPlanCache(anyString());
  }

  @Test
  public void testIgnoreErrorsDuringInvalidation() {
    when(invalidationHelper.isPlanCacheEnabled())
        .thenReturn(true);

    when(entries.find())
        .thenReturn(asList(
            createEntry("b", tomorrow),
            createEntry("d", tomorrow)
        ));

    when(goals.getModifiedOrCreatedSince(anyLong()))
        .thenReturn(Collections.emptyList());

    doThrow(new RuntimeException()).when(invalidationHelper).invalidateReflectionAssociatedPlanCache("b");

    PlanCacheSynchronizer synchronizer = new PlanCacheSynchronizer(goals, entries, () -> invalidationHelper);
    synchronizer.sync();

    verify(invalidationHelper, times(2)).invalidateReflectionAssociatedPlanCache(anyString());
  }

  private ReflectionGoal createGoal(String datasetId) {
    ReflectionGoal goal = new ReflectionGoal();
    goal.setDatasetId(datasetId);
    return goal;
  }

  private ReflectionEntry createEntry(String datasetId, long modifiedAt) {
    ReflectionEntry entry = new ReflectionEntry();
    entry.setDatasetId(datasetId);
    entry.setModifiedAt(modifiedAt);
    return entry;
  }
}
