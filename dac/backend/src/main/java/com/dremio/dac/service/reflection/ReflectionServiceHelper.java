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
package com.dremio.dac.service.reflection;

import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;

import com.dremio.dac.api.Reflection;
import com.dremio.dac.service.errors.ReflectionNotFound;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.service.reflection.ReflectionAdministrationService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionStatus;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Optional;

/**
 * Reflection service helper
 */
public class ReflectionServiceHelper {
  private final ReflectionAdministrationService.Factory reflectionAdministrationServiceFactory;
  private final ReflectionStatusService reflectionStatusService;

  @Inject
  public ReflectionServiceHelper(
    ReflectionAdministrationService.Factory reflectionAdministrationServiceFactory,
    ReflectionStatusService reflectionStatusService
  ) {
    this.reflectionAdministrationServiceFactory = reflectionAdministrationServiceFactory;
    this.reflectionStatusService = reflectionStatusService;
  }

  protected ReflectionAdministrationService getReflectionAdministrationService() {
    return reflectionAdministrationServiceFactory.get(ReflectionContext.SYSTEM_USER_CONTEXT);
  }

  public Optional<ReflectionGoal> getReflectionById(String id) {
    return getReflectionAdministrationService().getGoal(new ReflectionId(id));
  }

  public Iterable<ReflectionGoal> getAllReflections() {
    return getReflectionAdministrationService().getAllReflections();
  }

  public Iterable<ReflectionGoal> getReflectionsForDataset(String datasetid) {
    return getReflectionAdministrationService().getReflectionsByDatasetId(datasetid);
  }

  public ReflectionStatusUI getStatusForReflection(String reflectionId) {
    ReflectionId id = new ReflectionId(reflectionId);
    final ReflectionStatus status = reflectionStatusService.getReflectionStatus(id);
    return new ReflectionStatusUI(status);
  }

  public ReflectionGoal createReflection(ReflectionGoal goal) {
    ReflectionId id = getReflectionAdministrationService().create(goal);

    return getReflectionAdministrationService().getGoal(id).get();
  }

  public ReflectionGoal updateReflection(ReflectionGoal goal) {
    Optional<ReflectionGoal> existingGoal = getReflectionAdministrationService().getGoal(goal.getId());

    if (!existingGoal.isPresent()) {
      throw new ReflectionNotFound(goal.getId().getId());
    }

    ReflectionGoal reflection = existingGoal.get();

    // check if anything has changed - if not, don't bother updating
    if (
      !reflection.getName().equals(goal.getName()) ||
      !reflection.getArrowCachingEnabled().equals(goal.getArrowCachingEnabled()) ||
      reflection.getState() != goal.getState() ||
      !areReflectionDetailsEqual(reflection.getDetails(), goal.getDetails())
    ) {
      getReflectionAdministrationService().update(goal);
    }

    return getReflectionAdministrationService().getGoal(goal.getId()).get();
  }

  public List<ReflectionGoal> getRecommendedReflections(String id) {
    return getReflectionAdministrationService().getRecommendedReflections(id);
  }

  public void removeReflection(String id) {
    Optional<ReflectionGoal> goal = getReflectionAdministrationService().getGoal(new ReflectionId(id));

    if (goal.isPresent()) {
      getReflectionAdministrationService().remove(goal.get());
    } else {
      throw new ReflectionNotFound(id);
    }
  }

  public void clearAllReflections() {
    getReflectionAdministrationService().clearAll();
  }

  public long getCurrentSize(String id) {
    long size = 0;

    final ReflectionAdministrationService reflectionAdministrationService = getReflectionAdministrationService();
    ReflectionId reflectionId = new ReflectionId(id);
    if (reflectionAdministrationService.doesReflectionHaveAnyMaterializationDone(reflectionId)) {
      size = reflectionAdministrationService.getReflectionSize(reflectionId);
    }

    return size;
  }

  public long getTotalSize(String id) {
    long total = 0;

    final ReflectionAdministrationService reflectionAdministrationService = getReflectionAdministrationService();
    ReflectionId reflectionId = new ReflectionId(id);
    if (reflectionAdministrationService.doesReflectionHaveAnyMaterializationDone(reflectionId)) {
      total = reflectionAdministrationService.getTotalReflectionSize(reflectionId);
    }

    return total;
  }

  public void setSubstitutionEnabled(boolean enabled) {
    getReflectionAdministrationService().setSubstitutionEnabled(enabled);
  }

  public boolean isSubstitutionEnabled() {
    return getReflectionAdministrationService().isSubstitutionEnabled();
  }

  public static boolean areReflectionDetailsEqual(ReflectionDetails details1, ReflectionDetails details2) {
    boolean equal = false;

    if (
      areBothListsEqual(details1.getDimensionFieldList(), details2.getDimensionFieldList()) &&
      areBothListsEqual(details1.getDisplayFieldList(), details2.getDisplayFieldList()) &&
      areBothListsEqual(details1.getDistributionFieldList(), details2.getDistributionFieldList()) &&
      areBothListsEqual(details1.getMeasureFieldList(), details2.getMeasureFieldList()) &&
      areBothListsEqual(details1.getPartitionFieldList(), details2.getPartitionFieldList()) &&
      areBothListsEqual(details1.getSortFieldList(), details2.getSortFieldList()) &&
      details1.getPartitionDistributionStrategy().equals(details2.getPartitionDistributionStrategy())
    ) {
      equal = true;
    }

    return equal;
  }

  public ReflectionSettings getReflectionSettings() {
    return getReflectionAdministrationService().getReflectionSettings();
  }

  public boolean doesDatasetHaveActiveReflection(String datasetId) {
    // TODO: for performance reasons we just check if there are enabled reflections
    return getReflectionAdministrationService().getEnabledReflectionCountForDataset(datasetId) > 0;
  }

  public void refreshReflectionsForDataset(String datasetId) {
    getReflectionAdministrationService().requestRefresh(datasetId);
  }

  private static boolean areBothListsEqual(Collection collection1, Collection collection2) {
    // CollectionUtils.isEqualCollection is not null safe
    if (collection1 == null || collection2 == null) {
      return CollectionUtils.isEmpty(collection1) && CollectionUtils.isEmpty(collection2);
    }
    else {
      return CollectionUtils.isEqualCollection(collection1, collection2);
    }
  }

  public boolean isReflectionIncremental(String id) {
    return getReflectionAdministrationService().isReflectionIncremental(new ReflectionId(id));
  }

  public Reflection newReflection(ReflectionGoal goal) {
    final String goalId = goal.getId().getId();
    return new Reflection(goal, getStatusForReflection(goalId), getCurrentSize(goalId), getTotalSize(goalId));
  }
}
