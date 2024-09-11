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

import static com.dremio.exec.catalog.CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_SCHEDULE_POLICY_ENABLED;

import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.dac.api.Reflection;
import com.dremio.dac.service.errors.ReflectionNotFound;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionAdministrationService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionStatus;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.collect.Iterables;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.calcite.util.Pair;

/** Reflection service helper */
public class ReflectionServiceHelper {
  private final ReflectionAdministrationService.Factory reflectionAdministrationServiceFactory;
  private final ReflectionStatusService reflectionStatusService;
  private final OptionManager optionManager;

  @Inject
  public ReflectionServiceHelper(
      ReflectionAdministrationService.Factory reflectionAdministrationServiceFactory,
      ReflectionStatusService reflectionStatusService,
      OptionManager optionManager) {
    this.reflectionAdministrationServiceFactory = reflectionAdministrationServiceFactory;
    this.reflectionStatusService = reflectionStatusService;
    this.optionManager = optionManager;
  }

  public ReflectionAdministrationService getReflectionAdministrationService() {
    return reflectionAdministrationServiceFactory.get(ReflectionContext.SYSTEM_USER_CONTEXT);
  }

  public ReflectionStatusService getReflectionStatusService() {
    return reflectionStatusService;
  }

  public Optional<ReflectionGoal> getReflectionById(String id) {
    return getReflectionAdministrationService().getGoal(new ReflectionId(id));
  }

  public Iterable<ReflectionGoal> getAllReflections() {
    return getReflectionAdministrationService().getAllReflections();
  }

  public Iterable<ReflectionGoal> getReflectionsForDataset(String datasetid) {
    isVersionedSourceEnabled(datasetid);
    return getReflectionAdministrationService().getReflectionsByDatasetId(datasetid);
  }

  public ReflectionStatusUI getStatusForReflection(String reflectionId) {
    ReflectionId id = new ReflectionId(reflectionId);
    final ReflectionStatus status = reflectionStatusService.getReflectionStatus(id);
    return new ReflectionStatusUI(status);
  }

  public ReflectionGoal createReflection(ReflectionGoal goal) {
    isVersionedSourceEnabled(goal.getDatasetId());
    ReflectionId id = getReflectionAdministrationService().create(goal);

    return getReflectionAdministrationService().getGoal(id).get();
  }

  public ReflectionGoal updateReflection(ReflectionGoal goal) {
    isVersionedSourceEnabled(goal.getDatasetId());
    Optional<ReflectionGoal> existingGoal =
        getReflectionAdministrationService().getGoal(goal.getId());

    if (!existingGoal.isPresent()) {
      throw new ReflectionNotFound(goal.getId().getId());
    }

    getReflectionAdministrationService().update(goal);

    return getReflectionAdministrationService().getGoal(goal.getId()).get();
  }

  public List<ReflectionGoal> getRecommendedReflections(String id, ReflectionSuggestionType type) {
    return getReflectionAdministrationService().getRecommendedReflections(id, type);
  }

  public void removeReflection(String id) {
    Optional<ReflectionGoal> goal =
        getReflectionAdministrationService().getGoal(new ReflectionId(id));

    if (goal.isPresent()) {
      getReflectionAdministrationService().remove(goal.get());
    } else {
      throw new ReflectionNotFound(id);
    }
  }

  public void clearAllReflections() {
    getReflectionAdministrationService().clearAll();
  }

  public void retryUnavailableReflections() {
    getReflectionAdministrationService().retryUnavailable();
  }

  /**
   * If reflection has a done materialization, returns the current size in bytes and output records
   *
   * @param id
   * @return
   */
  public Pair<Long, Long> getCurrentSize(String id) {
    final ReflectionAdministrationService reflectionAdministrationService =
        getReflectionAdministrationService();
    ReflectionId reflectionId = new ReflectionId(id);
    Optional<Materialization> materialization =
        reflectionAdministrationService.getLastDoneMaterialization(reflectionId);
    if (materialization.isPresent()) {
      Pair<MaterializationMetrics, Long> metrics =
          getReflectionStatusService().getReflectionSize(materialization.get());
      return Pair.of(metrics.left.getFootprint(), metrics.right);
    }
    return Pair.of(0L, 0L);
  }

  public long getTotalSize(String id) {
    long total = 0;

    final ReflectionAdministrationService reflectionAdministrationService =
        getReflectionAdministrationService();
    ReflectionId reflectionId = new ReflectionId(id);
    if (reflectionAdministrationService.getLastDoneMaterialization(reflectionId).isPresent()) {
      total = getReflectionStatusService().getTotalReflectionSize(reflectionId);
    }

    return total;
  }

  public void setSubstitutionEnabled(boolean enabled) {
    getReflectionAdministrationService().setSubstitutionEnabled(enabled);
  }

  public boolean isSubstitutionEnabled() {
    return getReflectionAdministrationService().isSubstitutionEnabled();
  }

  public ReflectionSettings getReflectionSettings() {
    return getReflectionAdministrationService().getReflectionSettings();
  }

  public boolean doesDatasetHaveActiveReflection(String datasetId) {
    // TODO: for performance reasons we just check if there are enabled reflections
    return getReflectionAdministrationService().getEnabledReflectionCountForDataset(datasetId) > 0;
  }

  @WithSpan
  public void refreshReflectionsForDataset(String datasetId) {
    getReflectionAdministrationService().requestRefresh(datasetId);
  }

  public boolean isReflectionIncremental(String id) {
    return getReflectionAdministrationService().isReflectionIncremental(new ReflectionId(id));
  }

  public Reflection newReflection(ReflectionGoal goal) {
    final String goalId = goal.getId().getId();
    Pair<Long, Long> currentSize = getCurrentSize(goalId);
    return new Reflection(
        goal, getStatusForReflection(goalId), currentSize.left, getTotalSize(goalId));
  }

  public boolean doesDatasetHaveReflection(String datasetId) {
    return Iterables.size(getReflectionAdministrationService().getReflectionsByDatasetId(datasetId))
        > 0;
  }

  public void isVersionedSourceEnabled(String datasetId) {
    if (!optionManager.getOption(REFLECTION_VERSIONED_SOURCE_ENABLED)
        && VersionedDatasetId.isVersionedDatasetId(datasetId)) {
      throw new UnsupportedOperationException("Versioned source does not support reflection.");
    }
  }

  public boolean isIncrementalRefreshBySnapshotEnabled(DatasetConfig datasetConfig) {
    return IncrementalUpdateServiceUtils.isIncrementalRefreshBySnapshotEnabled(
        datasetConfig, optionManager);
  }

  public boolean isRefreshSchedulePolicyEnabled() {
    return optionManager.getOption(REFLECTION_SCHEDULE_POLICY_ENABLED);
  }

  public OptionManager getOptionManager() {
    return optionManager;
  }

  public AccelerationSettings fromAccelerationSettingsDescriptor(
      AccelerationSettingsDescriptor descriptor,
      AccelerationSettings oldSettings,
      DatasetConfig config) {
    AccelerationSettings settings =
        new AccelerationSettings()
            .setGracePeriod(descriptor.getAccelerationGracePeriod())
            .setMethod(descriptor.getMethod())
            .setRefreshField(descriptor.getRefreshField())
            .setNeverExpire(descriptor.getAccelerationNeverExpire())
            .setNeverRefresh(descriptor.getAccelerationNeverRefresh());
    if (descriptor.getAccelerationActivePolicyType() == RefreshPolicyType.PERIOD
        || descriptor.getAccelerationActivePolicyType() == RefreshPolicyType.SCHEDULE
        || descriptor.getAccelerationActivePolicyType() == RefreshPolicyType.NEVER) {
      settings.setRefreshPolicyType(descriptor.getAccelerationActivePolicyType());
    } else {
      settings.setRefreshPolicyType(RefreshPolicyType.PERIOD);
    }
    return settings;
  }

  public AccelerationSettings fromSourceConfig(SourceConfig sourceConfig) {
    AccelerationSettings settings =
        new AccelerationSettings()
            .setMethod(RefreshMethod.FULL)
            .setRefreshPeriod(sourceConfig.getAccelerationRefreshPeriod())
            .setRefreshSchedule(sourceConfig.getAccelerationRefreshSchedule())
            .setGracePeriod(sourceConfig.getAccelerationGracePeriod())
            .setNeverExpire(sourceConfig.getAccelerationNeverExpire())
            .setNeverRefresh(sourceConfig.getAccelerationNeverRefresh());
    if (sourceConfig.getAccelerationActivePolicyType() == RefreshPolicyType.PERIOD
        || sourceConfig.getAccelerationActivePolicyType() == RefreshPolicyType.SCHEDULE
        || sourceConfig.getAccelerationActivePolicyType() == RefreshPolicyType.NEVER) {
      settings.setRefreshPolicyType(sourceConfig.getAccelerationActivePolicyType());
    } else {
      settings.setRefreshPolicyType(RefreshPolicyType.PERIOD);
    }
    return settings;
  }
}
