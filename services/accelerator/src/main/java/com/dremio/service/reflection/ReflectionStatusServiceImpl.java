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

import static com.dremio.common.utils.SqlUtils.quotedCompound;
import static com.dremio.service.reflection.ReflectionUtils.hasMissingPartitions;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS;
import com.dremio.service.reflection.ReflectionStatus.CONFIG_STATUS;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Computes the reflection status for a reflections and external reflections
 */
public class ReflectionStatusServiceImpl implements ReflectionStatusService {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionStatusServiceImpl.class);

  private final Provider<Collection<NodeEndpoint>> nodeEndpointsProvider;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<CacheViewer> cacheViewer;


  private final ReflectionGoalsStore goalsStore;
  private final ReflectionEntriesStore entriesStore;
  private final MaterializationStore materializationStore;
  private final ExternalReflectionStore externalReflectionStore;

  private final ReflectionSettings reflectionSettings;
  private final ReflectionValidator validator;

  private static final Joiner JOINER = Joiner.on(", ");

  @VisibleForTesting
  ReflectionStatusServiceImpl(
      Provider<Collection<NodeEndpoint>> nodeEndpointsProvider,
      Provider<NamespaceService> namespaceService,
      Provider<CacheViewer> cacheViewer,
      ReflectionGoalsStore goalsStore,
      ReflectionEntriesStore entriesStore,
      MaterializationStore materializationStore,
      ExternalReflectionStore externalReflectionStore,
      ReflectionSettings reflectionSettings,
      ReflectionValidator validator) {
    this.nodeEndpointsProvider = nodeEndpointsProvider;
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.cacheViewer = Preconditions.checkNotNull(cacheViewer, "cache viewer required");
    this.goalsStore = Preconditions.checkNotNull(goalsStore, "goals store required");
    this.entriesStore = Preconditions.checkNotNull(entriesStore, "entries store required");
    this.materializationStore = Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.externalReflectionStore = Preconditions.checkNotNull(externalReflectionStore, "external reflection store required");
    this.reflectionSettings = Preconditions.checkNotNull(reflectionSettings, "reflection settings required");

    this.validator = Preconditions.checkNotNull(validator, "validator required");
  }

  public ReflectionStatusServiceImpl(
      Provider<Collection<NodeEndpoint>> nodeEndpointsProvider,
      Provider<NamespaceService> namespaceServiceProvider,
      Provider<CatalogService> catalogService,
      Provider<LegacyKVStoreProvider> storeProvider,
      Provider<CacheViewer> cacheViewer) {
    Preconditions.checkNotNull(storeProvider, "kv store provider required");
    Preconditions.checkNotNull(catalogService, "catalog service required");
    this.nodeEndpointsProvider = nodeEndpointsProvider;
    this.cacheViewer = Preconditions.checkNotNull(cacheViewer, "cache viewer required");
    this.namespaceService = new Provider<NamespaceService>() {
      @Override
      public NamespaceService get() {
        return namespaceServiceProvider.get();
      }
    };

    goalsStore = new ReflectionGoalsStore(storeProvider);
    entriesStore = new ReflectionEntriesStore(storeProvider);
    materializationStore = new MaterializationStore(storeProvider);
    externalReflectionStore = new ExternalReflectionStore(storeProvider);

    reflectionSettings = new ReflectionSettings(namespaceService, storeProvider);
    validator = new ReflectionValidator(catalogService);
  }

  /**
   * Returns the status of a reflection
   *
   * @param id the reflection id
   * @return The ReflectionStatus representing the status
   * @throws IllegalArgumentException if the reflection could not be found
   * @throws IllegalStateException if the reflection was deleted
   */
  @Override
  public ReflectionStatus getReflectionStatus(ReflectionId id) {
    final ReflectionGoal goal = goalsStore.get(id);
    Preconditions.checkArgument(goal != null, "Reflection %s not found", id.getId());

    // should never be called on a deleted reflection
    Preconditions.checkState(goal.getState() != ReflectionGoalState.DELETED,
      "Getting status for deleted reflection %s", id.getId());

    if (goal.getState() == ReflectionGoalState.DISABLED) {
      return new ReflectionStatus(
        false,
        CONFIG_STATUS.OK,
        REFRESH_STATUS.SCHEDULED,
        AVAILABILITY_STATUS.NONE,
        0, 0, 0);
    }

    // get the refresh period
    final DatasetConfig config = Preconditions.checkNotNull(namespaceService.get().findDatasetByUUID(goal.getDatasetId()),
      "Dataset not present for reflection %s", id.getId());
    final AccelerationSettings settings = reflectionSettings.getReflectionSettings(new NamespaceKey(config.getFullPathList()));
    final boolean hasManualRefresh = settings.getNeverRefresh();

    final Optional<ReflectionEntry> entryOptional = Optional.ofNullable(entriesStore.get(id));
    if (!entryOptional.isPresent()) {
      // entry not created yet, e.g. reflection created but manager isn't awake yet
      return new ReflectionStatus(
        true,
        CONFIG_STATUS.OK,
        hasManualRefresh ? REFRESH_STATUS.MANUAL : REFRESH_STATUS.SCHEDULED,
        AVAILABILITY_STATUS.NONE,
        0, 0, 0);
    }
    final ReflectionEntry entry = entryOptional.get();

    final CONFIG_STATUS configStatus = validator.isValid(goal) ? CONFIG_STATUS.OK : CONFIG_STATUS.INVALID;

    REFRESH_STATUS refreshStatus = REFRESH_STATUS.SCHEDULED;
    if (entry.getState() == ReflectionState.REFRESHING || entry.getState() == ReflectionState.METADATA_REFRESH) {
      refreshStatus = REFRESH_STATUS.RUNNING;
    } else if (entry.getState() == ReflectionState.FAILED) {
      refreshStatus = REFRESH_STATUS.GIVEN_UP;
    } else if (hasManualRefresh) {
      refreshStatus = REFRESH_STATUS.MANUAL;
    }

    AVAILABILITY_STATUS availabilityStatus = AVAILABILITY_STATUS.NONE;

    long lastDataFetch = -1;
    long expiresAt = -1;

    // if no materialization available, can skip these
    final Materialization lastMaterializationDone = materializationStore.getLastMaterializationDone(id);
    if (lastMaterializationDone != null) {
      lastDataFetch = lastMaterializationDone.getLastRefreshFromPds();
      expiresAt = Optional.ofNullable(lastMaterializationDone.getExpiration()).orElse(0L);

      final Set<String> activeHosts = getActiveHosts();
      final long now = System.currentTimeMillis();
      if (hasMissingPartitions(lastMaterializationDone.getPartitionList(), activeHosts)) {
        availabilityStatus = AVAILABILITY_STATUS.INCOMPLETE;
      } else if (expiresAt < now) {
        availabilityStatus = AVAILABILITY_STATUS.EXPIRED;
      } else if (cacheViewer.get().isCached(lastMaterializationDone.getId())) {
        availabilityStatus = AVAILABILITY_STATUS.AVAILABLE;
      } else {
        // if the reflection has any valid materialization, then it can accelerate
        if (StreamSupport.stream(materializationStore.getAllDone(id, now).spliterator(), false)
          .anyMatch(
            m -> !hasMissingPartitions(m.getPartitionList(), activeHosts) && cacheViewer.get().isCached(m.getId())
            )
          ) {
          availabilityStatus = AVAILABILITY_STATUS.AVAILABLE;
        }
      }
    }

    int failureCount = entry.getNumFailures();

    return new ReflectionStatus(true, configStatus, refreshStatus, availabilityStatus, failureCount,
      lastDataFetch, expiresAt);
  }

  private Set<String> getActiveHosts() {
    return Sets.newHashSet(nodeEndpointsProvider.get().stream().map(CoordinationProtos.NodeEndpoint::getAddress).collect(Collectors.toList()));
  }

  private ExternalReflectionStatus.STATUS computeStatus(ExternalReflection reflection) {
    // check if the reflection is still valid
    final DatasetConfig queryDataset = namespaceService.get().findDatasetByUUID(reflection.getQueryDatasetId());
    if (queryDataset == null) {
      return ExternalReflectionStatus.STATUS.INVALID;
    }
    final DatasetConfig targetDataset = namespaceService.get().findDatasetByUUID(reflection.getTargetDatasetId());
    if (targetDataset == null) {
      return ExternalReflectionStatus.STATUS.INVALID;
    }

    // now check if the query and target datasets didn't change
    try {
      if (!ReflectionUtils.hashEquals(reflection.getQueryDatasetHash(), queryDataset, namespaceService.get())) {
        return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
      } else if (!ReflectionUtils.hashEquals(reflection.getTargetDatasetHash(), targetDataset, namespaceService.get())) {
        return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
      }
    } catch (NamespaceException e) {
      // most likely one of the ancestors has been deleted
      return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
    }

    // check that we are still able to get a MaterializationDescriptor
    try {
      if (ReflectionUtils.getMaterializationDescriptor(reflection, namespaceService.get()) == null) {
        return ExternalReflectionStatus.STATUS.INVALID;
      }
    } catch(NamespaceException e) {
      logger.debug("Could not find dataset for reflection {}", reflection.getId(), e);
      return ExternalReflectionStatus.STATUS.INVALID;
    }

    return ExternalReflectionStatus.STATUS.OK;
  }

  /**
   * Returns the status of an external reflection
   *
   * @param id The id of the reflection
   * @return The ExternalReflectionStatus representing the status of the reflection
   * @throws IllegalArgumentException if the reflection could not be found
   */
  @Override
  public ExternalReflectionStatus getExternalReflectionStatus(ReflectionId id) {
    final ExternalReflection reflection = externalReflectionStore.get(id.getId());
    Preconditions.checkArgument(reflection != null,"Reflection %s not found", id.getId());

    return new ExternalReflectionStatus(computeStatus(reflection));
  }

  @Override
  public Iterable<AccelerationListManager.ReflectionInfo> getReflections() {
    final Iterable<ReflectionGoal> goalReflections = ReflectionUtils.getAllReflections(goalsStore);
    final Iterable<ExternalReflection> externalReflections = externalReflectionStore.getExternalReflections();
    Stream<AccelerationListManager.ReflectionInfo> reflections = StreamSupport.stream(goalReflections.spliterator(),
      false).map(goal -> {
      final String dataset = quotedCompound(namespaceService.get().findDatasetByUUID(goal.getDatasetId()).getFullPathList());
      final Optional<ReflectionStatus> statusOpt = getNoThrowStatus(goal.getId());
      String combinedStatus = "UNKNOWN";
      int numFailures = 0;
      if (statusOpt.isPresent()) {
        combinedStatus = statusOpt.get().getCombinedStatus().toString();
        numFailures = statusOpt.get().getNumFailures();
      }

      return new AccelerationListManager.ReflectionInfo(
        goal.getId().getId(),
        goal.getName(),
        goal.getType().toString(),
        combinedStatus,
        numFailures,
        dataset,
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getSortFieldList()).stream().map(ReflectionField::getName).collect(Collectors.toList())),
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getPartitionFieldList()).stream().map(ReflectionField::getName).collect(Collectors.toList())),
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getDistributionFieldList()).stream().map(ReflectionField::getName).collect(Collectors.toList())),
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getDimensionFieldList()).stream().map(ReflectionDimensionField::getName).collect(Collectors.toList())),
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getMeasureFieldList()).stream().map(ReflectionMeasureField::getName).collect(Collectors.toList())),
        JOINER.join(AccelerationUtils.selfOrEmpty(goal.getDetails().getDisplayFieldList()).stream().map(ReflectionField::getName).collect(Collectors.toList())),
        null,
        goal.getArrowCachingEnabled()
      );
    });

    Stream<AccelerationListManager.ReflectionInfo> externalReflectionsInfo = StreamSupport.stream
      (externalReflections.spliterator(), false)
      .map(externalReflection -> {
          DatasetConfig dataset = namespaceService.get().findDatasetByUUID(externalReflection.getQueryDatasetId());
          if (dataset == null) {
            return null;
          }
          DatasetConfig targetDataset = namespaceService.get().findDatasetByUUID(externalReflection.getTargetDatasetId());
          if (targetDataset == null) {
            return null;
          }
          String datasetPath = quotedCompound(dataset.getFullPathList());
          String targetDatasetPath = quotedCompound(targetDataset.getFullPathList());
          final Optional<ExternalReflectionStatus> statusOptional = getNoThrowStatusForExternal(new ReflectionId(externalReflection.getId()));
          final String status = statusOptional.isPresent() ? statusOptional.get().getConfigStatus().toString() : "UNKNOWN";
          return new AccelerationListManager.ReflectionInfo(
            externalReflection.getId(),
            externalReflection.getName(),
            "EXTERNAL",
            status,
            0,
            datasetPath,
            null,
            null,
            null,
            null,
            null,
            null,
            targetDatasetPath,
            false
          );
      }).filter(Objects::nonNull);
    return Stream.concat(reflections, externalReflectionsInfo).collect(Collectors.toList());
  }

  @Override
  public Iterable<ReflectionRPC.RefreshInfo> getRefreshInfos() {
    return StreamSupport.stream(materializationStore.getAllRefreshes().spliterator(), false)
      .map(this::toProto).collect(Collectors.toList());
  }

  private ReflectionRPC.RefreshInfo toProto(Refresh refreshInfo) {
    ReflectionRPC.RefreshInfo.Builder refreshInfoBuilder = ReflectionRPC.RefreshInfo.newBuilder();

    if (refreshInfo.getId() != null) {
      refreshInfoBuilder.setId(refreshInfo.getId().getId());
    }
    if (refreshInfo.getReflectionId() != null) {
      refreshInfoBuilder.setReflectionId(refreshInfo.getReflectionId().getId());
    }
    if (refreshInfo.getSeriesId() != null) {
      refreshInfoBuilder.setSeriesId(refreshInfo.getSeriesId());
    }
    if (refreshInfo.getCreatedAt() != null) {
      refreshInfoBuilder.setCreatedAt(refreshInfo.getCreatedAt());
    }
    if (refreshInfo.getModifiedAt() != null) {
      refreshInfoBuilder.setModifiedAt(refreshInfo.getModifiedAt());
    }
    if(refreshInfo.getPath() != null) {
      refreshInfoBuilder.setPath(refreshInfo.getPath());
    }
    if (refreshInfo.getJob() != null) {
      if (refreshInfo.getJob().getJobId() != null) {
        refreshInfoBuilder.setJobId(refreshInfo.getJob().getJobId());
      }
      if (refreshInfo.getJob().getJobStart() != null) {
        refreshInfoBuilder.setJobStart(refreshInfo.getJob().getJobStart());
      }
      if (refreshInfo.getJob().getJobEnd() != null) {
        refreshInfoBuilder.setJobEnd(refreshInfo.getJob().getJobEnd());
      }
      if (refreshInfo.getJob().getInputBytes() != null) {
        refreshInfoBuilder.setInputBytes(refreshInfo.getJob().getInputBytes());
      }
      if (refreshInfo.getJob().getInputRecords() != null) {
        refreshInfoBuilder.setInputRecords(refreshInfo.getJob().getInputRecords());
      }
      if (refreshInfo.getJob().getOutputBytes() != null) {
        refreshInfoBuilder.setOutputBytes(refreshInfo.getJob().getOutputBytes());
      }
      if (refreshInfo.getJob().getOutputRecords() != null) {
        refreshInfoBuilder.setOutputRecords(refreshInfo.getJob().getOutputRecords());
      }
    }
    if (refreshInfo.getMetrics() != null) {
      if (refreshInfo.getMetrics().getFootprint() != null) {
        refreshInfoBuilder.setFootprint(refreshInfo.getMetrics().getFootprint());
      }
      if (refreshInfo.getMetrics().getOriginalCost() != null) {
        refreshInfoBuilder.setOriginalCost(refreshInfo.getMetrics().getOriginalCost());
      }
    }
    if (refreshInfo.getUpdateId() != null) {
      UpdateIdWrapper wrapper = new UpdateIdWrapper(refreshInfo.getUpdateId());
      String updateId = wrapper.toStringValue();
      refreshInfoBuilder.setUpdateId(updateId);
    }
    if (refreshInfo.getPartitionList() != null) {
      StringBuilder strB = new StringBuilder("[");
      for (DataPartition partition : refreshInfo.getPartitionList()) {
        strB.append("\"");
        strB.append(partition.getAddress());
        strB.append("\"");
        strB.append(",");
      }
      if (!refreshInfo.getPartitionList().isEmpty()) {
        strB.deleteCharAt(strB.length() - 1);
      }
      strB.append("]");
      refreshInfoBuilder.setPartition(strB.toString());
    }
    if (refreshInfo.getSeriesOrdinal() != null) {
      refreshInfoBuilder.setSeriesOrdinal(refreshInfo.getSeriesOrdinal());
    }
    return refreshInfoBuilder.build();
  }

  private Optional<ReflectionStatus> getNoThrowStatus(ReflectionId id) {
    try {
      return Optional.of(getReflectionStatus(id));
    } catch (IllegalArgumentException | IllegalStateException e) {
      logger.warn("Couldn't compute status for reflection {}", id, e);
    }

    return Optional.empty();
  }

  private Optional<ExternalReflectionStatus> getNoThrowStatusForExternal(ReflectionId externalReflectionId) {
    try {
      return Optional.of(getExternalReflectionStatus(externalReflectionId));
    } catch (IllegalArgumentException e) {
      logger.warn("Couldn't compute status for reflection {}", externalReflectionId, e);
    }

    return Optional.empty();
  }
}
