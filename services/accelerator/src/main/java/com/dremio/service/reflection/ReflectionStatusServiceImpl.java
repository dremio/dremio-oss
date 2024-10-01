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
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionUtils.getId;

import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.util.OptionUtil;
import com.dremio.options.OptionManager;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.UsedReflections;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS;
import com.dremio.service.reflection.ReflectionStatus.CONFIG_STATUS;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_METHOD;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Failure;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.inject.Provider;
import org.apache.calcite.util.Pair;

/** Computes the reflection status for a reflections and external reflections */
public class ReflectionStatusServiceImpl implements ReflectionStatusService {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReflectionStatusServiceImpl.class);

  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<CatalogService> catalogService;
  private final Provider<JobsService> jobsService;
  private final Provider<CacheViewer> cacheViewer;
  private final Provider<OptionManager> optionManager;

  private final ReflectionGoalsStore goalsStore;
  private final ReflectionEntriesStore entriesStore;
  private final MaterializationStore materializationStore;
  private final ExternalReflectionStore externalReflectionStore;

  private final ReflectionValidator validator;

  private static final Joiner JOINER = Joiner.on(", ");
  private final Provider<ReflectionUtils> reflectionUtils;

  @VisibleForTesting
  ReflectionStatusServiceImpl(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<CacheViewer> cacheViewer,
      ReflectionGoalsStore goalsStore,
      ReflectionEntriesStore entriesStore,
      MaterializationStore materializationStore,
      ExternalReflectionStore externalReflectionStore,
      ReflectionValidator validator,
      Provider<CatalogService> catalogService,
      Provider<JobsService> jobsService,
      Provider<OptionManager> optionManager,
      Provider<ReflectionUtils> reflectionUtils) {
    this.clusterCoordinatorProvider = Preconditions.checkNotNull(clusterCoordinatorProvider);
    this.cacheViewer = Preconditions.checkNotNull(cacheViewer, "cache viewer required");
    this.goalsStore = Preconditions.checkNotNull(goalsStore, "goals store required");
    this.entriesStore = Preconditions.checkNotNull(entriesStore, "entries store required");
    this.materializationStore =
        Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.externalReflectionStore =
        Preconditions.checkNotNull(externalReflectionStore, "external reflection store required");
    this.validator = Preconditions.checkNotNull(validator, "validator required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "job service required");
    this.optionManager = optionManager;
    this.reflectionUtils = reflectionUtils;
  }

  public ReflectionStatusServiceImpl(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<CatalogService> catalogService,
      Provider<JobsService> jobsService,
      Provider<LegacyKVStoreProvider> storeProvider,
      Provider<CacheViewer> cacheViewer,
      Provider<OptionManager> optionManager,
      Provider<ReflectionUtils> reflectionUtils) {
    this(
        clusterCoordinatorProvider,
        cacheViewer,
        new ReflectionGoalsStore(storeProvider),
        new ReflectionEntriesStore(storeProvider),
        new MaterializationStore(storeProvider),
        new ExternalReflectionStore(storeProvider),
        new ReflectionValidator(catalogService, optionManager),
        catalogService,
        jobsService,
        optionManager,
        reflectionUtils);
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

    // The dataset that the reflection refers to must exist.
    final EntityExplorer entityExplorer =
        catalogService
            .get()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .setCheckValidity(false)
                    .build());
    DremioTable table = entityExplorer.getTable(goal.getDatasetId());
    Preconditions.checkNotNull(table, "Dataset not present for reflection %s", id.getId());
    return getReflectionStatus(
        goal, Optional.ofNullable(materializationStore.getLastMaterializationDone(id)), table);
  }

  @Override
  public ReflectionStatus getReflectionStatus(
      ReflectionGoal goal,
      Optional<Materialization> lastMaterializationDone,
      DremioTable dremioTable) {
    ReflectionId id = goal.getId();
    // should never be called on a deleted reflection
    Preconditions.checkState(
        goal.getState() != ReflectionGoalState.DELETED,
        "Getting status for deleted reflection %s",
        id.getId());

    if (goal.getState() == ReflectionGoalState.DISABLED) {
      return new ReflectionStatus(
          cacheViewer.get().isInitialized(),
          false,
          CONFIG_STATUS.OK,
          REFRESH_STATUS.SCHEDULED,
          AVAILABILITY_STATUS.NONE,
          0,
          null,
          -1,
          -1,
          REFRESH_METHOD.NONE,
          -1);
    }

    final Optional<ReflectionEntry> entryOptional = Optional.ofNullable(entriesStore.get(id));
    if (!entryOptional.isPresent()) {
      // entry not created yet, e.g. reflection goal created but manager isn't awake yet
      REFRESH_STATUS refreshStatus = REFRESH_STATUS.SCHEDULED;

      // If a goal is still ENABLED yet still doesn't have a ReflectionEntry, then something really
      // bad happened.
      // This could include the reflection manager no longer syncing because DCS taskleader election
      // failed,
      // a hang/deadlock in the reflection manager or somehow a sync took longer than 60s and
      // skipped this goal.
      final long lastWakeupTime = System.currentTimeMillis() - ReflectionManager.WAKEUP_OVERLAP_MS;
      if (goal.getCreatedAt() < lastWakeupTime) {
        refreshStatus = REFRESH_STATUS.GIVEN_UP;
      }
      return new ReflectionStatus(
          cacheViewer.get().isInitialized(),
          true,
          CONFIG_STATUS.OK,
          refreshStatus,
          AVAILABILITY_STATUS.NONE,
          0,
          null,
          -1,
          -1,
          REFRESH_METHOD.NONE,
          -1);
    }

    final ReflectionEntry entry = entryOptional.get();
    final CONFIG_STATUS configStatus =
        validator.isValid(goal, dremioTable) ? CONFIG_STATUS.OK : CONFIG_STATUS.INVALID;

    REFRESH_STATUS refreshStatus;
    switch (entry.getState()) {
      case REFRESH_PENDING:
        refreshStatus = REFRESH_STATUS.PENDING;
        break;
      case REFRESH:
      case REFRESHING:
      case UPDATE:
      case METADATA_REFRESH:
      case COMPACTING:
        refreshStatus = REFRESH_STATUS.RUNNING;
        break;
      case FAILED:
        refreshStatus = REFRESH_STATUS.GIVEN_UP;
        break;
      case DEPRECATE:
        // Should never get here
        refreshStatus = REFRESH_STATUS.MANUAL;
        break;
      case ACTIVE:
        refreshStatus =
            reflectionUtils.get().getRefreshStatusForActiveReflection(optionManager.get(), entry);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + entry.getState());
    }

    AVAILABILITY_STATUS availabilityStatus = AVAILABILITY_STATUS.NONE;

    long lastDataFetch = -1;
    long expiresAt = -1;
    long lastRefreshDuration = -1;

    // if no materialization available, can skip these
    if (lastMaterializationDone.isPresent()) {
      Materialization materialization = lastMaterializationDone.get();
      lastDataFetch = materialization.getLastRefreshFromPds();
      lastRefreshDuration =
          Optional.ofNullable(materialization.getLastRefreshDurationMillis()).orElse(-1L);
      expiresAt = Optional.ofNullable(materialization.getExpiration()).orElse(0L);

      if (cacheViewer.get().isCached(materialization.getId())) {
        availabilityStatus = AVAILABILITY_STATUS.AVAILABLE;
      } else if (optionManager.get().getOption(MATERIALIZATION_CACHE_ENABLED)
          && Optional.ofNullable(materialization.getLastRefreshFinished()).orElse(0L)
                  + optionManager.get().getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS)
              > System.currentTimeMillis()) {
        // Continue to show RUNNING between the time the materialization is DONE but not yet in the
        // materialization cache
        refreshStatus = REFRESH_STATUS.RUNNING;
      }
    }

    int failureCount = entry.getNumFailures();
    Failure lastFailure = entry.getLastFailure();

    REFRESH_METHOD refreshMethod = REFRESH_METHOD.NONE;
    if (entry.getRefreshMethod() != null) {
      switch (entry.getRefreshMethod()) {
        case FULL:
          refreshMethod = REFRESH_METHOD.FULL;
          break;
        case INCREMENTAL:
          refreshMethod = REFRESH_METHOD.INCREMENTAL;
          break;
        default:
          refreshMethod = REFRESH_METHOD.NONE;
      }
    }

    return new ReflectionStatus(
        cacheViewer.get().isInitialized(),
        true,
        configStatus,
        refreshStatus,
        availabilityStatus,
        failureCount,
        lastFailure,
        lastDataFetch,
        expiresAt,
        refreshMethod,
        lastRefreshDuration);
  }

  private Set<String> getActiveHosts() {
    return clusterCoordinatorProvider.get().getExecutorEndpoints().stream()
        .map(CoordinationProtos.NodeEndpoint::getAddress)
        .collect(Collectors.toSet());
  }

  private ExternalReflectionStatus.STATUS computeStatus(ExternalReflection reflection) {
    final EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    // check if the reflection is still valid
    DatasetConfig queryDatasetConfig =
        CatalogUtil.getDatasetConfig(catalog, reflection.getQueryDatasetId());
    if (queryDatasetConfig == null) {
      return ExternalReflectionStatus.STATUS.INVALID;
    }
    DatasetConfig targetDatasetConfig =
        CatalogUtil.getDatasetConfig(catalog, reflection.getTargetDatasetId());
    if (targetDatasetConfig == null) {
      return ExternalReflectionStatus.STATUS.INVALID;
    }

    // now check if the query and target datasets didn't change
    try {
      if (!DatasetHashUtils.hashEquals(
          reflection.getQueryDatasetHash(), queryDatasetConfig, catalog)) {
        return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
      } else if (!DatasetHashUtils.hashEquals(
          reflection.getTargetDatasetHash(), targetDatasetConfig, catalog)) {
        return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
      }
    } catch (NamespaceException e) {
      // most likely one of the ancestors has been deleted
      return ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
    }

    // check that we are still able to get a MaterializationDescriptor
    try {
      if (ReflectionUtils.getMaterializationDescriptor(reflection, catalog) == null) {
        return ExternalReflectionStatus.STATUS.INVALID;
      }
    } catch (NamespaceException e) {
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
    Preconditions.checkArgument(reflection != null, "Reflection %s not found", id.getId());

    return new ExternalReflectionStatus(computeStatus(reflection));
  }

  @Override
  public Iterator<AccelerationListManager.ReflectionInfo> getReflections() {
    final Iterable<ReflectionGoal> goalReflections = ReflectionUtils.getAllReflections(goalsStore);
    final Iterable<ExternalReflection> externalReflections =
        externalReflectionStore.getExternalReflections();
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());

    final List<ReflectionId> reflectionIds =
        StreamSupport.stream(goalReflections.spliterator(), false)
            .map(ReflectionGoal::getId)
            .collect(Collectors.toList());
    final List<Integer> reflectionConsideredJobCounts =
        getReflectionUsedJobCounts(reflectionIds, UsedReflections.UsageType.CONSIDERED);
    final List<Integer> reflectionMatchedJobCounts =
        getReflectionUsedJobCounts(reflectionIds, UsedReflections.UsageType.MATCHED);
    final List<Integer> reflectionChosenJobCounts =
        getReflectionUsedJobCounts(reflectionIds, UsedReflections.UsageType.CHOSEN);
    AtomicInteger reflectionCount = new AtomicInteger();
    Stream<AccelerationListManager.ReflectionInfo> reflections =
        StreamSupport.stream(goalReflections.spliterator(), false)
            .map(
                goal -> {
                  try {
                    final DatasetConfig datasetConfig =
                        CatalogUtil.getDatasetConfig(catalog, goal.getDatasetId());
                    if (datasetConfig == null) {
                      return null;
                    }
                    final Optional<ReflectionStatus> statusOpt = getNoThrowStatus(goal.getId());
                    String combinedStatus = "UNKNOWN";
                    int numFailures = 0;
                    String lastFailureMessage = null;
                    String lastFailureStack = null;
                    String refreshStatus = null;
                    String accelerationStatus = null;
                    long lastRefreshDurationMillis = -1;
                    String refreshMethod = null;
                    long expiresAt = -1;
                    if (statusOpt.isPresent()) {
                      combinedStatus = statusOpt.get().getCombinedStatus().toString();
                      numFailures = statusOpt.get().getNumFailures();
                      if (statusOpt.get().getLastFailure() != null) {
                        lastFailureMessage = statusOpt.get().getLastFailure().getMessage();
                        lastFailureStack = statusOpt.get().getLastFailure().getStackTrace();
                      }
                      refreshStatus = statusOpt.get().getRefreshStatus().toString();
                      accelerationStatus = statusOpt.get().getAvailabilityStatus().toString();
                      lastRefreshDurationMillis = statusOpt.get().getLastRefreshDuration();
                      refreshMethod = statusOpt.get().getRefreshMethod().toString();
                      expiresAt = statusOpt.get().getExpiresAt();
                    }

                    Optional<Materialization> materialization =
                        Optional.ofNullable(
                            materializationStore.getLastMaterializationDone(goal.getId()));
                    long currentSize = 0;
                    long recordCount = -1;
                    long lastRefreshFromTable = 0;

                    if (materialization.isPresent()) {
                      Pair<MaterializationMetrics, Long> metrics =
                          getReflectionSize(materialization.get());
                      currentSize = metrics.left.getFootprint();
                      recordCount = metrics.right;
                      lastRefreshFromTable = materialization.get().getLastRefreshFromPds();
                    }

                    int cur = reflectionCount.getAndIncrement();
                    return new AccelerationListManager.ReflectionInfo(
                        goal.getId().getId(),
                        goal.getName(),
                        goal.getType().toString(),
                        Optional.ofNullable(goal.getCreatedAt()).orElse(0L) == 0
                            ? null
                            : new Timestamp(goal.getCreatedAt()),
                        Optional.ofNullable(goal.getModifiedAt()).orElse(0L) == 0
                            ? null
                            : new Timestamp(goal.getModifiedAt()),
                        combinedStatus,
                        numFailures,
                        lastFailureMessage,
                        lastFailureStack,
                        datasetConfig.getId().getId(),
                        quotedCompound(datasetConfig.getFullPathList()),
                        datasetConfig.getType().toString(),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(goal.getDetails().getSortFieldList())
                                .stream()
                                .map(ReflectionField::getName)
                                .collect(Collectors.toList())),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(goal.getDetails().getPartitionFieldList())
                                .stream()
                                .map(f -> getPartitionInfo(f))
                                .collect(Collectors.toList())),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(
                                    goal.getDetails().getDistributionFieldList())
                                .stream()
                                .map(ReflectionField::getName)
                                .collect(Collectors.toList())),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(goal.getDetails().getDimensionFieldList())
                                .stream()
                                .map(ReflectionDimensionField::getName)
                                .collect(Collectors.toList())),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(goal.getDetails().getMeasureFieldList())
                                .stream()
                                .map(ReflectionMeasureField::getName)
                                .collect(Collectors.toList())),
                        JOINER.join(
                            AccelerationUtils.selfOrEmpty(goal.getDetails().getDisplayFieldList())
                                .stream()
                                .map(ReflectionField::getName)
                                .collect(Collectors.toList())),
                        null,
                        goal.getArrowCachingEnabled(),
                        refreshStatus,
                        accelerationStatus,
                        recordCount,
                        currentSize,
                        getTotalReflectionSize(goal.getId()),
                        lastRefreshDurationMillis,
                        lastRefreshFromTable == 0 ? null : new Timestamp(lastRefreshFromTable),
                        refreshMethod,
                        expiresAt == -1 ? null : new Timestamp(expiresAt),
                        optionManager.get().getOption(PlannerSettings.ENABLE_JOB_COUNT_CONSIDERED)
                            ? reflectionConsideredJobCounts.get(cur)
                            : -1,
                        optionManager.get().getOption(PlannerSettings.ENABLE_JOB_COUNT_MATCHED)
                            ? reflectionMatchedJobCounts.get(cur)
                            : -1,
                        optionManager.get().getOption(PlannerSettings.ENABLE_JOB_COUNT_CHOSEN)
                            ? reflectionChosenJobCounts.get(cur)
                            : -1);
                  } catch (Exception e) {
                    logger.debug("Unable to get ReflectionInfo for {}", getId(goal), e);
                  }
                  return null;
                })
            .filter(Objects::nonNull);

    Stream<AccelerationListManager.ReflectionInfo> externalReflectionsInfo =
        StreamSupport.stream(externalReflections.spliterator(), false)
            .map(
                externalReflection -> {
                  try {
                    DatasetConfig dataset =
                        CatalogUtil.getDatasetConfig(
                            catalog, externalReflection.getQueryDatasetId());
                    if (dataset == null) {
                      return null;
                    }
                    DatasetConfig targetDataset =
                        CatalogUtil.getDatasetConfig(
                            catalog, externalReflection.getTargetDatasetId());
                    if (targetDataset == null) {
                      return null;
                    }
                    String targetDatasetPath = quotedCompound(targetDataset.getFullPathList());
                    final Optional<ExternalReflectionStatus> statusOptional =
                        getNoThrowStatusForExternal(new ReflectionId(externalReflection.getId()));
                    final String status =
                        statusOptional.isPresent()
                            ? statusOptional.get().getConfigStatus().toString()
                            : "UNKNOWN";
                    return new AccelerationListManager.ReflectionInfo(
                        externalReflection.getId(),
                        externalReflection.getName(),
                        "EXTERNAL",
                        null,
                        null,
                        status,
                        0,
                        null,
                        null,
                        dataset.getId().getId(),
                        quotedCompound(dataset.getFullPathList()),
                        dataset.getType().toString(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        targetDatasetPath,
                        false,
                        null,
                        null,
                        -1,
                        -1,
                        -1,
                        -1,
                        null,
                        null,
                        null,
                        -1,
                        -1,
                        -1);
                  } catch (Exception e) {
                    logger.debug(
                        "Unable to get ReflectionInfo for {}", getId(externalReflection), e);
                  }
                  return null;
                })
            .filter(Objects::nonNull);
    return Stream.concat(reflections, externalReflectionsInfo).iterator();
  }

  public String getPartitionInfo(ReflectionPartitionField reflectionField) {
    PartitionTransform partitionTransform = ReflectionUtils.getPartitionTransform(reflectionField);
    if (partitionTransform.isIdentity()) {
      return reflectionField.getName();
    }
    return partitionTransform.toString();
  }

  @Override
  public Iterator<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse> getRefreshInfos() {
    return StreamSupport.stream(materializationStore.getAllRefreshes().spliterator(), false)
        .map(this::toProto)
        .iterator();
  }

  private ReflectionDescriptionServiceRPC.GetRefreshInfoResponse toProto(Refresh refreshInfo) {
    ReflectionDescriptionServiceRPC.GetRefreshInfoResponse.Builder refreshInfoBuilder =
        ReflectionDescriptionServiceRPC.GetRefreshInfoResponse.newBuilder();

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
    if (refreshInfo.getPath() != null) {
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

  private Optional<ExternalReflectionStatus> getNoThrowStatusForExternal(
      ReflectionId externalReflectionId) {
    try {
      return Optional.of(getExternalReflectionStatus(externalReflectionId));
    } catch (IllegalArgumentException e) {
      logger.warn("Couldn't compute status for reflection {}", externalReflectionId, e);
    }

    return Optional.empty();
  }

  @Override
  public long getTotalReflectionSize(ReflectionId reflectionId) {
    Iterable<Refresh> refreshes = materializationStore.getRefreshesByReflectionId(reflectionId);
    long size = 0;
    for (Refresh refresh : refreshes) {
      if (refresh.getMetrics() != null) {
        size += Optional.ofNullable(refresh.getMetrics().getFootprint()).orElse(0L);
      }
    }
    return size;
  }

  @Override
  public Pair<MaterializationMetrics, Long> getReflectionSize(Materialization materialization) {
    return materializationStore.getMetrics(materialization);
  }

  /**
   * Get used job counts of the provided usage type for a list of reflections from Job Service.
   *
   * @param reflectionIds
   * @param usageType
   * @return
   */
  private List<Integer> getReflectionUsedJobCounts(
      List<ReflectionId> reflectionIds, UsedReflections.UsageType usageType) {
    final JobCountsRequest.Builder builder = JobCountsRequest.newBuilder();
    builder.getReflectionsBuilder().setUsageType(usageType);
    reflectionIds.forEach(
        reflectionId -> builder.getReflectionsBuilder().addReflectionIds(reflectionId.getId()));
    builder.setJobCountsAgeInDays(
        OptionUtil.getJobCountsAgeInDays(
            optionManager.get().getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS)));
    return jobsService.get().getJobCounts(builder.build()).getCountList();
  }
}
