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
package com.dremio.service.reflection.refresh;

import static com.dremio.service.accelerator.AccelerationUtils.selfOrEmpty;
import static com.dremio.service.reflection.ReflectionUtils.getId;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.planner.acceleration.MaterializationExpander;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.RecordWriter;
import com.dremio.io.file.Path;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.job.proto.Acceleration.Substitution;
import com.dremio.service.job.proto.ExtraInfo;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JoinAnalyzer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.DependencyGraph.DependencyException;
import com.dremio.service.reflection.DependencyManager;
import com.dremio.service.reflection.DependencyResolutionContext;
import com.dremio.service.reflection.DependencyUtils;
import com.dremio.service.reflection.ExtractedDependencies;
import com.dremio.service.reflection.ReflectionServiceImpl.ExpansionHelper;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.JobDetails;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.MaterializationPlan;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.RelNode;

/** Handles a completed job. Created to handle a single job and then discarded. */
public class RefreshDoneHandler {
  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RefreshDoneHandler.class);
  private final DependencyManager dependencyManager;
  private final MaterializationStore materializationStore;
  private final MaterializationPlanStore materializationPlanStore;
  private final Function<Catalog, ExpansionHelper> expansionHelper;
  private final Path accelerationBasePath;

  private final ReflectionEntry reflection;
  private final Materialization materialization;
  private final com.dremio.service.job.JobDetails job;
  private final JobsService jobsService;
  private final BufferAllocator allocator;
  private final CatalogService catalogService;
  private final DependencyResolutionContext dependencyResolutionContext;
  private final SabotConfig sabotConfig;

  public RefreshDoneHandler(
      ReflectionEntry entry,
      Materialization materialization,
      com.dremio.service.job.JobDetails job,
      JobsService jobsService,
      MaterializationStore materializationStore,
      MaterializationPlanStore materializationPlanStore,
      DependencyManager dependencyManager,
      Function<Catalog, ExpansionHelper> expansionHelper,
      Path accelerationBasePath,
      BufferAllocator allocator,
      CatalogService catalogService,
      DependencyResolutionContext dependencyResolutionContext,
      SabotConfig sabotConfig) {
    this.reflection = Preconditions.checkNotNull(entry, "reflection entry required");
    this.materialization = Preconditions.checkNotNull(materialization, "materialization required");
    this.job = Preconditions.checkNotNull(job, "jobDetails required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobsService required");
    this.dependencyManager = Preconditions.checkNotNull(dependencyManager, "dependencies required");
    this.materializationStore = materializationStore;
    this.materializationPlanStore = materializationPlanStore;
    this.expansionHelper = Preconditions.checkNotNull(expansionHelper, "expansion helper required");
    this.accelerationBasePath =
        Preconditions.checkNotNull(accelerationBasePath, "acceleration base path required");
    this.allocator = allocator;
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalogService required");
    this.dependencyResolutionContext = Preconditions.checkNotNull(dependencyResolutionContext);
    this.sabotConfig = Preconditions.checkNotNull(sabotConfig);
  }

  public RefreshDoneHandler(RefreshDoneHandler other) {
    this.reflection = other.reflection;
    this.materialization = other.materialization;
    this.job = other.job;
    this.jobsService = other.jobsService;
    this.dependencyManager = other.dependencyManager;
    this.materializationStore = other.materializationStore;
    this.materializationPlanStore = other.materializationPlanStore;
    this.expansionHelper = other.expansionHelper;
    this.accelerationBasePath = other.accelerationBasePath;
    this.allocator = other.allocator;
    this.catalogService = other.catalogService;
    this.dependencyResolutionContext = other.dependencyResolutionContext;
    this.sabotConfig = other.sabotConfig;
  }

  /**
   * computes various materialization attributes and stats and saves the materialization in the
   * store
   *
   * @return refresh decision
   * @throws NamespaceException if we fail to access a dataset while updating the dependencies
   * @throws IllegalStateException if the materialization is missing refreshes
   * @throws DependencyException if cyclic dependency detected
   */
  public RefreshDecision handle() throws NamespaceException, DependencyException {
    JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    Preconditions.checkState(
        lastAttempt.getState() == JobState.COMPLETED,
        "Cannot handle job with non completed state %s",
        lastAttempt.getState());

    final RefreshDecision decision = getRefreshDecision(lastAttempt);

    final ByteString planBytes =
        Preconditions.checkNotNull(
            decision.getLogicalPlan(), "refresh jobInfo has no logical plan");

    updateDependencies(reflection, lastAttempt.getInfo(), decision, dependencyManager);

    failIfNotEnoughRefreshesAvailable(decision);

    final JobDetails details = ReflectionUtils.computeJobDetails(lastAttempt);
    final boolean dataWritten =
        Optional.ofNullable(details.getOutputRecords()).orElse(0L) > 0
            || lastAttempt.getStats().getRemovedFiles() > 0;
    boolean isEmptyReflection =
        getIsEmptyReflection(
            decision.getInitialRefresh().booleanValue(), dataWritten, materialization);
    final Refresh previousRefresh =
        materializationStore.getMostRecentRefresh(materialization.getReflectionId());
    boolean updateIdNeedsOverwrite = updateIdNeedsOverwrite(decision, previousRefresh);
    if (dataWritten || isEmptyReflection || updateIdNeedsOverwrite) {
      createAndSaveRefresh(details, decision, lastAttempt);
    } else {
      logger.debug(
          "materialization {} didn't write any data, we won't create a refresh entry",
          getId(materialization));
    }

    final Optional<Long> oldestDependentMaterialization =
        dependencyManager.getOldestDependentMaterialization(reflection.getId());
    final long lastRefreshFromTable =
        oldestDependentMaterialization.orElse(materialization.getInitRefreshSubmit());
    if (!dataWritten && !decision.getInitialRefresh()) {
      populateMaterializationForNoopRefresh(decision, details, lastRefreshFromTable);
    } else {
      long currentTime = System.currentTimeMillis();
      final JoinAnalysis joinAnalysis = computeJoinAnalysis(decision);
      materialization
          .setDisableDefaultReflection(decision.getDisableDefaultReflection() == Boolean.TRUE)
          .setExpiration(computeExpiration())
          .setInitRefreshExecution(details.getJobStart())
          .setLastRefreshFromPds(lastRefreshFromTable)
          .setLastRefreshFinished(currentTime)
          .setLastRefreshDurationMillis(currentTime - materialization.getInitRefreshSubmit())
          .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
          .setSeriesId(decision.getSeriesId())
          .setSeriesOrdinal(
              dataWritten
                  ? decision.getSeriesOrdinal()
                  : Math.max(decision.getSeriesOrdinal() - 1, 0))
          .setJoinAnalysis(joinAnalysis)
          .setPartitionList(getDataPartitions());
    }

    materializationStore.save(materialization);
    MaterializationPlan plan = new MaterializationPlan();
    plan.setId(MaterializationPlanStore.createMaterializationPlanId(materialization.getId()));
    plan.setMaterializationId(materialization.getId());
    plan.setReflectionId(materialization.getReflectionId());
    plan.setVersion(DremioVersionInfo.getVersion());
    plan.setLogicalPlan(planBytes);
    materializationPlanStore.save(plan);

    return decision;
  }

  /**
   * Populate various fields in materialization before it is saved. For redundant (noop) refresh
   * only
   */
  protected void populateMaterializationForNoopRefresh(
      RefreshDecision decision, JobDetails details, long lastRefreshFromTable) {
    // if we don't create a refresh entry we still need to copy the materialization fields
    // from the previous materialization as it will be owning the same refreshes
    final Materialization lastDone =
        Preconditions.checkNotNull(
            materializationStore.getLastMaterializationDone(materialization.getReflectionId()),
            "incremental refresh didn't write any data and previous materializations expired");
    long currentTime = System.currentTimeMillis();
    materialization
        .setDisableDefaultReflection(decision.getDisableDefaultReflection() == Boolean.TRUE)
        .setExpiration(computeExpiration())
        .setInitRefreshExecution(details.getJobStart())
        .setLastRefreshFromPds(lastRefreshFromTable)
        .setLastRefreshFinished(currentTime)
        .setLastRefreshDurationMillis(currentTime - materialization.getInitRefreshSubmit())
        .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
        .setSeriesId(decision.getSeriesId())
        .setSeriesOrdinal(lastDone.getSeriesOrdinal())
        .setJoinAnalysis(lastDone.getJoinAnalysis())
        .setPartitionList(lastDone.getPartitionList())
        .setIsNoopRefresh(true);
  }

  private boolean updateIdNeedsOverwrite(RefreshDecision decision, Refresh previousRefresh) {
    final boolean isFull = decision.getAccelerationSettings().getMethod() == RefreshMethod.FULL;
    if (decision.getInitialRefresh()
        || decision.getOutputUpdateId() == null
        || previousRefresh == null
        || isFull) {
      return false;
    }
    return !decision.getOutputUpdateId().equals(previousRefresh.getUpdateId())
        && !decision.getNoOpRefresh();
  }

  /**
   * Determines if the current reflection has 0 rows and is an Iceberg Reflection We will only allow
   * the initial refresh for Iceberg Materialization to be empty We don't need to handle incremental
   * refreshes, as we can still use the previous refresh in the series We don't want to handle
   * non-Iceberg materialization due to possible path discrepancy
   *
   * @param initialRefresh is it Initial Refresh or later refresh
   * @param dataWritten Was any data written while saving the reflection
   * @param materialization current materialization
   * @return true if it is based on an Empty Iceberg Reflection
   */
  public static boolean getIsEmptyReflection(
      boolean initialRefresh, boolean dataWritten, Materialization materialization) {

    boolean allowEmptyRefresh =
        initialRefresh
            && materialization.getIsIcebergDataset() != null
            && materialization.getIsIcebergDataset();
    return !dataWritten && allowEmptyRefresh;
  }

  public RefreshDecision getRefreshDecision(final JobAttempt jobAttempt) {
    if (jobAttempt.getExtraInfoList() == null || jobAttempt.getExtraInfoList().isEmpty()) {
      throw new IllegalStateException("No refresh decision found in refresh job.");
    }

    List<ExtraInfo> extraInfo =
        jobAttempt.getExtraInfoList().stream()
            .filter(i -> RefreshHandler.DECISION_NAME.equals(i.getName()))
            .collect(Collectors.toList());

    if (extraInfo.size() != 1) {
      throw new IllegalStateException(
          String.format("Expected to have one refresh decision, saw: %d.", extraInfo.size()));
    }

    return RefreshHandler.ABSTRACT_SERIALIZER.revert(extraInfo.get(0).getData().toByteArray());
  }

  /**
   * throws {@link IllegalStateException} if some refreshes owned by the materialization are
   * missing. This can only happen if the materialization somehow took too long to finish and the
   * previous incremental materialization was deleted along with its refreshes
   */
  private void failIfNotEnoughRefreshesAvailable(final RefreshDecision decision) {
    if (decision.getInitialRefresh() || decision.getNoOpRefresh()) {
      return;
    }

    final int seriesOrdinal = decision.getSeriesOrdinal();
    // seriesOrdinal is 0-based so we should expect number of refreshes in the store to be equal to
    // the seriesOrdinal
    Iterable<Refresh> refreshes =
        materializationStore.getRefreshesForSeries(reflection.getId(), decision.getSeriesId());
    final int numRefreshes = Iterables.size(refreshes);
    Preconditions.checkState(
        numRefreshes == seriesOrdinal,
        "Materialization %s is missing refreshes. Expected %s but only found %s",
        getId(materialization),
        seriesOrdinal,
        numRefreshes);
  }

  public void updateDependencies(
      final ReflectionEntry entry,
      final JobInfo info,
      final RefreshDecision decision,
      final DependencyManager dependencyManager)
      throws NamespaceException, DependencyException {
    final ExtractedDependencies dependencies =
        DependencyUtils.extractDependencies(info, decision, catalogService);
    if (decision.getInitialRefresh()) {
      if (dependencies.isEmpty()) {
        throw UserException.reflectionError()
            .message(
                "Could not find any physical dependencies for reflection %s most likely "
                    + "because one of its datasets has a select with options or it's a select from values",
                entry.getId())
            .build(logger);
      }

      dependencyManager.setDependencies(entry.getId(), dependencies);
    } else if (!dependencies.getPlanDependencies().isEmpty()) {
      // for incremental refresh, only update the dependencies if planDependencies are not empty,
      // otherwise it's most
      // likely an empty incremental refresh
      dependencyManager.setDependencies(entry.getId(), dependencies);
    }
    dependencyManager.updateRefreshPolicyType(entry, dependencyResolutionContext);
    dependencyManager.updateStaleMaterialization(entry.getId());
  }

  private void createAndSaveRefresh(
      final JobDetails details, final RefreshDecision decision, final JobAttempt lastAttempt) {
    final JobId jobId = JobsProtoUtil.toStuff(job.getJobId());
    final boolean isFull = decision.getAccelerationSettings().getMethod() == RefreshMethod.FULL;
    final UpdateId updateId;
    if (isFull) {
      updateId = new UpdateId();
    } else {
      if (decision.getOutputUpdateId() != null) {
        updateId = decision.getOutputUpdateId();
      } else {
        updateId = getUpdateId(jobId, jobsService, allocator);
        if (decision.getAccelerationSettings().getRefreshField() != null) {
          updateId.setUpdateIdType(UpdateId.IdType.FIELD);
        } else {
          updateId.setUpdateIdType(UpdateId.IdType.MTIME);
        }
      }
    }
    final MaterializationMetrics metrics =
        ReflectionUtils.computeMetrics(job, jobsService, allocator, jobId);
    final List<DataPartition> dataPartitions =
        ReflectionUtils.computeDataPartitions(JobsProtoUtil.getLastAttempt(job).getInfo());
    final AttemptId attemptId = AttemptIdUtils.fromString(lastAttempt.getAttemptId());
    final List<String> refreshPath =
        RefreshHandler.getRefreshPath(
            materialization.getReflectionId(), materialization, decision, attemptId);
    final boolean isIcebergRefresh =
        materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    final String icebergBasePath =
        ReflectionUtils.getIcebergReflectionBasePath(refreshPath, isIcebergRefresh);
    Preconditions.checkArgument(
        !isIcebergRefresh
            || decision.getInitialRefresh()
            || icebergBasePath.equals(materialization.getBasePath()));
    final Refresh refresh =
        ReflectionUtils.createRefresh(
            reflection.getId(),
            refreshPath,
            decision.getSeriesId(),
            decision.getSeriesOrdinal(),
            updateId,
            details,
            metrics,
            dataPartitions,
            isIcebergRefresh,
            icebergBasePath);

    logger.trace("Refresh created: {}", refresh);
    materializationStore.save(refresh);

    logger.debug(
        "Refresh written to {} for {}",
        PathUtils.constructFullPath(refreshPath),
        ReflectionUtils.getId(materialization));
  }

  private List<DataPartition> getDataPartitions() {
    return ImmutableList.copyOf(
        materializationStore
            .getRefreshes(materialization)
            .transformAndConcat(
                new Function<Refresh, Iterable<DataPartition>>() {
                  @Override
                  public Iterable<DataPartition> apply(Refresh input) {
                    return input.getPartitionList() != null
                        ? input.getPartitionList()
                        : ImmutableList.of();
                  }
                })
            .toSet());
  }

  protected JoinAnalysis computeJoinAnalysis(RefreshDecision decision) {
    final JobInfo info = JobsProtoUtil.getLastAttempt(job).getInfo();
    JoinAnalysis joinAnalysis = getJoinAnalysis(info, decision);
    if (joinAnalysis == null) {
      return null;
    }
    if (info.getAcceleration() != null) {
      for (Substitution sub : selfOrEmpty(info.getAcceleration().getSubstitutionsList())) {
        final MaterializationId materializationId =
            new MaterializationId(sub.getId().getMaterializationId());
        Materialization usedMaterialization = materializationStore.get(materializationId);
        MaterializationPlan usedMaterializationPlan =
            materializationPlanStore.getVersionedPlan(materializationId);
        if (usedMaterialization == null || usedMaterializationPlan == null) {
          continue;
        }

        try (ExpansionHelper helper =
            expansionHelper.apply(CatalogUtil.getSystemCatalogForReflections(catalogService))) {
          RelNode usedMaterializationLogicalPlan =
              MaterializationExpander.deserializePlan(
                  usedMaterializationPlan.getLogicalPlan().toByteArray(),
                  helper.getConverter(),
                  catalogService);
          if (usedMaterialization.getJoinAnalysis() != null) {
            joinAnalysis =
                JoinAnalyzer.merge(
                    joinAnalysis,
                    usedMaterialization.getJoinAnalysis(),
                    usedMaterializationLogicalPlan,
                    usedMaterialization.getId().getId());
          }
        }
      }
    }

    return joinAnalysis;
  }

  /** Given a JobInfo and Refresh Decision return the Join Analysis for this job */
  protected JoinAnalysis getJoinAnalysis(JobInfo info, RefreshDecision decision) {
    return info.getJoinAnalysis();
  }

  /**
   * @return next updateId
   */
  private static UpdateId getUpdateId(
      final JobId jobId, final JobsService jobsService, BufferAllocator allocator) {
    final int fetchLimit = 1000;
    UpdateIdWrapper updateIdWrapper = new UpdateIdWrapper();

    int offset = 0;
    while (true) {
      try (final JobDataFragment data =
          JobDataClientUtils.getJobData(jobsService, allocator, jobId, offset, fetchLimit)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          byte[] b = (byte[]) data.extractValue(RecordWriter.METADATA_COLUMN, i);
          if (b == null) {
            throw new IllegalStateException("Didn't find metadata output for job " + jobId.getId());
          }
          updateIdWrapper.update(UpdateIdWrapper.deserialize(b));
        }
        offset += data.getReturnedRowCount();
      }
    }
    return updateIdWrapper.getUpdateId();
  }

  /**
   * compute reflection own expiration time.
   *
   * <p>if the query doesn't depend directly on pds the grace period will be set to Long MAX as the
   * expectation is that we will compute a proper expiration time from the dependent reflections
   *
   * @return expiration time
   */
  private long computeExpiration() {
    final ReflectionId reflectionId = materialization.getReflectionId();
    final long jobStart = materialization.getInitRefreshSubmit();
    final Optional<Long> gracePeriod =
        dependencyManager.getGracePeriod(reflectionId, dependencyResolutionContext);
    final Optional<Long> earliestExpiration = dependencyManager.getEarliestExpiration(reflectionId);

    if (gracePeriod.isPresent() && earliestExpiration.isPresent()) {
      return Math.min(earliestExpiration.get(), jobStart + gracePeriod.get());
    } else if (gracePeriod.isPresent()) {
      return jobStart + gracePeriod.get();
    } else if (earliestExpiration.isPresent()) {
      return earliestExpiration.get();
    } else {
      throw UserException.reflectionError()
          .message(
              "Couldn't compute expiration for materialization %s", materialization.getId().getId())
          .build(logger);
    }
  }

  protected Materialization getMaterialization() {
    return materialization;
  }

  protected MaterializationStore getMaterializationStore() {
    return materializationStore;
  }
}
