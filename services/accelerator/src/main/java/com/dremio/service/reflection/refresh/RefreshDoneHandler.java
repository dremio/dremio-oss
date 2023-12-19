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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
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
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Handles a completed job.  Created to handle a single job and then discarded.
 */
public class RefreshDoneHandler {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshDoneHandler.class);

  private final DependencyManager dependencyManager;
  private final MaterializationStore materializationStore;
  private final Supplier<ExpansionHelper> expansionHelper;
  private final Path accelerationBasePath;

  private final ReflectionEntry reflection;
  private final Materialization materialization;
  private final com.dremio.service.job.JobDetails job;
  private final JobsService jobsService;
  private final BufferAllocator allocator;
  private final CatalogService catalogService;
  private final DependencyResolutionContext dependencyResolutionContext;

  public RefreshDoneHandler(
    ReflectionEntry entry,
    Materialization materialization,
    com.dremio.service.job.JobDetails job,
    JobsService jobsService,
    MaterializationStore materializationStore,
    DependencyManager dependencyManager,
    Supplier<ExpansionHelper> expansionHelper,
    Path accelerationBasePath,
    BufferAllocator allocator,
    CatalogService catalogService,
    DependencyResolutionContext dependencyResolutionContext) {
    this.reflection = Preconditions.checkNotNull(entry, "reflection entry required");
    this.materialization = Preconditions.checkNotNull(materialization, "materialization required");
    this.job = Preconditions.checkNotNull(job, "jobDetails required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobsService required");
    this.dependencyManager = Preconditions.checkNotNull(dependencyManager, "dependencies required");
    this.materializationStore = materializationStore;
    this.expansionHelper = Preconditions.checkNotNull(expansionHelper, "expansion helper required");
    this.accelerationBasePath = Preconditions.checkNotNull(accelerationBasePath, "acceleration base path required");
    this.allocator = allocator;
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalogService required");
    this.dependencyResolutionContext = Preconditions.checkNotNull(dependencyResolutionContext);
  }

  /**
   * computes various materialization attributes and stats and saves the materialization in the store
   * @return refresh decision
   * @throws NamespaceException if we fail to access a dataset while updating the dependencies
   * @throws IllegalStateException if the materialization is missing refreshes
   * @throws DependencyException if cyclic dependency detected
   */
  public RefreshDecision handle() throws NamespaceException, DependencyException {
    JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    Preconditions.checkState(lastAttempt.getState() == JobState.COMPLETED,
      "Cannot handle job with non completed state %s", lastAttempt.getState());

    final RefreshDecision decision = getRefreshDecision(lastAttempt);

    final ByteString planBytes = Preconditions.checkNotNull(decision.getLogicalPlan(),
      "refresh jobInfo has no logical plan");

    updateDependencies(reflection, lastAttempt.getInfo(), decision, dependencyManager);

    failIfNotEnoughRefreshesAvailable(decision);

    final JobDetails details = ReflectionUtils.computeJobDetails(lastAttempt);
    final boolean dataWritten = Optional.ofNullable(details.getOutputRecords()).orElse(0L) > 0
      || lastAttempt.getStats().getRemovedFiles() > 0;
    boolean isEmptyReflection = getIsEmptyReflection(decision.getInitialRefresh().booleanValue(), dataWritten, materialization);
    if (dataWritten || isEmptyReflection) {
      createAndSaveRefresh(details, decision, lastAttempt);
    } else {
      logger.debug("materialization {} didn't write any data, we won't create a refresh entry", getId(materialization));
    }


    if (!dataWritten && !decision.getInitialRefresh()) {
      // for incremental refresh, if we don't create a refresh entry we still need to copy the materialization fields
      // from the previous materialization as it will be owning the same refreshes
      Preconditions.checkState(decision.getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL,
        "non initial refresh only allowed for INCREMENTAL refreshes");
      final Materialization lastDone = Preconditions.checkNotNull(
        materializationStore.getLastMaterializationDone(materialization.getReflectionId()),
        "incremental refresh didn't write any data and previous materializations expired");

      materialization.setExpiration(computeExpiration())
        .setInitRefreshExecution(details.getJobStart())
        .setLastRefreshFromPds(lastDone.getLastRefreshFromPds())
        .setLastRefreshFinished(lastDone.getLastRefreshFinished())
        .setLastRefreshDurationMillis(lastDone.getLastRefreshDurationMillis())
        .setLogicalPlan(lastDone.getLogicalPlan())
        .setLogicalPlanStrippedHash(decision.getLogicalPlanStrippedHash())
        .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
        .setSeriesId(decision.getSeriesId())
        .setSeriesOrdinal(lastDone.getSeriesOrdinal())
        .setJoinAnalysis(lastDone.getJoinAnalysis())
        .setPartitionList(lastDone.getPartitionList());
    } else {
      final Optional<Long> oldestDependentMaterialization = dependencyManager.getOldestDependentMaterialization(reflection.getId());
      long currentTime = System.currentTimeMillis();
      long lastRefreshTime = oldestDependentMaterialization.orElse(materialization.getInitRefreshSubmit());
      materialization.setExpiration(computeExpiration())
        .setInitRefreshExecution(details.getJobStart())
        .setLastRefreshFromPds(lastRefreshTime)
        .setLastRefreshFinished(currentTime)
        .setLastRefreshDurationMillis(currentTime - materialization.getInitRefreshSubmit())
        .setLogicalPlan(planBytes)
        .setLogicalPlanStrippedHash(decision.getLogicalPlanStrippedHash())
        .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
        .setSeriesId(decision.getSeriesId())
        .setSeriesOrdinal(dataWritten ? decision.getSeriesOrdinal() : Math.max(decision.getSeriesOrdinal() - 1, 0))
        .setJoinAnalysis(computeJoinAnalysis())
        .setPartitionList(getDataPartitions());
    }

    materializationStore.save(materialization);

    return decision;
  }

  /** Determines if the current reflection has 0 rows and is an Iceberg Reflection
   * We will only allow the initial refresh for Iceberg Materialization to be empty
   * We don't need to handle incremental refreshes, as we can still use the previous refresh in the series
   * We don't want to handle non-Iceberg materialization due to possible path discrepancy
   * @param initialRefresh is it Initial Refresh or later refresh
   * @param dataWritten Was any data written while saving the reflection
   * @param materialization current materialization
   * @return true if it is based on an Empty Iceberg Reflection
   */
  public static boolean getIsEmptyReflection(boolean initialRefresh, boolean dataWritten, Materialization materialization){

  boolean allowEmptyRefresh =  initialRefresh && materialization.getIsIcebergDataset() != null
                                  && materialization.getIsIcebergDataset();
  return  !dataWritten && allowEmptyRefresh;
}
  public RefreshDecision getRefreshDecision(final JobAttempt jobAttempt) {
    if(jobAttempt.getExtraInfoList() == null || jobAttempt.getExtraInfoList().isEmpty()) {
      throw new IllegalStateException("No refresh decision found in refresh job.");
    }

    List<ExtraInfo> extraInfo = jobAttempt.getExtraInfoList().stream()
        .filter(i -> RefreshHandler.DECISION_NAME.equals(i.getName()))
        .collect(Collectors.toList());

    if(extraInfo.size() != 1) {
      throw new IllegalStateException(String.format("Expected to have one refresh decision, saw: %d.", extraInfo.size()));
    }

    return RefreshHandler.ABSTRACT_SERIALIZER.revert(extraInfo.get(0).getData().toByteArray());
  }

  /**
   * throws {@link IllegalStateException} if some refreshes owned by the materialization are missing.
   * This can only happen if the materialization somehow took too long to finish and the previous incremental
   * materialization was deleted along with its refreshes
   */
  private void failIfNotEnoughRefreshesAvailable(final RefreshDecision decision) {
    if (decision.getInitialRefresh()) {
      return;
    }

    final int seriesOrdinal = decision.getSeriesOrdinal();
    // seriesOrdinal is 0-based so we should expect number of refreshes in the store to be equal to the seriesOrdinal
    Iterable<Refresh> refreshes = materializationStore.getRefreshesForSeries(reflection.getId(), decision.getSeriesId());
    final int numRefreshes = Iterables.size(refreshes);
    Preconditions.checkState(numRefreshes == seriesOrdinal,
      "Materialization %s is missing refreshes. Expected %s but only found %s",
      getId(materialization), seriesOrdinal, numRefreshes);
  }

  public void updateDependencies(final ReflectionEntry entry, final JobInfo info, final RefreshDecision decision,
                                 final DependencyManager dependencyManager) throws NamespaceException, DependencyException {
    final ExtractedDependencies dependencies = DependencyUtils.extractDependencies(info, decision, catalogService);
    if (decision.getInitialRefresh()) {
      if (dependencies.isEmpty()) {
        throw UserException.reflectionError()
          .message("Could not find any physical dependencies for reflection %s most likely " +
            "because one of its datasets has a select with options or it's a select from values", entry.getId())
          .build(logger);
      }

      dependencyManager.setDependencies(entry.getId(), dependencies);
    } else if (!dependencies.getPlanDependencies().isEmpty()) {
      // for incremental refresh, only update the dependencies if planDependencies are not empty, otherwise it's most
      // likely an empty incremental refresh
      dependencyManager.setDependencies(entry.getId(), dependencies);
    }
    dependencyManager.updateDontGiveUp(entry, dependencyResolutionContext);
  }

  private void createAndSaveRefresh(final JobDetails details, final RefreshDecision decision,final JobAttempt lastAttempt) {
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
    final MaterializationMetrics metrics = ReflectionUtils.computeMetrics(job, jobsService, allocator, jobId);
    final List<DataPartition> dataPartitions = ReflectionUtils.computeDataPartitions(JobsProtoUtil.getLastAttempt(job).getInfo());
    final AttemptId attemptId = AttemptIdUtils.fromString(lastAttempt.getAttemptId());
    final List<String> refreshPath = RefreshHandler.getRefreshPath(materialization.getReflectionId(),materialization,decision, attemptId);
    final boolean isIcebergRefresh = materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    final String icebergBasePath = ReflectionUtils.getIcebergReflectionBasePath(refreshPath, isIcebergRefresh);
    Preconditions.checkArgument(!isIcebergRefresh || decision.getInitialRefresh() || icebergBasePath.equals(materialization.getBasePath()));
    final Refresh refresh = ReflectionUtils.createRefresh(reflection.getId(), refreshPath, decision.getSeriesId(),
      decision.getSeriesOrdinal(), updateId, details, metrics, dataPartitions, isIcebergRefresh, icebergBasePath);

    logger.trace("Refresh created: {}", refresh);
    materializationStore.save(refresh);

    logger.debug("Refresh written to {} for {}", PathUtils.constructFullPath(refreshPath), ReflectionUtils.getId(materialization));
  }
  private List<DataPartition> getDataPartitions() {
    return ImmutableList.copyOf(materializationStore.getRefreshes(materialization)
      .transformAndConcat(new Function<Refresh, Iterable<DataPartition>>() {
        @Override
        public Iterable<DataPartition> apply(Refresh input) {
          return input.getPartitionList() != null ? input.getPartitionList() : ImmutableList.of();
        }
      }).toSet());
  }

  private JoinAnalysis computeJoinAnalysis() {
    final JobInfo info = JobsProtoUtil.getLastAttempt(job).getInfo();
    if (info.getJoinAnalysis() == null) {
      return null;
    }

    JoinAnalysis joinAnalysis = info.getJoinAnalysis();

    if (info.getAcceleration() != null) {
      for (Substitution sub : selfOrEmpty(info.getAcceleration().getSubstitutionsList())) {
        Materialization usedMaterialization = materializationStore.get(new MaterializationId(sub.getId().getMaterializationId()));
        if (usedMaterialization == null) {
          continue;
        }

        try (ExpansionHelper helper = expansionHelper.get()){
          RelNode usedMaterializationLogicalPlan = MaterializationExpander.deserializePlan(usedMaterialization.getLogicalPlan().toByteArray(), helper.getConverter(), catalogService);
          if (usedMaterialization.getJoinAnalysis() != null) {
            joinAnalysis = JoinAnalyzer.merge(joinAnalysis, usedMaterialization.getJoinAnalysis(), usedMaterializationLogicalPlan, usedMaterialization.getId().getId());
          }
        }
      }
    }

    return joinAnalysis;
  }

  /**
   * @return next updateId
   */
  private static UpdateId getUpdateId(final JobId jobId, final JobsService jobsService, BufferAllocator allocator) {
    final int fetchLimit = 1000;
    UpdateIdWrapper updateIdWrapper = new UpdateIdWrapper();

    int offset = 0;
    while (true) {
      try (final JobDataFragment data = JobDataClientUtils.getJobData(jobsService, allocator, jobId, offset, fetchLimit)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          byte[] b = (byte[]) data.extractValue(RecordWriter.METADATA_COLUMN, i);
          if(b == null) {
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
   * if the query doesn't depend directly on pds the grace period will be set to Long MAX as the expectation is that
   * we will compute a proper expiration time from the dependent reflections
   *
   * @return expiration time
   */
  private long computeExpiration() {
    final ReflectionId reflectionId = materialization.getReflectionId();
    final long jobStart = materialization.getInitRefreshSubmit();
    final Optional<Long> gracePeriod = dependencyManager.getGracePeriod(reflectionId, dependencyResolutionContext);
    final Optional<Long> earliestExpiration = dependencyManager.getEarliestExpiration(reflectionId);

    if (gracePeriod.isPresent() && earliestExpiration.isPresent()) {
      return Math.min(earliestExpiration.get(), jobStart + gracePeriod.get());
    } else if (gracePeriod.isPresent()) {
      return jobStart + gracePeriod.get();
    } else if (earliestExpiration.isPresent()) {
      return earliestExpiration.get();
    } else {
      throw UserException.reflectionError()
        .message("Couldn't compute expiration for materialization %s", materialization.getId().getId())
        .build(logger);
    }
  }
}
