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
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.acceleration.MaterializationExpander;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
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
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.DependencyGraph.DependencyException;
import com.dremio.service.reflection.DependencyManager;
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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Handles successful refresh jobs
 */
public class RefreshDoneHandler {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshDoneHandler.class);

  private final DependencyManager dependencyManager;
  private final NamespaceService namespaceService;
  private final MaterializationStore materializationStore;
  private final Supplier<ExpansionHelper> expansionHelper;
  private final Path accelerationBasePath;

  private final ReflectionEntry reflection;
  private final Materialization materialization;
  private final com.dremio.service.job.JobDetails job;
  private final JobsService jobsService;
  private final BufferAllocator allocator;

  public RefreshDoneHandler(
    ReflectionEntry entry,
    Materialization materialization,
    com.dremio.service.job.JobDetails job,
    JobsService jobsService,
    NamespaceService namespaceService,
    MaterializationStore materializationStore,
    DependencyManager dependencyManager,
    Supplier<ExpansionHelper> expansionHelper,
    Path accelerationBasePath,
    BufferAllocator allocator) {
    this.reflection = Preconditions.checkNotNull(entry, "reflection entry required");
    this.materialization = Preconditions.checkNotNull(materialization, "materialization required");
    this.job = Preconditions.checkNotNull(job, "jobDetails required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobsService required");
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.dependencyManager = Preconditions.checkNotNull(dependencyManager, "dependencies required");
    this.materializationStore = materializationStore;
    this.expansionHelper = Preconditions.checkNotNull(expansionHelper, "expansion helper required");
    this.accelerationBasePath = Preconditions.checkNotNull(accelerationBasePath, "acceleration base path required");
    this.allocator = allocator;
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

    updateDependencies(reflection.getId(), lastAttempt.getInfo(), decision, namespaceService, dependencyManager);

    failIfNotEnoughRefreshesAvailable(decision);

    final JobDetails details = ReflectionUtils.computeJobDetails(lastAttempt);
    final boolean dataWritten = Optional.fromNullable(details.getOutputRecords()).or(0L) > 0;
    if (dataWritten) {
      createAndSaveRefresh(details, decision);
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
        .setLogicalPlan(lastDone.getLogicalPlan())
        .setLogicalPlanStrippedHash(decision.getLogicalPlanStrippedHash())
        .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
        .setSeriesId(decision.getSeriesId())
        .setSeriesOrdinal(lastDone.getSeriesOrdinal())
        .setJoinAnalysis(lastDone.getJoinAnalysis())
        .setPartitionList(lastDone.getPartitionList());
    } else {
      final Optional<Long> oldestDependentMaterialization = dependencyManager.getOldestDependentMaterialization(reflection.getId());
      materialization.setExpiration(computeExpiration())
        .setInitRefreshExecution(details.getJobStart())
        .setLastRefreshFromPds(oldestDependentMaterialization.or(materialization.getInitRefreshSubmit()))
        .setLogicalPlan(planBytes)
        .setLogicalPlanStrippedHash(decision.getLogicalPlanStrippedHash())
        .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION)
        .setSeriesId(decision.getSeriesId())
        .setSeriesOrdinal(dataWritten ? decision.getSeriesOrdinal() : decision.getSeriesOrdinal() - 1)
        .setJoinAnalysis(computeJoinAnalysis())
        .setPartitionList(getDataPartitions());
    }

    materializationStore.save(materialization);

    return decision;
  }

  public static RefreshDecision getRefreshDecision(final JobAttempt jobAttempt) {
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

  public static void updateDependencies(final ReflectionId id, final JobInfo info, final RefreshDecision decision,
      final NamespaceService namespaceService, final DependencyManager dependencyManager) throws NamespaceException, DependencyException {
    final ExtractedDependencies dependencies = DependencyUtils.extractDependencies(namespaceService, info, decision);
    if (decision.getInitialRefresh()) {
      if (dependencies.isEmpty()) {
        throw UserException.reflectionError()
          .message("Could not find any physical dependencies for reflection %s most likely " +
            "because one of its datasets has a select with options or it's a select from values", id.getId())
          .build(logger);
      }

      dependencyManager.setDependencies(id, dependencies);
    } else if (!dependencies.getPlanDependencies().isEmpty()) {
      // for incremental refresh, only update the dependencies if planDependencies are not empty, otherwise it's most
      // likely an empty incremental refresh
      dependencyManager.setDependencies(id, dependencies);
    }
  }

  private void createAndSaveRefresh(final JobDetails details, final RefreshDecision decision) {
    final JobId jobId = JobsProtoUtil.toStuff(job.getJobId());
    final boolean isFull = decision.getAccelerationSettings().getMethod() == RefreshMethod.FULL;
    final UpdateId updateId = isFull ? new UpdateId() : getUpdateId(jobId, jobsService, allocator);
    final MaterializationMetrics metrics = ReflectionUtils.computeMetrics(job, jobsService, allocator, jobId);
    final List<DataPartition> dataPartitions = ReflectionUtils.computeDataPartitions(JobsProtoUtil.getLastAttempt(job).getInfo());
    final List<String> refreshPath = ReflectionUtils.getRefreshPath(jobId, accelerationBasePath, jobsService, allocator);
    final boolean isIcebergRefresh = materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    final String icebergBasePath = ReflectionUtils.getIcebergReflectionBasePath(materialization, refreshPath, isIcebergRefresh);
    final Refresh refresh = ReflectionUtils.createRefresh(reflection.getId(), refreshPath, decision.getSeriesId(),
      decision.getSeriesOrdinal(), updateId, details, metrics, dataPartitions, isIcebergRefresh, icebergBasePath);

    logger.trace("Refresh created: {}", refresh);
    materializationStore.save(refresh);

    logger.debug("materialization {} was written to {}", ReflectionUtils.getId(materialization), PathUtils.constructFullPath(refreshPath));
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
          RelNode usedMaterializationLogicalPlan = MaterializationExpander.deserializePlan(usedMaterialization.getLogicalPlan().toByteArray(), helper.getConverter());
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
    final Optional<Long> gracePeriod = dependencyManager.getGracePeriod(reflectionId);
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
