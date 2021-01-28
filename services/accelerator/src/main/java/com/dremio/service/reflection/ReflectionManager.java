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

import static com.dremio.common.utils.PathUtils.constructFullPath;
import static com.dremio.exec.ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS;
import static com.dremio.service.reflection.ReflectionOptions.COMPACTION_TRIGGER_FILE_SIZE;
import static com.dremio.service.reflection.ReflectionOptions.COMPACTION_TRIGGER_NUMBER_FILES;
import static com.dremio.service.reflection.ReflectionOptions.ENABLE_COMPACTION;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_NUM_ENTRIES;
import static com.dremio.service.reflection.ReflectionUtils.computeDataPartitions;
import static com.dremio.service.reflection.ReflectionUtils.getId;
import static com.dremio.service.reflection.ReflectionUtils.getMaterializationPath;
import static com.dremio.service.reflection.ReflectionUtils.submitRefreshJob;
import static com.dremio.service.reflection.proto.ReflectionState.ACTIVE;
import static com.dremio.service.reflection.proto.ReflectionState.COMPACTING;
import static com.dremio.service.reflection.proto.ReflectionState.DEPRECATE;
import static com.dremio.service.reflection.proto.ReflectionState.FAILED;
import static com.dremio.service.reflection.proto.ReflectionState.METADATA_REFRESH;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESH;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESHING;
import static com.dremio.service.reflection.proto.ReflectionState.UPDATE;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.util.DremioEdition;
import com.dremio.datastore.WarningTimer;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.MaterializationSettings;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.SubstitutionSettings;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.ReflectionServiceImpl.DescriptorCache;
import com.dremio.service.reflection.ReflectionServiceImpl.ExpansionHelper;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Failure;
import com.dremio.service.reflection.proto.JobDetails;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.RefreshDoneHandler;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Manages reflections, excluding external reflections, by observing changes to the reflection goals, datasets, materialization
 * jobs and executing the appropriate handling logic sequentially.
 */
public class ReflectionManager implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionManager.class);
  private static final long START_WAIT_MILLIS = 5*60*1000;

  /**
   * Callback that allows async handlers to wake up the manager once they are done.
   */
  public interface WakeUpCallback {
    void wakeup(String reason);
  }

  /**
   * when the manager wakes up, it looks at all reflection goals that have been added/modified since the last wakeup.
   * this assumes that entries saved to the kvStore will instantaneously be available, but in practice there will always
   * be a slight delay.
   * this constant defines protects against skipping those entries.
   */
  private static final long WAKEUP_OVERLAP_MS = 60_000;

  private final SabotContext sabotContext;
  private final JobsService jobsService;
  private final NamespaceService namespaceService;
  private final OptionManager optionManager;
  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore reflectionStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final MaterializationStore materializationStore;
  private final DependencyManager dependencyManager;
  private final DescriptorCache descriptorCache;
  private final Set<ReflectionId> reflectionsToUpdate;
  private final WakeUpCallback wakeUpCallback;
  private final Supplier<ExpansionHelper> expansionHelper;
  private volatile Path accelerationBasePath;
  private final BufferAllocator allocator;
  private final ReflectionGoalChecker reflectionGoalChecker;
  private RefreshStartHandler refreshStartHandler;

  private volatile EntryCounts lastStats = new EntryCounts();
  private long lastWakeupTime;

  ReflectionManager(SabotContext sabotContext, JobsService jobsService, NamespaceService namespaceService,
                    OptionManager optionManager, ReflectionGoalsStore userStore, ReflectionEntriesStore reflectionStore,
                    ExternalReflectionStore externalReflectionStore, MaterializationStore materializationStore,
                    DependencyManager dependencyManager, DescriptorCache descriptorCache,
                    Set<ReflectionId> reflectionsToUpdate, WakeUpCallback wakeUpCallback,
                    Supplier<ExpansionHelper> expansionHelper, BufferAllocator allocator, Path accelerationBasePath,
                    ReflectionGoalChecker reflectionGoalChecker, RefreshStartHandler refreshStartHandler) {
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "sabotContext required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobsService required");
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespaceService required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "optionManager required");
    this.userStore = Preconditions.checkNotNull(userStore, "reflection user store required");
    this.reflectionStore = Preconditions.checkNotNull(reflectionStore, "reflection store required");
    this.externalReflectionStore = Preconditions.checkNotNull(externalReflectionStore);
    this.materializationStore = Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.dependencyManager = Preconditions.checkNotNull(dependencyManager, "dependency manager required");
    this.descriptorCache = Preconditions.checkNotNull(descriptorCache, "descriptor cache required");
    this.reflectionsToUpdate = Preconditions.checkNotNull(reflectionsToUpdate, "reflections to update required");
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
    this.expansionHelper = Preconditions.checkNotNull(expansionHelper, "sqlConvertSupplier required");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator required");
    this.accelerationBasePath = Preconditions.checkNotNull(accelerationBasePath);
    this.reflectionGoalChecker = Preconditions.checkNotNull(reflectionGoalChecker);
    this.refreshStartHandler = Preconditions.checkNotNull(refreshStartHandler);
    Metrics.newGauge(Metrics.join("reflections", "unknown"), () -> ReflectionManager.this.lastStats.unknown);
    Metrics.newGauge(Metrics.join("reflections", "failed"), () -> ReflectionManager.this.lastStats.failed);
    Metrics.newGauge(Metrics.join("reflections", "active"), () -> ReflectionManager.this.lastStats.active);
    Metrics.newGauge(Metrics.join("reflections", "refreshing"), () -> ReflectionManager.this.lastStats.refreshing);
  }

  @Override
  public void run() {
    try (WarningTimer timer = new WarningTimer("Reflection Manager", TimeUnit.SECONDS.toMillis(5))) {
      logger.trace("running the reflection manager");
      try {
        sync();
      } catch (Throwable e) {
        logger.error("Reflection manager failed", e);
      }
    }
  }

  @VisibleForTesting
  void sync(){
    long lastWakeupTime = System.currentTimeMillis();
    final long previousLastWakeupTime = lastWakeupTime - WAKEUP_OVERLAP_MS;
    // updating the store's lastWakeupTime here. This ensures that if we're failing we don't do a denial of service attack
    // this assumes we properly handle exceptions for each goal/entry independently and we don't exit the loop before we
    // go through all entities otherwise we may "skip" handling some entities in case of failures
    final long deletionGracePeriod = optionManager.getOption(REFLECTION_DELETION_GRACE_PERIOD) * 1000;
    final long deletionThreshold = System.currentTimeMillis() - deletionGracePeriod;
    final int numEntriesToDelete = (int) optionManager.getOption(REFLECTION_DELETION_NUM_ENTRIES);

    handleReflectionsToUpdate();
    handleDeletedDatasets();
    handleGoals(previousLastWakeupTime);
    handleEntries();
    deleteDeprecatedMaterializations(deletionThreshold, numEntriesToDelete);
    deprecateMaterializations();
    deleteDeprecatedGoals(deletionThreshold);
    this.lastWakeupTime = lastWakeupTime;
  }

  /**
   * handle all reflections marked by the reflection service as need to update.<br>
   * those are reflections with plans that couldn't be expended and thus need to be set in UPDATE state
   */
  private void handleReflectionsToUpdate() {
    final Iterator<ReflectionId> iterator = reflectionsToUpdate.iterator();
    while (iterator.hasNext()) {
      final ReflectionId rId = iterator.next();
      try {
        final ReflectionEntry entry = reflectionStore.get(rId);
        if (entry != null) {
          cancelRefreshJobIfAny(entry);
          entry.setState(UPDATE);
          reflectionStore.save(entry);
        }
      } finally {
        // block should never throw, but in case it does we don't want be stuck trying to update the same entry
        iterator.remove();
      }
    }
  }

  /**
   * 4th pass: remove any deleted goal that's due
   *
   * @param deletionThreshold thrshold after which deprecated reflection goals are deleted
   */
  private void deleteDeprecatedGoals(long deletionThreshold) {
    Iterable<ReflectionGoal> goalsDueForDeletion = userStore.getDeletedBefore(deletionThreshold);
    for (ReflectionGoal goal : goalsDueForDeletion) {
      logger.debug("reflection goal {} due for deletion", goal.getId().getId());
      userStore.delete(goal.getId());
    }
  }

  private void deprecateMaterializations() {
    final long now = System.currentTimeMillis();
    Iterable<Materialization> materializations = materializationStore.getAllExpiredWhen(now);
    for (Materialization materialization : materializations) {
      try {
        deprecateMaterialization(materialization);
      } catch (Exception e) {
        logger.warn("Couldn't deprecate materialization {}", getId(materialization));
      }
    }
  }

  /**
   * 3rd pass: go through the materialization store
   *
   * @param deletionThreshold threshold time after which deprecated materialization are deleted
   * @param numEntries number of entries that should be deleted now
   */
  private void deleteDeprecatedMaterializations(long deletionThreshold, int numEntries) {
    Iterable<Materialization> materializations = materializationStore.getDeletableEntriesModifiedBefore(deletionThreshold, numEntries);
    for (Materialization materialization : materializations) {
      logger.debug("deprecated materialization {} due for deletion", getId(materialization));
      try {
        deleteMaterialization(materialization);
      } catch (Exception e) {
        logger.warn("Couldn't delete deprecated materialization {}", getId(materialization));
      }
    }
  }

  /**
   * 2nd pass: go through the reflection store
   */
  private void handleEntries() {
    final long noDependencyRefreshPeriodMs = optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS) * 1000;

    Iterable<ReflectionEntry> entries = reflectionStore.find();
    final EntryCounts ec = new EntryCounts();
    for (ReflectionEntry entry : entries) {
      try {
        handleEntry(entry, noDependencyRefreshPeriodMs, ec);
      } catch (Exception e) {
        ec.unknown++;
        logger.error("Couldn't handle reflection entry {}", entry.getId().getId(), e);
        reportFailure(entry, entry.getState());
      }
    }
    this.lastStats = ec;
  }

  private void handleDeletedDatasets() {
    Iterable<ReflectionGoal> goals = userStore.getAllNotDeleted();
    for (ReflectionGoal goal : goals) {
      handleDatasetDeletion(goal.getDatasetId(), goal.getId());
    }

    Iterable<ExternalReflection> externalReflections = externalReflectionStore.getExternalReflections();
    for (ExternalReflection externalReflection : externalReflections) {
      handleDatasetDeletionForExternalReflection(externalReflection);
    }
  }

  /**
   * Small class that stores the results of reflection entry review
   */
  private final class EntryCounts {
    private long failed;
    private long refreshing;
    private long active;
    private long unknown;
  }

  private void handleEntry(ReflectionEntry entry, final long noDependencyRefreshPeriodMs, EntryCounts counts) {
    final ReflectionState state = entry.getState();
    switch (state) {
      case FAILED:
        counts.failed++;
        // do nothing
        //TODO filter out those when querying the reflection store
        break;
      case REFRESHING:
      case METADATA_REFRESH:
      case COMPACTING:
        counts.refreshing++;
        handleRefreshingEntry(entry);
        break;
      case UPDATE:
        counts.refreshing++;
        deprecateMaterializations(entry);
        startRefresh(entry);
        break;
      case ACTIVE:
        if (!dependencyManager.shouldRefresh(entry, noDependencyRefreshPeriodMs)) {
          counts.active++;
          // only refresh ACTIVE reflections when they are due for refresh
          break;
        }
      case REFRESH:
        counts.refreshing++;
        logger.info("reflection {} is due for refresh", getId(entry));
        startRefresh(entry);
        break;
      case DEPRECATE:
        deprecateMaterializations(entry);
        deleteReflection(entry);
        break;
      default:
        throw new IllegalStateException("Unsupported reflection state " + state);
    }
  }

  /**
   * handles entry in REFRESHING/METADATA_REFRESH state
   */

  private void handleRefreshingEntry(final ReflectionEntry entry) {
    // handle job completion
    final Materialization m = Preconditions.checkNotNull(materializationStore.getLastMaterialization(entry.getId()),
      "Reflection in refreshing state has no materialization entries", entry.getId());
    if (m.getState() != MaterializationState.RUNNING) {
      // Reflection in refreshing state should have a materialization in RUNNING state but if somehow we end up
      // in this weird state where the materialization store has an entry not in RUNNING state, we need to cleanup that entry.
      try {
        deleteMaterialization(m);
        deleteReflection(entry);
        descriptorCache.invalidate(m.getId());
      } catch (Exception e) {
        logger.warn("Couldn't clean up {} materialization {} during refresh", m.getState(), getId(m));
      }
      return;
    }

    com.dremio.service.job.JobDetails job;
    try {
      JobDetailsRequest request = JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(entry.getRefreshJobId()))
        .setUserName(SYSTEM_USERNAME)
        .setFromStore(true)
        .build();
      job = jobsService.getJobDetails(request);

    } catch (JobNotFoundException e) {
      // something's wrong, a refreshing entry means we already submitted a job and we should be able to retrieve it.
      // let's handle this as a failure to avoid hitting an infinite loop trying to handle this reflection entry
      m.setState(MaterializationState.FAILED)
        .setFailure(new Failure().setMessage(String.format("Couldn't retrieve refresh job %s", entry.getRefreshJobId().getId())));
      materializationStore.save(m);
      reportFailure(entry, ACTIVE);
      return;
    }

    if (!job.getCompleted()) {
      // job not done yet
      return;
    }
    JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    switch (lastAttempt.getState()) {
      case COMPLETED:
        try {
          logger.debug("job {} for materialization {} completed successfully", job.getJobId().getId(), getId(m));
          handleSuccessfulJob(entry, m, job);
        } catch (Exception e){
          m.setState(MaterializationState.FAILED)
            .setFailure(new Failure().setMessage("Unexpected error occurred during job " + job.getJobId().getId()));
          materializationStore.save(m);
          reportFailure(entry, ACTIVE);
        }
        break;
      case CANCELED:
        logger.debug("job {} for materialization {} was cancelled", job.getJobId().getId(), getId(m));
        if (entry.getState() == REFRESHING) {
          // try to update the dependencies even when a refreshing job fails
          updateDependenciesIfPossible(entry, lastAttempt);
        }
        m.setState(MaterializationState.CANCELED);
        materializationStore.save(m);
        entry.setState(ACTIVE);
        reflectionStore.save(entry);
        break;
      case FAILED:
        logger.debug("job {} for materialization {} failed", job.getJobId().getId(), getId(m));
        if (entry.getState() == REFRESHING) {
          // try to update the dependencies even when a refreshing job fails
          updateDependenciesIfPossible(entry, lastAttempt);
        }
        final String jobFailure = Optional.fromNullable(lastAttempt.getInfo().getFailureInfo())
          .or("Reflection Job failed without reporting an error message");
        m.setState(MaterializationState.FAILED)
          .setFailure(new Failure().setMessage(jobFailure));
        materializationStore.save(m);
        reportFailure(entry, ACTIVE);
        break;
      default:
        // nothing to do for non terminal states
        break;
    }
  }

  private void updateDependenciesIfPossible(final ReflectionEntry entry, final JobAttempt jobAttempt) {
    if (dependencyManager.reflectionHasKnownDependencies(entry.getId())) {
      return;
    }

    try {
      final RefreshDecision decision = RefreshDoneHandler.getRefreshDecision(jobAttempt);
      RefreshDoneHandler.updateDependencies(entry.getId(), jobAttempt.getInfo(), decision, namespaceService, dependencyManager);
    } catch (Exception | AssertionError e) {
      logger.warn("Couldn't retrieve any dependency for reflection {}", getId(entry), e);
    }
  }

  /**
   * 1st pass: observe changes in the reflection user store
   * find all goals that were created or modified since last wakeup
   * for each identified description
   * if it doesn't have a corresponding reflection it's a NEW one
   * if it does and the version has changed it's an UPDATE
   * if it has a DELETED state, it's...well guess ;)
   *
   * @param lastWakeupTime previous wakeup time
   */
  private void handleGoals(long lastWakeupTime) {
    Iterable<ReflectionGoal> goals = userStore.getModifiedOrCreatedSince(lastWakeupTime);
    for (ReflectionGoal goal : goals) {
      try {
        handleGoal(goal);
      } catch (Exception e) {
        logger.error("Couldn't handle reflection goal {}", goal.getId().getId(), e);
      }
    }
  }

  private void handleDatasetDeletion(String datasetId, ReflectionId rId) {
    // make sure the corresponding dataset was not deleted
    if (namespaceService.findDatasetByUUID(datasetId) == null) {
      // dataset not found, mark goal as deleted
      logger.debug("dataset deleted for reflection {}", rId.getId());

      final ReflectionGoal goal = userStore.get(rId);
      if (goal != null) {
        try {
          userStore.save(goal.setState(ReflectionGoalState.DELETED));
          return;
        } catch (ConcurrentModificationException cme) {
          // someone's changed the reflection goal, we'll delete it next time the manager wakes up
          logger.debug("concurrent modification when trying mark reflection goal {} as deleted", rId.getId());
        }
      }

      // something wrong here
      throw new IllegalStateException("no reflection found for an existing reflection entry: " + rId.getId());
    }
  }

  private void handleDatasetDeletionForExternalReflection(ExternalReflection externalReflection) {
    if (namespaceService.findDatasetByUUID(externalReflection.getQueryDatasetId()) == null
      || namespaceService.findDatasetByUUID(externalReflection.getTargetDatasetId()) == null) {
      externalReflectionStore.deleteExternalReflection(externalReflection.getId());
    }
  }

  @VisibleForTesting
  void handleGoal(ReflectionGoal goal) {
    final ReflectionEntry entry = reflectionStore.get(goal.getId());
    if (entry == null) {
      // no corresponding reflection, goal has been created or enabled
      if (goal.getState() == ReflectionGoalState.ENABLED) { // we still need to make sure user didn't create a disabled goal
        reflectionStore.save(create(goal));
      }
    } else if (reflectionGoalChecker.isEqual(goal, entry)) {
      return; //no changes, do nothing
    } else if(reflectionGoalChecker.checkHash(goal, entry)){
      // Check if entries need to update meta data that is not used in the materialization
      updateThatHasChangedEntry(goal, entry);

      for (Materialization materialization : materializationStore.find(entry.getId())) {
        if (!Objects.equals(materialization.getArrowCachingEnabled(), goal.getArrowCachingEnabled())) {
          materializationStore.save(
            materialization
              .setArrowCachingEnabled(goal.getArrowCachingEnabled())
              .setReflectionGoalVersion(goal.getTag())
          );
        }
      }
    } else {
      // descriptor changed
      logger.debug("reflection goal {} updated. state {} -> {}", getId(goal), entry.getState(), goal.getState());
      cancelRefreshJobIfAny(entry);
      final boolean enabled = goal.getState() == ReflectionGoalState.ENABLED;
      entry.setState(enabled ? UPDATE : DEPRECATE)
        .setArrowCachingEnabled(goal.getArrowCachingEnabled())
        .setGoalVersion(goal.getTag())
        .setName(goal.getName())
        .setReflectionGoalHash(reflectionGoalChecker.calculateReflectionGoalVersion(goal));
      reflectionStore.save(entry);
    }
  }

  private void updateThatHasChangedEntry(ReflectionGoal reflectionGoal, ReflectionEntry reflectionEntry) {
    boolean shouldBeUnstuck =
      reflectionGoal.getState() == ReflectionGoalState.ENABLED && reflectionEntry.getState() == FAILED;

    reflectionStore.save(
      reflectionEntry
        .setArrowCachingEnabled(reflectionGoal.getArrowCachingEnabled())
        .setGoalVersion(reflectionGoal.getTag())
        .setName(reflectionGoal.getName())
        .setState(shouldBeUnstuck ? UPDATE : reflectionEntry.getState())
        .setNumFailures(shouldBeUnstuck ? 0 : reflectionEntry.getNumFailures())
    );
  }

  private void deleteReflection(ReflectionEntry entry) {
    logger.debug("deleting reflection {}", getId(entry));
    reflectionStore.delete(entry.getId());
    dependencyManager.delete(entry.getId());
  }

  private void deleteMaterialization(Materialization materialization) {
    if (Iterables.isEmpty(materializationStore.getRefreshesExclusivelyOwnedBy(materialization))) {
      logger.debug("materialization {} doesn't own any refresh, entry will be deleted without running a drop table", getId(materialization));
      materializationStore.delete(materialization.getId());
      return;
    }

    // set the materialization to DELETED so we don't try to delete it again
    materialization.setState(MaterializationState.DELETED);
    materializationStore.save(materialization);

    try {
      final String pathString = constructFullPath(getMaterializationPath(materialization));
      final String query = String.format("DROP TABLE IF EXISTS %s", pathString);

      JobProtobuf.MaterializationSummary materializationSummary = JobProtobuf.MaterializationSummary.newBuilder()
        .setReflectionId(materialization.getReflectionId().getId())
        .setLayoutVersion(materialization.getReflectionGoalVersion())
        .setMaterializationId(materialization.getId().getId())
        .build();
      jobsService.submitJob(
        SubmitJobRequest.newBuilder()
          .setMaterializationSettings(MaterializationSettings.newBuilder()
            .setMaterializationSummary(materializationSummary)
            .setSubstitutionSettings(SubstitutionSettings.newBuilder().addAllExclusions(ImmutableList.of()).build())
            .build())
          .setSqlQuery(SqlQuery.newBuilder()
            .setSql(query)
            .addAllContext(Collections.<String>emptyList())
            .setUsername(SYSTEM_USERNAME)
            .build())
          .setQueryType(QueryType.ACCELERATOR_DROP)
          .build(),
        JobStatusListener.NO_OP);
    } catch (Exception e) {
      logger.warn("failed to drop materialization {}", materialization.getId().getId(), e);
    }

  }

  private void deprecateMaterializations(ReflectionEntry entry) {
    // mark all materializations for the reflection as DEPRECATED
    // we only care about DONE materializations
    Iterable<Materialization> materializations = materializationStore.getAllDone(entry.getId());
    for (Materialization materialization : materializations) {
      deprecateMaterialization(materialization);
    }
  }

  private void deprecateMaterialization(Materialization materialization) {
    logger.debug("deprecating materialization {}/{}",
      materialization.getReflectionId().getId(), materialization.getId().getId());
    materialization.setState(MaterializationState.DEPRECATED);
    materializationStore.save(materialization);
    descriptorCache.invalidate(materialization.getId());
  }

  private void deprecateLastMaterialization(Materialization materialization) {
    Materialization lastDone = materializationStore.getLastMaterializationDone(materialization.getReflectionId());
    if (lastDone != null) {
      deprecateMaterialization(lastDone);
    }
  }

  private void handleMaterializationDone(ReflectionEntry entry, Materialization materialization) {
    final long now = System.currentTimeMillis();
    final long materializationExpiry = Optional.fromNullable(materialization.getExpiration()).or(0L);
    if (materializationExpiry <= now) {
      materialization.setState(MaterializationState.FAILED)
        .setFailure(new Failure().setMessage("Successful materialization already expired"));
      logger.warn("Successful materialization {} already expired", ReflectionUtils.getId(materialization));
    } else {
      deprecateLastMaterialization(materialization);
      materialization.setState(MaterializationState.DONE);
      entry.setState(ACTIVE)
        .setLastSuccessfulRefresh(System.currentTimeMillis())
        .setNumFailures(0);
    }
  }

  private void cancelRefreshJobIfAny(ReflectionEntry entry) {
    if (entry.getState() != REFRESHING && entry.getState() != METADATA_REFRESH) {
      return;
    }

    final Materialization m = Preconditions.checkNotNull(materializationStore.getLastMaterialization(entry.getId()),
      "reflection entry %s is in REFRESHING|METADATA_REFRESH state but has no materialization entry", entry.getId());

    try {
      logger.debug("cancelling materialization job {} for reflection {}", entry.getRefreshJobId().getId(), getId(entry));
      // even though the following method can block if the job's foreman is on a different node, it's not a problem here
      // as we always submit reflection jobs on the same node as the manager
      jobsService.cancel(CancelJobRequest.newBuilder()
          .setUsername(SYSTEM_USERNAME)
          .setJobId(JobsProtoUtil.toBuf(entry.getRefreshJobId()))
          .setReason("Query cancelled by Reflection Manager. Reflection configuration is stale")
          .build());
    } catch (JobException e) {
      logger.warn("Failed to cancel refresh job updated reflection {}", getId(entry), e);
    }

    // mark the materialization as cancelled
    m.setState(MaterializationState.CANCELED);
    materializationStore.save(m);

    // we don't need to handle the job, if it did complete and wrote some data, they will eventually get deleted
    // when the materialization entry is deleted
  }

  void setAccelerationBasePath(Path path) {
    if (path.equals(accelerationBasePath)) {
      return;
    }
    Iterable<ReflectionEntry> entries = reflectionStore.find();
    // if there are already reflections don't update the path if the input and current path is different.
    if (Iterables.size(entries) > 0) {
      logger.warn("Failed to set acceleration base path as there are reflections present. Input path {} existing path {}",
        path, accelerationBasePath);
      return;
    }
    this.accelerationBasePath = path;
  }

  @VisibleForTesting
  void handleSuccessfulJob(ReflectionEntry entry, Materialization materialization, com.dremio.service.job.JobDetails job) {
    switch (entry.getState()) {
      case REFRESHING:
        refreshingJobSucceded(entry, materialization, job);
        break;
      case COMPACTING:
        compactionJobSucceeded(entry, materialization, job);
        break;
      case METADATA_REFRESH:
        metadataRefreshJobSucceeded(entry, materialization);
        break;
      default:
        throw new IllegalStateException("Unexpected state " + entry.getState());
    }
  }

  private void refreshingJobSucceded(ReflectionEntry entry, Materialization materialization, com.dremio.service.job.JobDetails job) {

    try {
      final RefreshDoneHandler handler = new RefreshDoneHandler(entry, materialization, job, jobsService,
        namespaceService, materializationStore, dependencyManager, expansionHelper, accelerationBasePath, allocator);
      final RefreshDecision decision = handler.handle();

      // no need to set the following attributes if we fail to handle the refresh
      // one could argue that we should still try to compute entry.dontGiveUp() if we were able to extract the dependencies
      // but if we really fail to handle a successful refresh job for 3 times in a row, the entry is in a bad state
      entry.setRefreshMethod(decision.getAccelerationSettings().getMethod())
        .setRefreshField(decision.getAccelerationSettings().getRefreshField())
        .setDontGiveUp(dependencyManager.dontGiveUp(entry.getId()));
      if (!optionManager.getOption(ReflectionOptions.STRICT_INCREMENTAL_REFRESH)) {
        entry.setShallowDatasetHash(decision.getDatasetHash());
      } else {
        entry.setDatasetHash(decision.getDatasetHash());
      }
    } catch (Exception | AssertionError e) {
      logger.warn("failed to handle reflection {} job done", getId(entry), e);
      materialization.setState(MaterializationState.FAILED)
        .setFailure(new Failure().setMessage("Failed to handle successful refresh job " + job.getJobId().getId()));
    }

    // update the namespace metadata before saving information to the reflection store to avoid concurrent updates.
    if (materialization.getState() != MaterializationState.FAILED) {

      // expiration may not be set if materialization has failed
      final long materializationExpiry = Optional.fromNullable(materialization.getExpiration()).or(0L);
      if (materializationExpiry <= System.currentTimeMillis()) {
        materialization.setState(MaterializationState.FAILED)
          .setFailure(new Failure().setMessage("Successful materialization already expired"));
        logger.warn("Successful materialization {} already expired", ReflectionUtils.getId(materialization));
      } else {
        // even if the materialization didn't write any data it may still own refreshes if it's a non-initial incremental
        // otherwise we don't want to refresh an empty table as it will just fail
        final List<Refresh> refreshes = materializationStore.getRefreshes(materialization).toList();
        if (!refreshes.isEmpty()) {
          try {
            Preconditions.checkState(materialization.getState() != MaterializationState.FAILED, "failed materialization");
            if (!compactIfNecessary(entry, materialization, refreshes)) {
              refreshMetadata(entry, materialization);
            }
          } catch (Exception | AssertionError e) {
            logger.warn("failed to start a LOAD MATERIALIZATION job for {}", getId(materialization), e);
            materialization.setState(MaterializationState.FAILED)
              .setFailure(new Failure().setMessage("Failed to start a LOAD MATERIALIZATION job"));
          }
        } else {
          handleMaterializationDone(entry, materialization);
        }
      }
    }

    if (materialization.getState() == MaterializationState.FAILED) {
      reportFailure(entry, ACTIVE);
    }

    materializationStore.save(materialization);
    reflectionStore.save(entry);
  }

  private boolean compactIfNecessary(ReflectionEntry entry, Materialization materialization, List<Refresh> refreshes) {
    if (!optionManager.getOption(ENABLE_COMPACTION)) {
      return false; // compaction disabled by user
    }
    if (entry.getRefreshMethod() != RefreshMethod.FULL) {
      logger.debug("Skipping compaction check for reflection {} with incremental refresh", getId(entry));
      return false; // only applies to full refresh
    }

    // full refresh materializations must contain a single refresh entry
    Preconditions.checkState(refreshes.size() == 1, "expected 1 refresh entry found %s", refreshes.size());
    final Refresh refresh = refreshes.get(0);

    if (!shouldCompact(entry, refresh)) {
      return false;
    }

    final ReflectionGoal goal = Preconditions.checkNotNull(userStore.get(entry.getId()),
      "Couldn't find associated reflection goal to reflection %s", getId(entry));
    final List<ReflectionField> partitionFields = goal.getDetails().getPartitionFieldList();
    if (partitionFields != null && !partitionFields.isEmpty()) {
      //TODO warn user through UI
      logger.warn("Cannot compact materialization {} as it contains partition columns", getId(materialization));
      return false;
    }

    // mark current materialization as COMPACTED (terminal state)
    materialization
      .setState(MaterializationState.COMPACTED);
    materializationStore.save(materialization);

    logger.debug("Compacting reflection {}", getId(entry));

    // create a new materialization entry for the compacted data
    final Materialization newMaterialization = new Materialization()
      .setId(new MaterializationId(UUID.randomUUID().toString()))
      .setReflectionId(entry.getId())
      .setState(MaterializationState.RUNNING)
      .setExpiration(materialization.getExpiration())
      .setLastRefreshFromPds(materialization.getLastRefreshFromPds())
      .setLogicalPlan(materialization.getLogicalPlan())
      .setReflectionGoalVersion(materialization.getReflectionGoalVersion())
      .setJoinAnalysis(materialization.getJoinAnalysis())
      .setInitRefreshSubmit(System.currentTimeMillis()) // needed to properly return this materialization as last one
      .setInitRefreshExecution(materialization.getInitRefreshExecution())
      .setInitRefreshJobId(materialization.getInitRefreshJobId());
    materializationStore.save(newMaterialization);

    // start compaction job
    final String sql = String.format("COMPACT MATERIALIZATION \"%s\".\"%s\" AS '%s'", entry.getId().getId(), materialization.getId().getId(), newMaterialization.getId().getId());

    final JobId compactionJobId = submitRefreshJob(jobsService, namespaceService, entry, materialization, sql,
      new WakeUpManagerWhenJobDone(wakeUpCallback, "compaction job done"));

    newMaterialization
      .setInitRefreshJobId(compactionJobId.getId());
    materializationStore.save(newMaterialization);

    entry.setState(COMPACTING)
      .setRefreshJobId(compactionJobId);
    reflectionStore.save(entry);

    logger.debug("started job {} to compact materialization {} as {}", compactionJobId.getId(), getId(materialization), newMaterialization.getId().getId());
    return true;
  }

  private void compactionJobSucceeded(ReflectionEntry entry, Materialization materialization, com.dremio.service.job.JobDetails job) {
    // update materialization seriesId/seriesOrdinal to point to the new refresh
    final long seriesId = System.currentTimeMillis();
    materialization.setSeriesId(seriesId)
      .setSeriesOrdinal(0);

    // create new refresh entry that points to the compacted data
    final JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    final JobInfo jobInfo = lastAttempt.getInfo();
    final JobDetails jobDetails = ReflectionUtils.computeJobDetails(lastAttempt);
    final List<DataPartition> dataPartitions = computeDataPartitions(jobInfo);
    final MaterializationMetrics metrics = ReflectionUtils.computeMetrics(job, jobsService, allocator, JobsProtoUtil.toStuff(job.getJobId()));
    final List<String> refreshPath = ReflectionUtils.getRefreshPath(JobsProtoUtil.toStuff(job.getJobId()), accelerationBasePath, jobsService, allocator);
    final boolean isIcebergRefresh = materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    final String icebergBasePath = ReflectionUtils.getIcebergReflectionBasePath(materialization, refreshPath, isIcebergRefresh);
    final Refresh refresh = ReflectionUtils.createRefresh(materialization.getReflectionId(), refreshPath, seriesId,
      0, new UpdateId(), jobDetails, metrics, dataPartitions, isIcebergRefresh, icebergBasePath);
    refresh.setCompacted(true);

    // no need to update entry lastSuccessfulRefresh, as it may only cause unnecessary refreshes on dependant reflections

    materializationStore.save(materialization);
    materializationStore.save(refresh);

    // start a metadata refresh and delete compacted materialization, in parallel
    deleteCompactedMaterialization(entry);
    refreshMetadata(entry, materialization);
  }

  private void deleteCompactedMaterialization(ReflectionEntry entry) {
    final Materialization compacted = materializationStore.getLastMaterializationCompacted(entry.getId());
    if (compacted == null) {
      logger.warn("Couldn't find any compacted materialization for reflection {}", getId(entry));
      return;
    }

    deleteMaterialization(compacted);
  }

  private boolean shouldCompact(ReflectionEntry entry, Refresh refresh) {
    final long medianFileSize = refresh.getMetrics().getMedianFileSize();
    final int numFiles = refresh.getMetrics().getNumFiles();

    logger.debug("Reflection {} refresh wrote {} files with a median size of {} bytes", getId(entry), numFiles, medianFileSize);
    return numFiles > optionManager.getOption(COMPACTION_TRIGGER_NUMBER_FILES)
      && medianFileSize < optionManager.getOption(COMPACTION_TRIGGER_FILE_SIZE)*1024*1024;
  }

  private void metadataRefreshJobSucceeded(ReflectionEntry entry, Materialization materialization) {

    try {
      descriptorCache.update(materialization);
    } catch (Exception | AssertionError e) {
      logger.warn("failed to update materialization cache for {}", getId(materialization), e);
      materialization.setState(MaterializationState.FAILED)
        .setFailure(new Failure().setMessage("Cache update failed"));
    }

    if (materialization.getState() != MaterializationState.FAILED) {
      handleMaterializationDone(entry, materialization);
    }

    // we need to check the state of the materialization again because handleMaterializationDone() can change its state
    if (materialization.getState() == MaterializationState.FAILED) {
      // materialization failed
      reportFailure(entry, ACTIVE);
    }

    materializationStore.save(materialization);
    reflectionStore.save(entry);
  }

  private void refreshMetadata(ReflectionEntry entry, Materialization materialization) {
    final String sql = String.format("LOAD MATERIALIZATION METADATA \"%s\".\"%s\"",
      materialization.getReflectionId().getId(), materialization.getId().getId());

    final JobId jobId = submitRefreshJob(jobsService, namespaceService, entry, materialization, sql,
      new WakeUpManagerWhenJobDone(wakeUpCallback, "metadata refresh job done"));

    entry.setState(METADATA_REFRESH)
      .setRefreshJobId(jobId);
    reflectionStore.save(entry);

    logger.debug("started job {} to load materialization metadata {}", jobId.getId(), getId(materialization));
  }

  private void startRefresh(ReflectionEntry entry) {
    final long jobSubmissionTime = System.currentTimeMillis();
    // we should always update lastSubmittedRefresh to avoid an immediate refresh if we fail to start a refresh job
    entry.setLastSubmittedRefresh(jobSubmissionTime);

    if (DremioEdition.get() != DremioEdition.MARKETPLACE) {
      if (sabotContext.getExecutors().isEmpty() && System.currentTimeMillis() - sabotContext.getEndpoint().getStartTime() < START_WAIT_MILLIS) {
        logger.warn("reflection {} was not refreshed because no executors were available", entry.getId().getId());
        reportFailure(entry, ACTIVE);
        return;
      }
    }

    try {
      final JobId refreshJobId = refreshStartHandler.startJob(entry, jobSubmissionTime, optionManager);

      entry.setState(REFRESHING)
        .setRefreshJobId(refreshJobId);
      reflectionStore.save(entry);

      logger.debug("Started job {} to materialize reflection {}", refreshJobId.getId(), entry.getId().getId());
    } catch (Exception | AssertionError e) {
      // we failed to start the refresh
      logger.warn("failed to refresh reflection {}", getId(entry), e);
      // did we create a RUNNING materialization entry ?
      final Materialization m = materializationStore.getRunningMaterialization(entry.getId());
      if (m != null) {
        // yes. Let's make sure we mark it as FAILED
        m.setState(MaterializationState.FAILED);
        materializationStore.save(m);
      }
      reportFailure(entry, ACTIVE);
    }
  }

  private void reportFailure(ReflectionEntry entry, ReflectionState newState) {
    if (entry.getDontGiveUp()) {
      logger.debug("ignoring failure on reflection {} as it is marked as don't give up", getId(entry));
      entry.setState(newState)
        .setNumFailures(entry.getNumFailures() + 1);
      reflectionStore.save(entry);
      return;
    }

    final int numFailures = entry.getNumFailures() + 1;
    final long failuresThreshold = optionManager.getOption(LAYOUT_REFRESH_MAX_ATTEMPTS);
    final boolean markAsFailed = numFailures >= failuresThreshold;
    entry.setNumFailures(numFailures)
      .setState(markAsFailed ? FAILED : newState);
    reflectionStore.save(entry);

    if (markAsFailed) {
      logger.debug("reflection {} had {} consecutive failure and was marked with a FAILED state", getId(entry), numFailures);
      // remove the reflection from the dependency manager to update the dependencies of all its dependent reflections
      dependencyManager.delete(entry.getId());
    }
  }

  private ReflectionEntry create(ReflectionGoal goal) {
    logger.debug("creating new reflection {}", goal.getId().getId());
    //We currently store meta data in the Reflection Entry that possibly should not be there such as boost
    return new ReflectionEntry()
      .setId(goal.getId())
      .setGoalVersion(goal.getTag())
      .setReflectionGoalHash(reflectionGoalChecker.calculateReflectionGoalVersion(goal))
      .setDatasetId(goal.getDatasetId())
      .setState(REFRESH)
      .setType(goal.getType())
      .setName(goal.getName())
      .setArrowCachingEnabled(goal.getArrowCachingEnabled());
  }

  public long getLastWakeupTime() {
    return lastWakeupTime;
  }
}
