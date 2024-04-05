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
import static com.dremio.service.reflection.ReflectionOptions.ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_ORPHAN_REFRESH;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_GRACE_PERIOD;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_DELETION_NUM_ENTRIES;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_PENDING_ENABLED;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
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
import static com.dremio.service.reflection.proto.ReflectionState.REFRESH_PENDING;
import static com.dremio.service.reflection.proto.ReflectionState.UPDATE;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.common.util.DremioEdition;
import com.dremio.datastore.WarningTimer;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.DeleteJobCountsRequest;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.MaterializationSettings;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.SubstitutionSettings;
import com.dremio.service.job.UsedReflections;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.reflection.ReflectionServiceImpl.DescriptorCache;
import com.dremio.service.reflection.ReflectionServiceImpl.ExpansionHelper;
import com.dremio.service.reflection.ReflectionServiceImpl.PlanCacheInvalidationHelper;
import com.dremio.service.reflection.materialization.AccelerationStoragePlugin;
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
import com.dremio.service.reflection.proto.ReflectionPartitionField;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.RefreshDoneHandler;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Manages reflections, excluding external reflections, by observing changes to the reflection
 * goals, datasets, materialization jobs and executing the appropriate handling logic sequentially.
 */
public class ReflectionManager implements Runnable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReflectionManager.class);
  private static final long START_WAIT_MILLIS = 5 * 60 * 1000;
  @VisibleForTesting static final long GOAL_DELETION_WAIT_MILLIS = 60_000 * 60 * 4; // 4 hours

  private static final String REFRESH_DONE_HANDLER =
      "dremio.reflection.refresh.refresh-done-handler.class";

  /** Callback that allows async handlers to wake up the manager once they are done. */
  public interface WakeUpCallback {
    void wakeup(String reason);
  }

  /**
   * when the manager wakes up, it looks at all reflection goals that have been added/modified since
   * the last wakeup. this assumes that entries saved to the kvStore will instantaneously be
   * available, but in practice there will always be a slight delay. this constant defines protects
   * against skipping those entries.
   */
  static final long WAKEUP_OVERLAP_MS = 60_000;

  private final SabotContext sabotContext;
  private final JobsService jobsService;
  private final OptionManager optionManager;
  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore reflectionStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final MaterializationStore materializationStore;
  private final MaterializationPlanStore materializationPlanStore;
  private final DependencyManager dependencyManager;
  private final DescriptorCache descriptorCache;
  private final WakeUpCallback wakeUpCallback;
  private final Function<Catalog, ExpansionHelper> expansionHelper;
  private final Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper;
  private final BufferAllocator allocator;
  private final ReflectionGoalChecker reflectionGoalChecker;
  private final RefreshStartHandler refreshStartHandler;
  private final CatalogService catalogService;
  private final NamespaceService namespaceService;
  private volatile EntryCounts lastStats = new EntryCounts();
  private long lastWakeupTime;
  private long lastOrphanCheckTime;
  private DependencyResolutionContextFactory dependencyResolutionContextFactory;
  private final Meter.MeterProvider<Timer> syncHistogram;

  ReflectionManager(
      SabotContext sabotContext,
      JobsService jobsService,
      CatalogService catalogService,
      NamespaceService namespaceService,
      OptionManager optionManager,
      ReflectionGoalsStore userStore,
      ReflectionEntriesStore reflectionStore,
      ExternalReflectionStore externalReflectionStore,
      MaterializationStore materializationStore,
      MaterializationPlanStore materializationPlanStore,
      DependencyManager dependencyManager,
      DescriptorCache descriptorCache,
      WakeUpCallback wakeUpCallback,
      Function<Catalog, ExpansionHelper> expansionHelper,
      Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper,
      BufferAllocator allocator,
      ReflectionGoalChecker reflectionGoalChecker,
      RefreshStartHandler refreshStartHandler,
      DependencyResolutionContextFactory dependencyResolutionContextFactory) {
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "sabotContext required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobsService required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "optionManager required");
    this.userStore = Preconditions.checkNotNull(userStore, "reflection user store required");
    this.reflectionStore = Preconditions.checkNotNull(reflectionStore, "reflection store required");
    this.externalReflectionStore = Preconditions.checkNotNull(externalReflectionStore);
    this.materializationStore =
        Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.materializationPlanStore = materializationPlanStore;
    this.dependencyManager =
        Preconditions.checkNotNull(dependencyManager, "dependency manager required");
    this.descriptorCache = Preconditions.checkNotNull(descriptorCache, "descriptor cache required");
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
    this.expansionHelper =
        Preconditions.checkNotNull(expansionHelper, "sqlConvertSupplier required");
    this.planCacheInvalidationHelper =
        Preconditions.checkNotNull(
            planCacheInvalidationHelper, "planCacheInvalidatorHelper required");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalogService required");
    this.namespaceService =
        Preconditions.checkNotNull(namespaceService, "namespaceService required");
    this.reflectionGoalChecker = Preconditions.checkNotNull(reflectionGoalChecker);
    this.refreshStartHandler = Preconditions.checkNotNull(refreshStartHandler);
    this.dependencyResolutionContextFactory =
        Preconditions.checkNotNull(dependencyResolutionContextFactory);
    Metrics.newGauge(
        ReflectionMetrics.createName(ReflectionMetrics.RM_UNKNOWN),
        () -> ReflectionManager.this.lastStats.unknown);
    Metrics.newGauge(
        ReflectionMetrics.createName(ReflectionMetrics.RM_FAILED),
        () -> ReflectionManager.this.lastStats.failed);
    Metrics.newGauge(
        ReflectionMetrics.createName(ReflectionMetrics.RM_ACTIVE),
        () -> ReflectionManager.this.lastStats.active);
    Metrics.newGauge(
        ReflectionMetrics.createName(ReflectionMetrics.RM_REFRESHING),
        () -> ReflectionManager.this.lastStats.refreshing);
    syncHistogram =
        Timer.builder(ReflectionMetrics.createName(ReflectionMetrics.RM_SYNC))
            .description("Histogram of reflection manager sync times")
            .withRegistry(io.micrometer.core.instrument.Metrics.globalRegistry);
  }

  @Override
  public void run() {
    try (WarningTimer timer =
        new WarningTimer("Reflection Manager", TimeUnit.SECONDS.toMillis(5))) {
      boolean success = false;
      logger.trace("running the reflection manager");
      Instant start = Instant.now();
      try {
        sync();
        success = true;
      } catch (Throwable e) {
        logger.error("Reflection manager failed", e);
      } finally {
        Duration duration = Duration.between(start, Instant.now());
        PlannerMetrics.withOutcome(syncHistogram, success).record(duration);
        if (logger.isDebugEnabled()) {
          logger.debug("Reflection manager sync took {} ms", duration.toMillis());
        }
      }
    }
  }

  @WithSpan
  @VisibleForTesting
  void sync() {
    long currentTime = System.currentTimeMillis();
    // updating the store's lastWakeupTime here. This ensures that if we're failing we don't do a
    // denial of service attack
    // this assumes we properly handle exceptions for each goal/entry independently and we don't
    // exit the loop before we
    // go through all entities otherwise we may "skip" handling some entities in case of failures
    final long deletionGracePeriod = getDeletionGracePeriod();
    final long orphanThreshold =
        currentTime - optionManager.getOption(MATERIALIZATION_ORPHAN_REFRESH) * 1000;
    final long deletionThreshold = currentTime - deletionGracePeriod;
    final int numEntriesToDelete = (int) optionManager.getOption(REFLECTION_DELETION_NUM_ENTRIES);
    Span.current()
        .setAttribute("dremio.reflectionmanager.deletion_grace_period", deletionGracePeriod);
    Span.current()
        .setAttribute("dremio.reflectionmanager.num_entries_to_delete", numEntriesToDelete);
    Span.current().setAttribute("dremio.reflectionmanager.current_time", currentTime);
    Span.current().setAttribute("dremio.reflectionmanager.last_wakeup_time", lastWakeupTime);

    handleDeletedDatasets();
    handleGoals(lastWakeupTime - WAKEUP_OVERLAP_MS);
    try (DependencyResolutionContext context = dependencyResolutionContextFactory.create()) {
      Span.current()
          .setAttribute(
              "dremio.reflectionmanager.has_acceleration_settings_changed",
              context.hasAccelerationSettingsChanged());
      handleEntries(context);
      final DeleteCounts dc = new DeleteCounts(numEntriesToDelete);
      deleteDeprecatedMaterializations(deletionThreshold, dc);
      deleteMaterializationOrphans(context, orphanThreshold, deletionThreshold, dc);
      if (dc.deleted > 0) {
        logger.info("Submitted {} DROP TABLE statements.", dc.deleted);
      }
    }
    deprecateMaterializations();
    deleteDeprecatedGoals(deletionThreshold);
    this.lastWakeupTime = currentTime;
  }

  /**
   * Computes deletionGracePeriod based on whether materialization cache and query cache are
   * enabled. If either are enabled, deletion has to allow one {@link PlanCacheSynchronizer#sync} to
   * occur or else plan cache could end up referring to a disabled/deleted reflection.
   *
   * @return deletionGracePeriod
   */
  private long getDeletionGracePeriod() {
    if (!optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)
        && !optionManager.getOption(PlannerSettings.QUERY_PLAN_CACHE_ENABLED)) {
      return optionManager.getOption(REFLECTION_DELETION_GRACE_PERIOD) * 1000;
    } else {
      return Math.max(
          optionManager.getOption(REFLECTION_DELETION_GRACE_PERIOD) * 1000,
          optionManager.getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS) + 60_000);
    }
  }

  private void deleteOrphanMaterialization(Materialization materialization, DeleteCounts dc) {
    dc.orphans++;
    if (dc.deleted >= dc.maxDeletes) {
      return;
    }
    logger.debug("Deleting orphan {}", getId(materialization));
    try {
      deleteMaterialization(materialization);
    } catch (RuntimeException e) {
      logger.warn("Couldn't delete orphan {}", getId(materialization), e);
    }
    dc.deleted++;
  }

  /**
   * A materialization can get orphaned when 1. reflection_entry for materialization is not found.
   * 2. DROP TABLE job on the materialization fails.
   *
   * <p>In these situations, the materialization may still be taking up diskspace so re-submit DROP
   * TABLE job. See {@link AccelerationStoragePlugin#dropTable}
   */
  @WithSpan
  private void deleteMaterializationOrphans(
      DependencyResolutionContext context,
      long orphanThreshold,
      long depreciateDeletionThreshold,
      DeleteCounts dc) {

    if (orphanThreshold <= this.lastOrphanCheckTime) {
      return;
    }

    Iterable<Materialization> materializations = materializationStore.getAllMaterializations();
    for (Materialization materialization : materializations) {
      final ReflectionId rId = materialization.getReflectionId();
      // Use the DependencyManager's MaterializationInfo cache to check if the reflection still
      // exists
      if (rId == null || dependencyManager.getMaterializationInfoUnchecked(rId) == null) {
        if (materialization.getState() == MaterializationState.DEPRECATED) {
          if (materialization.getModifiedAt() <= depreciateDeletionThreshold) {
            deleteOrphanMaterialization(materialization, dc);
          }
        } else {
          deleteOrphanMaterialization(materialization, dc);
        }
      } else if (materialization.getState() == MaterializationState.DELETED
          && materialization.getModifiedAt() < orphanThreshold) {
        // Reflection entry still exists and previous DROP TABLE on the materialization failed.  Try
        // to drop again.
        deleteOrphanMaterialization(materialization, dc);
      }
    }
    this.lastOrphanCheckTime = System.currentTimeMillis();
    if (dc.orphans > 0) {
      logger.info("Found {} orphan materializations.", dc.orphans);
    }
  }

  /**
   * Remove any deleted goals that are due. Goal deletion will also clean up the KV store by
   * removing the reflection folder and possibly any child materialization datasets. Normally, the
   * child datasets should be removed as part of the deprecated materialization DROP TABLE job.
   * Since we would like the materialization dataset to be deleted properly through the DROP TABLE
   * job, we'll wait an additional GOAL_DELETION_WAIT_MILLIS before deleting the folder and any
   * child contents.
   *
   * @param deletionThreshold threshold after which deprecated reflection goals are deleted
   */
  @VisibleForTesting
  @WithSpan
  void deleteDeprecatedGoals(long deletionThreshold) {
    Iterable<ReflectionGoal> goalsDueForDeletion =
        userStore.getDeletedBefore(deletionThreshold - GOAL_DELETION_WAIT_MILLIS);
    for (ReflectionGoal goal : goalsDueForDeletion) {
      logger.debug("Goal due for deletion {}", getId(goal));
      userStore.delete(goal.getId());
      NamespaceKey reflectionFolderKey =
          new NamespaceKey(
              Lists.newArrayList(
                  ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME, goal.getId().getId()));
      try {
        FolderConfig config = namespaceService.getFolder(reflectionFolderKey);
        if (config != null) {
          namespaceService.deleteFolder(reflectionFolderKey, config.getTag());
        }
      } catch (Exception e) {
        logger.warn(
            "Unable to delete reflection folder {} for reflection {}",
            reflectionFolderKey,
            getId(goal),
            e);
      }
    }
  }

  @WithSpan
  private void deprecateMaterializations() {
    final long now = System.currentTimeMillis();
    Iterable<Materialization> materializations = materializationStore.getAllExpiredWhen(now);
    for (Materialization materialization : materializations) {
      try {
        deprecateMaterialization(materialization);
        ReflectionEntry entry = reflectionStore.get(materialization.getReflectionId());
        entry.setLastFailure(new Failure().setMessage("Last materialization expired"));
        updateReflectionEntry(entry);
      } catch (RuntimeException e) {
        logger.warn("Couldn't deprecate {}", getId(materialization));
      }
    }
  }

  /**
   * 3rd pass: go through the materialization store
   *
   * @param deletionThreshold threshold time after which deprecated materialization are deleted
   * @param dc
   */
  @WithSpan
  private void deleteDeprecatedMaterializations(long deletionThreshold, DeleteCounts dc) {
    Iterable<Materialization> materializations =
        materializationStore.getDeletableEntriesModifiedBefore(deletionThreshold, dc.maxDeletes);
    for (Materialization materialization : materializations) {
      logger.debug("Deleting deprecated {}", getId(materialization));
      try {
        dc.deleted++;
        deleteMaterialization(materialization);
      } catch (RuntimeException e) {
        logger.warn("Couldn't delete deprecated {}", getId(materialization));
      }
    }
  }

  /** 2nd pass: go through the reflection store */
  @WithSpan
  private void handleEntries(DependencyResolutionContext dependencyResolutionContext) {
    final long noDependencyRefreshPeriodMs =
        optionManager.getOption(ReflectionOptions.NO_DEPENDENCY_REFRESH_PERIOD_SECONDS) * 1000;

    Iterable<ReflectionEntry> entries = reflectionStore.find();
    final EntryCounts ec = new EntryCounts();
    for (ReflectionEntry entry : entries) {
      try {
        handleEntry(entry, noDependencyRefreshPeriodMs, ec, dependencyResolutionContext);
      } catch (RuntimeException e) {
        ec.unknown++;
        logger.error("Couldn't handle entry {}", getId(entry), e);
        reportFailure(entry, entry.getState());
        entry.setLastFailure(
            new Failure()
                .setMessage(
                    String.format("Couldn't handle entry %s: %s", getId(entry), e.getMessage()))
                .setStackTrace(Throwables.getStackTraceAsString(e)));
        updateReflectionEntry(entry);
      }
    }
    this.lastStats = ec;
    Span.current().setAttribute("dremio.reflectionmanager.entries_active", ec.active);
    Span.current().setAttribute("dremio.reflectionmanager.entries_failed", ec.failed);
    Span.current().setAttribute("dremio.reflectionmanager.entries_unknown", ec.unknown);
    Span.current().setAttribute("dremio.reflectionmanager.entries_refreshing", ec.refreshing);
  }

  @WithSpan
  private void handleDeletedDatasets() {
    Iterable<ReflectionGoal> goals = userStore.getAllNotDeleted();
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    int total = 0;
    int errors = 0;
    List<String> deleteReflectionIds = new ArrayList<>();
    for (ReflectionGoal goal : goals) {
      try {
        if (handleDatasetDeletion(goal.getDatasetId(), goal, catalog)) {
          deleteReflectionIds.add(goal.getId().getId());
        }
      } catch (RuntimeException exception) {
        // Usually source is down but need to catch all exceptions
        logger.debug("Unable the handleDatasetDeletion for {}", getId(goal), exception);
        errors++;
      }
      total++;
    }
    Iterable<ExternalReflection> externalReflections =
        externalReflectionStore.getExternalReflections();
    for (ExternalReflection externalReflection : externalReflections) {
      try {
        if (handleDatasetDeletionForExternalReflection(externalReflection, catalog)) {
          deleteReflectionIds.add(externalReflection.getId());
        }
      } catch (RuntimeException exception) {
        // Usually source is down but need to catch all exceptions
        logger.debug(
            "Unable the handleDatasetDeletion for {}", getId(externalReflection), exception);
        errors++;
      }
      total++;
    }
    try {
      if (deleteReflectionIds.size() > 0) {
        jobsService.deleteJobCounts(
            DeleteJobCountsRequest.newBuilder()
                .setReflections(
                    UsedReflections.newBuilder().addAllReflectionIds(deleteReflectionIds).build())
                .build());
      }
    } catch (Exception e) {
      logger.error(
          "Unable to delete job counts for reflection : {}, for deleted Datasets.",
          deleteReflectionIds,
          e);
    }
    Span.current().setAttribute("dremio.reflectionmanager.handle_deleted_datasets.total", total);
    Span.current().setAttribute("dremio.reflectionmanager.handle_deleted_datasets.errors", errors);
  }

  /** Small class that stores the results of reflection entry review */
  @VisibleForTesting
  static final class EntryCounts {
    private long failed;
    private long refreshing;
    private long active;
    private long unknown;
  }

  /** Small class that stores delete materialization counts */
  static final class DeleteCounts {
    DeleteCounts(int maxDeletes) {
      this.maxDeletes = maxDeletes;
    }

    private int deleted;
    private int orphans;
    private final int maxDeletes;
  }

  @VisibleForTesting
  void handleEntry(
      ReflectionEntry entry,
      final long noDependencyRefreshPeriodMs,
      EntryCounts counts,
      DependencyResolutionContext dependencyResolutionContext) {

    // If any acceleration settings changed in the system, we need to re-compute the dontGiveUp flag
    // which indicates
    // whether the reflection is manually triggered.  The reflection status UI heavily depends on
    // this flag being up-to-date.
    // Note: We can skip failed reflections because they have no dependencies and always have
    // dontGiveUp=false
    if (dependencyResolutionContext.hasAccelerationSettingsChanged()
        && entry.getState() != ReflectionState.FAILED) {
      dependencyManager.updateDontGiveUp(entry, dependencyResolutionContext);
    }

    final ReflectionState state = entry.getState();
    switch (state) {
      case FAILED:
        counts.failed++;
        // do nothing
        // TODO filter out those when querying the reflection store
        break;
      case REFRESHING:
      case METADATA_REFRESH:
      case COMPACTING:
        counts.refreshing++;
        handleRefreshingEntry(entry, dependencyResolutionContext);
        break;
      case UPDATE:
        counts.refreshing++;
        deprecateMaterializations(entry);
        startRefresh(entry);
        break;
      case ACTIVE:
        if (!dependencyManager.shouldRefresh(
            entry, noDependencyRefreshPeriodMs, dependencyResolutionContext)) {
          counts.active++;
          // only refresh ACTIVE reflections when they are due for refresh
          if (optionManager.getOption(ReflectionOptions.ENABLE_VACUUM_FOR_INCREMENTAL_REFLECTIONS)
              && dependencyManager.shouldVacuum(entry)) {
            vacuumReflection(entry);
          }
          break;
        }
        // fall through to refresh ACTIVE reflections that are due for refresh
      case REFRESH_PENDING:
        if (!refreshPendingHelper(
            entry, noDependencyRefreshPeriodMs, dependencyResolutionContext)) {
          counts.active++;
          // refresh pending until all dependencies finish refreshing or this reflection's
          // REFRESH_PENDING state timed out.
          break;
        }
        // fall through to refresh reflections that are due for refresh
      case REFRESH:
        counts.refreshing++;
        logger.info("Refresh due for {}", getId(entry));
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
   * Handles entry in REFRESH_PENDING state. A reflection is in REFRESH_PENDING state when it's due
   * for refresh, but it has direct or indirect dependencies also due for refresh or refreshing.
   * REFRESH_PENDING state could timeout (default 30 min by support flag
   * "reflection.manager.refresh_pending.timeout") to force the refresh to begin.
   *
   * @return true indicates refresh should begin. false indicates REFRESH_PENDING state should
   *     continue.
   */
  boolean refreshPendingHelper(
      final ReflectionEntry entry,
      final long noDependencyRefreshPeriodMs,
      final DependencyResolutionContext dependencyResolutionContext) {
    return refreshPendingHelper(
        System::currentTimeMillis, entry, noDependencyRefreshPeriodMs, dependencyResolutionContext);
  }

  boolean refreshPendingHelper(
      final java.util.function.Supplier<Long> currentTimeSupplier,
      final ReflectionEntry entry,
      final long noDependencyRefreshPeriodMs,
      final DependencyResolutionContext dependencyResolutionContext) {
    if (optionManager.getOption(REFLECTION_MANAGER_REFRESH_PENDING_ENABLED)) {
      final long refreshPendingTimeoutMillis =
          TimeUnit.MINUTES.toMillis(
              optionManager.getOption(
                  ReflectionOptions.REFLECTION_MANAGER_REFRESH_PENDING_TIMEOUT_MINUTES));
      if (dependencyManager.hasDependencyRefreshing(
          currentTimeSupplier,
          entry.getId(),
          noDependencyRefreshPeriodMs,
          dependencyResolutionContext)) {
        if (entry.getState() != REFRESH_PENDING) {
          logger.debug("Refresh pending for {}", getId(entry));
          entry.setState(REFRESH_PENDING);
          entry.setRefreshPendingBegin(currentTimeSupplier.get());
          updateReflectionEntry(entry);
          return false;
        }
        if (currentTimeSupplier.get() - entry.getRefreshPendingBegin()
            < refreshPendingTimeoutMillis) {
          return false;
        }
        logger.debug("Refresh pending timeout for {}", getId(entry));
      } else {
        logger.trace("{} has no dependency refreshing", getId(entry));
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace(
          "Refresh dependency paths for {}:\n{}",
          getId(entry),
          dependencyManager.describeRefreshPaths(entry.getId()));
    }
    return true;
  }

  public static void setSpanAttributes(
      final ReflectionEntry entry, final Materialization materialization) {
    if (entry != null) {
      Span.current()
          .setAttribute(
              "dremio.reflectionmanager.handle_entry.reflection_id", entry.getId().getId());
      Span.current().setAttribute("dremio.reflectionmanager.handle_entry.name", entry.getName());
      Span.current()
          .setAttribute("dremio.reflectionmanager.handle_entry.type", entry.getType().name());
      Span.current()
          .setAttribute("dremio.reflectionmanager.handle_entry.dataset_id", entry.getDatasetId());
      Span.current()
          .setAttribute("dremio.reflectionmanager.handle_entry.state", entry.getState().name());
    }
    if (materialization != null) {
      if (entry == null) {
        Span.current()
            .setAttribute(
                "dremio.reflectionmanager.handle_entry.reflection_id",
                materialization.getReflectionId().getId());
      }
      Span.current()
          .setAttribute(
              "dremio.reflectionmanager.handle_entry.materialization_id",
              materialization.getId().getId());
      Span.current()
          .setAttribute(
              "dremio.reflectionmanager.handle_entry.materialization_state",
              materialization.getState().name());
      Span.current()
          .setAttribute(
              "dremio.reflectionmanager.handle_entry.job_id",
              materialization.getInitRefreshJobId());
    }
  }

  /** handles entry in REFRESHING/METADATA_REFRESH state */
  @WithSpan
  private void handleRefreshingEntry(
      final ReflectionEntry entry, DependencyResolutionContext dependencyResolutionContext) {
    // handle job completion
    final Materialization m =
        Preconditions.checkNotNull(
            materializationStore.getLastMaterialization(entry.getId()),
            "Reflection %s in refreshing state has no materialization entries",
            entry.getId());
    if (m.getState() != MaterializationState.RUNNING && entry.getState() != COMPACTING) {
      // Reflection in refreshing state should have a materialization in RUNNING state but if
      // somehow we end up
      // in this weird state where the materialization store has an entry not in RUNNING state, we
      // need to cleanup that entry.
      // The exception to this is if we are running a OPTIMIZE job; the materialization is usable
      // while OPTIMIZE is running.
      try {
        deleteMaterialization(m);
        deleteReflection(entry);
        descriptorCache.invalidate(m);
      } catch (RuntimeException e) {
        logger.warn("Couldn't clean up {} {} during refresh", m.getState(), getId(m));
      }
      return;
    }
    setSpanAttributes(entry, m);

    com.dremio.service.job.JobDetails job;
    try {
      JobDetailsRequest request =
          JobDetailsRequest.newBuilder()
              .setJobId(JobsProtoUtil.toBuf(entry.getRefreshJobId()))
              .setUserName(SYSTEM_USERNAME)
              .setFromStore(true)
              .build();
      job = jobsService.getJobDetails(request);

    } catch (JobNotFoundException e) {
      // something's wrong, a refreshing entry means we already submitted a job and we should be
      // able to retrieve it.
      // let's handle this as a failure to avoid hitting an infinite loop trying to handle this
      // reflection entry
      m.setState(MaterializationState.FAILED)
          .setFailure(
              new Failure()
                  .setMessage(
                      String.format(
                          "Unable to retrieve job %s: %s",
                          entry.getRefreshJobId().getId(), e.getMessage())));
      materializationStore.save(m);
      reportFailure(entry, ACTIVE);
      entry.setLastFailure(
          new Failure()
              .setMessage(
                  String.format(
                      "Unable to retrieve job %s: %s",
                      entry.getRefreshJobId().getId(), e.getMessage()))
              .setStackTrace(Throwables.getStackTraceAsString(e)));
      updateReflectionEntry(entry);
      return;
    }

    if (!job.getCompleted()) {
      // job not done yet
      return;
    }
    JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    RefreshDoneHandler handler =
        new RefreshDoneHandler(
            entry,
            m,
            job,
            jobsService,
            materializationStore,
            materializationPlanStore,
            dependencyManager,
            expansionHelper,
            getAccelerationPlugin().getConfig().getPath(),
            allocator,
            catalogService,
            dependencyResolutionContext,
            sabotContext.getConfig());
    handler =
        sabotContext
            .getConfig()
            .getInstance(REFRESH_DONE_HANDLER, RefreshDoneHandler.class, handler, handler);
    switch (lastAttempt.getState()) {
      case COMPLETED:
        try {
          logger.debug(
              "Job {} completed successfully for {} took {} s",
              job.getJobId().getId(),
              getId(m),
              TimeUnit.MILLISECONDS.toSeconds(
                  lastAttempt.getInfo().getFinishTime() - lastAttempt.getInfo().getStartTime()));
          handleSuccessfulJob(entry, m, job, handler);
        } catch (Exception e) {
          final String message =
              String.format(
                  "Error occurred during job %s: %s", job.getJobId().getId(), e.getMessage());
          logger.error(message, e);
          m.setState(MaterializationState.FAILED).setFailure(new Failure().setMessage(message));
          materializationStore.save(m);
          reportFailure(entry, ACTIVE);
          entry.setLastFailure(
              new Failure()
                  .setMessage(
                      String.format(
                          "Error occurred during job %s: %s",
                          job.getJobId().getId(), e.getMessage()))
                  .setStackTrace(Throwables.getStackTraceAsString(e)));
          updateReflectionEntry(entry);
        }
        break;
      case CANCELED:
        logger.debug("Job {} was cancelled for {}", job.getJobId().getId(), getId(m));
        if (entry.getState() == COMPACTING) {
          optimizeJobCanceledOrFailed(entry, m, lastAttempt);
          break;
        } else if (entry.getState() == REFRESHING) {
          // try to update the dependencies even when a refreshing job fails
          updateDependenciesIfPossible(entry, lastAttempt, handler);
        }
        rollbackIcebergTableIfNecessary(m, lastAttempt, handler);
        m.setState(MaterializationState.CANCELED);
        materializationStore.save(m);
        entry.setState(ACTIVE);
        updateReflectionEntry(entry);
        break;
      case FAILED:
        logger.debug("Job {} failed for {}", job.getJobId().getId(), getId(m));
        if (entry.getState() == COMPACTING) {
          optimizeJobCanceledOrFailed(entry, m, lastAttempt);
          break;
        } else if (entry.getState() == REFRESHING) {
          // try to update the dependencies even when a refreshing job fails
          updateDependenciesIfPossible(entry, lastAttempt, handler);
        }
        rollbackIcebergTableIfNecessary(m, lastAttempt, handler);
        final String jobFailure =
            Optional.ofNullable(lastAttempt.getInfo().getFailureInfo())
                .orElse("Reflection Job failed without reporting an error message");
        m.setState(MaterializationState.FAILED).setFailure(new Failure().setMessage(jobFailure));
        materializationStore.save(m);
        reportFailure(entry, ACTIVE);
        entry.setLastFailure(new Failure().setMessage(jobFailure));
        updateReflectionEntry(entry);
        break;
      default:
        // nothing to do for non terminal states
        break;
    }
  }

  /**
   * It is possible that the iceberg reflection table is updated and commited, before the refresh
   * job fails/cancelled. In that case we need to rollback the table to the previous snapshot. Or
   * else we will insert the same records again in next refresh.
   */
  private void rollbackIcebergTableIfNecessary(
      final Materialization m,
      final JobAttempt jobAttempt,
      final RefreshDoneHandler refreshDoneHandler) {
    if (m.getIsIcebergDataset() != null && m.getIsIcebergDataset()) {
      if (jobAttempt.getExtraInfoList() == null || jobAttempt.getExtraInfoList().isEmpty()) {
        // It can happen that refresh plan was not generated successfully (for example, if source is
        // unavailable) and so
        // the refresh decision was not set, in which case there is no need to rollback
        return;
      }
      final RefreshDecision refreshDecision = refreshDoneHandler.getRefreshDecision(jobAttempt);
      if (refreshDecision.getInitialRefresh() != null && !refreshDecision.getInitialRefresh()) {
        final Table table = getIcebergTable(m.getReflectionId(), m.getBasePath());

        // rollback table if the snapshotId changed
        if (table.currentSnapshot().snapshotId() != m.getPreviousIcebergSnapshot()) {
          table.manageSnapshots().rollbackTo(m.getPreviousIcebergSnapshot()).commit();
        }
      }
    }
  }

  /**
   * A refresh reflection job may fail but the job can still return dependencies. This is important
   * for initial manual refreshes since retry won't happen and now that the dependency graph has
   * linked the PDS with this reflection, the user can still re-try the reflection manually from the
   * PDS.
   *
   * @param entry
   * @param jobAttempt
   * @param refreshDoneHandler
   */
  private void updateDependenciesIfPossible(
      final ReflectionEntry entry,
      final JobAttempt jobAttempt,
      final RefreshDoneHandler refreshDoneHandler) {
    if (dependencyManager.reflectionHasKnownDependencies(entry.getId())) {
      return;
    }

    try {
      final RefreshDecision decision = refreshDoneHandler.getRefreshDecision(jobAttempt);
      refreshDoneHandler.updateDependencies(
          entry, jobAttempt.getInfo(), decision, dependencyManager);
    } catch (Exception | AssertionError e) {
      logger.warn("Couldn't retrieve any dependency for {}", getId(entry), e);
    }
  }

  /**
   * 1st pass: observe changes in the reflection user store find all goals that were created or
   * modified since last wakeup for each identified description if it doesn't have a corresponding
   * reflection it's a NEW one if it does and the version has changed it's an UPDATE if it has a
   * DELETED state, it's...well guess ;)
   *
   * @param lastWakeupTime previous wakeup time
   */
  @WithSpan
  private void handleGoals(long lastWakeupTime) {
    Iterable<ReflectionGoal> goals = userStore.getModifiedOrCreatedSince(lastWakeupTime);
    for (ReflectionGoal goal : goals) {
      try {
        handleGoal(goal);
      } catch (Exception e) {
        logger.error("Couldn't handle goal for {}", getId(goal), e);
      }
    }
  }

  /**
   * Checks if dataset has been deleted from reflection manager's point of view. For example, a DROP
   * TABLE will delete a table. But a DROP BRANCH and DROP TAG could also result in a deleted table.
   * ASSIGN BRANCH and ASSIGN TAG could also result in a deleted table if the table is no longer
   * present in the updated ref's commit log.
   *
   * <p>Catalog returns null in the above scenarios. Catalog could also throw a UserException when
   * the source is down in which case we don't delete the dataset's reflections.
   *
   * @param datasetId
   * @param goal
   * @param catalog
   * @return true if ReflectionGoal is marked DELETED, else return false
   */
  private boolean handleDatasetDeletion(
      String datasetId, ReflectionGoal goal, EntityExplorer catalog) {
    // make sure the corresponding dataset was not deleted
    if (catalog.getTable(datasetId) == null) {
      // dataset not found, mark goal as deleted
      logger.debug("dataset with id {} deleted for {}", datasetId, getId(goal));

      final ReflectionGoal goal2 = userStore.get(goal.getId());
      if (goal2 != null) {
        try {
          userStore.save(goal2.setState(ReflectionGoalState.DELETED));
          return true;
        } catch (ConcurrentModificationException cme) {
          // someone's changed the reflection goal, we'll delete it next time the manager wakes up
          logger.debug(
              "concurrent modification when updating goal state to deleted for {} on dataset with id {}",
              getId(goal2),
              datasetId);
        }
      }
      // something wrong here
      throw new IllegalStateException(
          "no reflection found for " + getId(goal) + "on dataset with id" + datasetId);
    }
    return false;
  }

  private boolean handleDatasetDeletionForExternalReflection(
      ExternalReflection externalReflection, EntityExplorer catalog) {
    if (catalog.getTable(externalReflection.getQueryDatasetId()) == null
        || catalog.getTable(externalReflection.getTargetDatasetId()) == null) {
      externalReflectionStore.deleteExternalReflection(externalReflection.getId());
      return true;
    }
    return false;
  }

  @VisibleForTesting
  void handleGoal(ReflectionGoal goal) {
    final ReflectionEntry entry = reflectionStore.get(goal.getId());
    if (entry == null) {
      // no corresponding reflection, goal has been created or enabled
      if (goal.getState()
          == ReflectionGoalState
              .ENABLED) { // we still need to make sure user didn't create a disabled goal
        createReflectionEntry(create(goal));
      }
    } else if (reflectionGoalChecker.isEqual(goal, entry)) {
      return; // no changes, do nothing
    } else if (reflectionGoalChecker.checkHash(goal, entry)) {
      // Check if entries need to update meta data that is not used in the materialization
      updateThatHasChangedEntry(goal, entry);

      for (Materialization materialization : materializationStore.find(entry.getId())) {
        if (!Objects.equals(
            materialization.getArrowCachingEnabled(), goal.getArrowCachingEnabled())) {
          materializationStore.save(
              materialization
                  .setArrowCachingEnabled(goal.getArrowCachingEnabled())
                  .setReflectionGoalVersion(goal.getTag()));
        }
      }
    } else {
      // descriptor changed
      logger.debug("Updated state {} -> {} for {}", entry.getState(), goal.getState(), getId(goal));
      cancelRefreshJobIfAny(entry);
      final boolean enabled = goal.getState() == ReflectionGoalState.ENABLED;
      entry
          .setState(enabled ? UPDATE : DEPRECATE)
          .setArrowCachingEnabled(goal.getArrowCachingEnabled())
          .setGoalVersion(goal.getTag())
          .setName(goal.getName())
          .setReflectionGoalHash(reflectionGoalChecker.calculateReflectionGoalVersion(goal));
      updateReflectionEntry(entry);
    }
  }

  private void updateThatHasChangedEntry(
      ReflectionGoal reflectionGoal, ReflectionEntry reflectionEntry) {
    // This restarts a failed scheduled reflection
    boolean forceUpdate =
        reflectionGoal.getState() == ReflectionGoalState.ENABLED
            && reflectionEntry.getState() == FAILED;

    // This restarts any active reflection that is not able to accelerate queries
    if (!forceUpdate
        && reflectionEntry.getState().equals(ACTIVE)
        && materializationStore.getLastMaterializationDone(reflectionGoal.getId()) == null) {
      forceUpdate = true;
    }

    updateReflectionEntry(
        reflectionEntry
            .setArrowCachingEnabled(reflectionGoal.getArrowCachingEnabled())
            .setGoalVersion(reflectionGoal.getTag())
            .setName(reflectionGoal.getName())
            .setState(forceUpdate ? UPDATE : reflectionEntry.getState())
            .setNumFailures(forceUpdate ? 0 : reflectionEntry.getNumFailures()));
  }

  @WithSpan
  private void deleteReflection(ReflectionEntry entry) {
    setSpanAttributes(entry, null);
    logger.debug("Deleting reflection entry for {}", getId(entry));
    deleteReflectionEntry(entry.getId());
    dependencyManager.delete(entry.getId());
  }

  @WithSpan
  private void deleteMaterialization(Materialization materialization) {

    // set the materialization to DELETED so we don't try to delete it again
    materialization.setState(MaterializationState.DELETED);
    materializationStore.save(materialization);

    try {
      final String pathString = constructFullPath(getMaterializationPath(materialization));
      final String query = String.format("DROP TABLE IF EXISTS %s", pathString);

      JobProtobuf.MaterializationSummary materializationSummary =
          JobProtobuf.MaterializationSummary.newBuilder()
              .setReflectionId(materialization.getReflectionId().getId())
              .setLayoutVersion(materialization.getReflectionGoalVersion())
              .setMaterializationId(materialization.getId().getId())
              .build();
      final JobId jobId =
          jobsService
              .submitJob(
                  SubmitJobRequest.newBuilder()
                      .setMaterializationSettings(
                          MaterializationSettings.newBuilder()
                              .setMaterializationSummary(materializationSummary)
                              .setSubstitutionSettings(
                                  SubstitutionSettings.newBuilder()
                                      .addAllExclusions(ImmutableList.of())
                                      .build())
                              .build())
                      .setSqlQuery(
                          SqlQuery.newBuilder()
                              .setSql(query)
                              .addAllContext(Collections.emptyList())
                              .setUsername(SYSTEM_USERNAME)
                              .build())
                      .setQueryType(QueryType.ACCELERATOR_DROP)
                      .build(),
                  JobStatusListener.NO_OP)
              .getJobId();

      logger.debug(
          "Submitted DROP TABLE job {} for {}",
          jobId.getId(),
          ReflectionUtils.getId(materialization));

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

  @WithSpan
  private void deprecateMaterialization(Materialization materialization) {
    setSpanAttributes(null, materialization);
    logger.debug("Deprecating {}", ReflectionUtils.getId(materialization));
    materialization.setState(MaterializationState.DEPRECATED);
    materializationStore.save(materialization);
    descriptorCache.invalidate(materialization);
  }

  private void deprecateLastMaterialization(Materialization materialization) {
    Materialization lastDone =
        materializationStore.getLastMaterializationDone(materialization.getReflectionId());
    if (lastDone != null) {
      deprecateMaterialization(lastDone);
    }
  }

  private void handleMaterializationDone(ReflectionEntry entry, Materialization materialization) {
    final long now = System.currentTimeMillis();
    final long materializationExpiry =
        Optional.ofNullable(materialization.getExpiration()).orElse(0L);
    if (materializationExpiry <= now) {
      materialization
          .setState(MaterializationState.FAILED)
          .setFailure(new Failure().setMessage("Successful materialization already expired"));
      logger.warn(
          "Successful materialization already expired for {}",
          ReflectionUtils.getId(materialization));
      entry.setLastFailure(new Failure().setMessage("Successful materialization already expired"));
      updateReflectionEntry(entry);
    } else {
      deprecateLastMaterialization(materialization);
      materialization.setState(MaterializationState.DONE);
      entry
          .setState(ACTIVE)
          .setLastSuccessfulRefresh(System.currentTimeMillis())
          .setNumFailures(0)
          .setLastFailure(null);

      // Only update the vacuum info for incremental refreshes
      if (entry.getRefreshMethod() == RefreshMethod.INCREMENTAL
          && materialization.getIsIcebergDataset()
          && materialization.getSeriesOrdinal() > 0) {
        // Update current MaterializationId and SnapshotId cache, so we can determine when to vacuum
        // the reflection
        EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
        NamespaceKey namespaceKey =
            new NamespaceKey(
                Lists.newArrayList(
                    ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
                    entry.getId().getId(),
                    materialization.getId().getId()));
        DremioTable table = catalog.getTable(namespaceKey);
        if (table == null) {
          logger.error("Reflection {} not found", entry.getId().getId());
          return;
        }
        if (!materialization.getIsIcebergDataset()) {
          logger.debug(
              "Iceberg is disabled, cannot update materialization cache for reflection {}",
              entry.getId().getId());
          return;
        }
        long snapshotId =
            table.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getSnapshotId();
        dependencyManager.updateMaterializationInfo(
            entry.getId(), materialization.getId(), snapshotId, false);
      }
    }
  }

  private void cancelRefreshJobIfAny(ReflectionEntry entry) {
    if (entry.getState() != REFRESHING && entry.getState() != METADATA_REFRESH) {
      return;
    }

    final Materialization m =
        Preconditions.checkNotNull(
            materializationStore.getLastMaterialization(entry.getId()),
            "reflection entry %s is in REFRESHING|METADATA_REFRESH state but has no materialization entry",
            entry.getId());

    try {
      logger.debug("Cancelling job {} for {}", entry.getRefreshJobId().getId(), getId(entry));
      // even though the following method can block if the job's foreman is on a different node,
      // it's not a problem here
      // as we always submit reflection jobs on the same node as the manager
      jobsService.cancel(
          CancelJobRequest.newBuilder()
              .setUsername(SYSTEM_USERNAME)
              .setJobId(JobsProtoUtil.toBuf(entry.getRefreshJobId()))
              .setReason("Query cancelled by Reflection Manager. Reflection configuration is stale")
              .build());

      entry.setLastFailure(
          new Failure()
              .setMessage(
                  "Query cancelled by Reflection Manager. Reflection configuration is stale"));
      updateReflectionEntry(entry);
    } catch (JobException e) {
      logger.warn("Failed to cancel job for reflection {}", getId(entry), e);
    }

    // mark the materialization as cancelled
    m.setState(MaterializationState.CANCELED);
    materializationStore.save(m);

    // we don't need to handle the job, if it did complete and wrote some data, they will eventually
    // get deleted
    // when the materialization entry is deleted
  }

  @VisibleForTesting
  void handleSuccessfulJob(
      ReflectionEntry entry,
      Materialization materialization,
      com.dremio.service.job.JobDetails job,
      RefreshDoneHandler handler) {
    switch (entry.getState()) {
      case REFRESHING:
        refreshingJobSucceded(entry, materialization, job, handler);
        break;
      case COMPACTING:
        // There are two types of compaction - COMPACT and OPTIMIZE.
        // Once DX-60382 is resolved, we can remove the old COMPACT code.
        com.dremio.service.job.proto.QueryType queryType =
            JobsProtoUtil.getLastAttempt(job).getInfo().getQueryType();
        if (queryType == com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE) {
          compactionJobSucceeded(entry, materialization, job);
        } else if (queryType == com.dremio.service.job.proto.QueryType.ACCELERATOR_OPTIMIZE) {
          optimizeJobSucceeded(entry, materialization, job);
        }
        break;
      case METADATA_REFRESH:
        metadataRefreshJobSucceeded(entry, materialization);
        break;
      default:
        throw new IllegalStateException("Unexpected state " + entry.getState());
    }
  }

  private void refreshingJobSucceded(
      ReflectionEntry entry,
      Materialization materialization,
      com.dremio.service.job.JobDetails job,
      RefreshDoneHandler handler) {

    try {
      final RefreshDecision decision = handler.handle();

      // no need to set the following attributes if we fail to handle the refresh
      entry
          .setRefreshMethod(decision.getAccelerationSettings().getMethod())
          .setRefreshField(decision.getAccelerationSettings().getRefreshField())
          .setSnapshotBased(decision.getAccelerationSettings().getSnapshotBased())
          .setExpandedPlanDatasetHash(decision.getDatasetHash())
          .setShallowDatasetHash(decision.getDatasetHash());
    } catch (Exception | AssertionError e) {
      logger.warn("Failed to handle done job for {}", getId(entry), e);
      materialization
          .setState(MaterializationState.FAILED)
          .setFailure(
              new Failure()
                  .setMessage(
                      String.format(
                          "Failed to handle successful REFLECTION REFRESH job %s: %s",
                          job.getJobId().getId(), e.getMessage())));
      entry.setLastFailure(
          new Failure()
              .setMessage(
                  String.format(
                      "Failed to handle successful REFLECTION REFRESH job %s: %s",
                      job.getJobId().getId(), e.getMessage()))
              .setStackTrace(Throwables.getStackTraceAsString(e)));
    }

    // update the namespace metadata before saving information to the reflection store to avoid
    // concurrent updates.
    if (materialization.getState() != MaterializationState.FAILED) {

      // expiration may not be set if materialization has failed
      final long materializationExpiry =
          Optional.ofNullable(materialization.getExpiration()).orElse(0L);
      if (materializationExpiry <= System.currentTimeMillis()) {
        materialization
            .setState(MaterializationState.FAILED)
            .setFailure(new Failure().setMessage("Successful materialization already expired"));
        entry.setLastFailure(
            new Failure().setMessage("Successful materialization already expired"));
        updateReflectionEntry(entry);
        logger.warn(
            "Successful REFLECTION REFRESH but already expired for {}",
            ReflectionUtils.getId(materialization));
      } else {
        // even if the materialization didn't write any data it may still own refreshes if it's a
        // non-initial incremental
        // otherwise we don't want to refresh an empty table as it will just fail
        final List<Refresh> refreshes = materializationStore.getRefreshes(materialization).toList();
        if (!refreshes.isEmpty()) {
          try {
            Preconditions.checkState(
                materialization.getState() != MaterializationState.FAILED,
                "failed materialization");
            if (!compactIfNecessary(entry, materialization, refreshes)) {
              refreshMetadata(entry, materialization);
            }
          } catch (Exception | AssertionError e) {
            logger.warn(
                "Failed to start LOAD MATERIALIZATION job for {}", getId(materialization), e);
            materialization
                .setState(MaterializationState.FAILED)
                .setFailure(
                    new Failure()
                        .setMessage(
                            String.format(
                                "Failed to start LOAD MATERIALIZATION job: %s", e.getMessage())));
            entry.setLastFailure(
                new Failure()
                    .setMessage(
                        String.format(
                            "Failed to start LOAD MATERIALIZATION job: %s", e.getMessage()))
                    .setStackTrace(Throwables.getStackTraceAsString(e)));
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
    updateReflectionEntry(entry);
    try (PlanCacheInvalidationHelper helper = planCacheInvalidationHelper.get()) {
      helper.invalidateReflectionAssociatedPlanCache(entry.getDatasetId());
    }
  }

  private boolean compactIfNecessary(
      ReflectionEntry entry, Materialization materialization, List<Refresh> refreshes) {
    if (!optionManager.getOption(ENABLE_COMPACTION)) {
      return false; // compaction disabled by user
    }
    if (entry.getRefreshMethod() != RefreshMethod.FULL) {
      logger.debug("Skipping compaction check for incremental refresh on {}", getId(entry));
      return false; // only applies to full refresh
    }

    // full refresh materializations must contain a single refresh entry
    Preconditions.checkState(
        refreshes.size() == 1, "expected 1 refresh entry found %s", refreshes.size());
    final Refresh refresh = refreshes.get(0);

    if (!shouldCompact(entry, refresh)) {
      return false;
    }

    final ReflectionGoal goal =
        Preconditions.checkNotNull(
            userStore.get(entry.getId()),
            "Couldn't find associated reflection goal to reflection %s",
            getId(entry));
    final List<ReflectionPartitionField> partitionFields =
        goal.getDetails().getPartitionFieldList();
    if (partitionFields != null && !partitionFields.isEmpty()) {
      // TODO warn user through UI
      logger.warn("Compaction with partition fields not supported for {}", getId(materialization));
      return false;
    }

    // mark current materialization as COMPACTED (terminal state)
    materialization.setState(MaterializationState.COMPACTED);
    materializationStore.save(materialization);

    logger.debug("Compacting {}", getId(entry));

    // create a new materialization entry for the compacted data
    final Materialization newMaterialization =
        new Materialization()
            .setId(new MaterializationId(UUID.randomUUID().toString()))
            .setReflectionId(entry.getId())
            .setState(MaterializationState.RUNNING)
            .setExpiration(materialization.getExpiration())
            .setLastRefreshFromPds(materialization.getLastRefreshFromPds())
            .setLastRefreshFinished(materialization.getLastRefreshFinished())
            .setLastRefreshDurationMillis(materialization.getLastRefreshDurationMillis())
            .setReflectionGoalVersion(materialization.getReflectionGoalVersion())
            .setJoinAnalysis(materialization.getJoinAnalysis())
            .setInitRefreshSubmit(
                System
                    .currentTimeMillis()) // needed to properly return this materialization as last
            // one
            .setInitRefreshExecution(materialization.getInitRefreshExecution())
            .setInitRefreshJobId(materialization.getInitRefreshJobId());
    materializationStore.save(newMaterialization);

    // start compaction job
    final String sql =
        String.format(
            "COMPACT MATERIALIZATION \"%s\".\"%s\" AS '%s'",
            entry.getId().getId(),
            materialization.getId().getId(),
            newMaterialization.getId().getId());

    final JobId compactionJobId =
        submitRefreshJob(
            jobsService,
            catalogService,
            entry,
            materialization.getId(),
            sql,
            com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE,
            new WakeUpManagerWhenJobDone(wakeUpCallback, "compaction job done"));

    newMaterialization.setInitRefreshJobId(compactionJobId.getId());
    materializationStore.save(newMaterialization);

    entry.setState(COMPACTING).setRefreshJobId(compactionJobId);
    updateReflectionEntry(entry);

    logger.debug(
        "Submitted COMPACT MATERIALIZATION job {} for {} as {}",
        compactionJobId.getId(),
        getId(materialization),
        newMaterialization.getId().getId());
    return true;
  }

  protected void maybeOptimizeIncrementalReflectionFiles(
      ReflectionEntry entry, Materialization materialization) {
    final boolean isIcebergRefresh =
        materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    Optional<List<ReflectionField>> sortedFields =
        Optional.ofNullable(userStore.get(entry.getId()).getDetails().getSortFieldList());
    String entryId = getId(entry);
    ImmutableList<Refresh> refreshes =
        materializationStore
            .getRefreshes(materialization)
            .toSortedList(Comparator.comparingInt(Refresh::getSeriesOrdinal))
            .reverse();
    int refreshesSinceOptimization = 0;
    if (!optionManager.getOption(ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS)
        || !isIcebergRefresh) {
      // OPTIMIZE TABLE only works for Iceberg tables and the feature flag must be enabled
      return;
    }
    if (entry.getRefreshMethod() != RefreshMethod.INCREMENTAL) {
      // only applies to incremental refreshes
      logger.debug("Skipping OPTIMIZE TABLE for full refresh on reflection {}", entryId);
      return;
    }
    // TODO: remove this logic when sorted tables are supported following completion of DX-55508.
    if (sortedFields.isPresent() && sortedFields.get().size() > 0) {
      logger.debug(
          "Skipping OPTIMIZE TABLE for reflection {} because sorted reflections are not supported",
          entryId);
      return;
    }
    if (refreshes.size()
        < optionManager.getOption(
            ReflectionOptions.OPTIMIZE_REFLECTION_REQUIRED_REFRESHES_BETWEEN_RUNS)) {
      // Don't bother going through the refreshes unless there are enough to OPTIMIZE
      logger.debug(
          "Skipping OPTIMIZE TABLE for reflection {} because it has not been refreshed enough times.",
          entryId);
      return;
    }
    // Go through the refreshes and see if enough have happened since the last OPTIMIZE job.
    for (Refresh r : refreshes) {
      if (r.getCompacted()) {
        logger.debug(
            "Refreshes until next OPTIMIZE job for reflection {}: {}",
            entryId,
            optionManager.getOption(
                    ReflectionOptions.OPTIMIZE_REFLECTION_REQUIRED_REFRESHES_BETWEEN_RUNS)
                - refreshesSinceOptimization);
        return;
      } else {
        refreshesSinceOptimization += 1;
      }
      if (refreshesSinceOptimization
          == optionManager.getOption(
              ReflectionOptions.OPTIMIZE_REFLECTION_REQUIRED_REFRESHES_BETWEEN_RUNS)) {
        break;
      }
    }

    logger.debug("Starting OPTIMIZE TABLE job for reflection {}", entryId);

    int newSeriesOrdinal = materialization.getSeriesOrdinal() + 1;
    final Materialization newMaterialization =
        new Materialization()
            .setId(new MaterializationId(UUID.randomUUID().toString()))
            .setReflectionId(entry.getId())
            .setState(MaterializationState.RUNNING)
            .setExpiration(materialization.getExpiration())
            .setLastRefreshFromPds(materialization.getLastRefreshFromPds())
            .setLastRefreshFinished(materialization.getLastRefreshFinished())
            .setLastRefreshDurationMillis(materialization.getLastRefreshDurationMillis())
            .setReflectionGoalVersion(materialization.getReflectionGoalVersion())
            .setJoinAnalysis(materialization.getJoinAnalysis())
            .setInitRefreshSubmit(
                System
                    .currentTimeMillis()) // needed to properly return this materialization as last
            // one
            .setInitRefreshExecution(materialization.getInitRefreshExecution())
            .setInitRefreshJobId(materialization.getInitRefreshJobId())
            .setSeriesId(materialization.getSeriesId())
            .setSeriesOrdinal(newSeriesOrdinal)
            .setIsIcebergDataset(true)
            .setPreviousIcebergSnapshot(materialization.getPreviousIcebergSnapshot())
            .setBasePath(materialization.getBasePath())
            .setStripVersion(StrippingFactory.LATEST_STRIP_VERSION);

    materializationStore.save(newMaterialization);

    // start OPTIMIZE TABLE job
    final String sql =
        String.format(
            "OPTIMIZE TABLE \"%s\".\"%s\".\"%s\"",
            ACCELERATOR_STORAGEPLUGIN_NAME, entry.getId().getId(), materialization.getId().getId());
    final JobId jobId =
        submitRefreshJob(
            jobsService,
            catalogService,
            entry,
            materialization.getId(),
            sql,
            com.dremio.service.job.proto.QueryType.ACCELERATOR_OPTIMIZE,
            new WakeUpManagerWhenJobDone(wakeUpCallback, "OPTIMIZE TABLE for reflection job done"));

    newMaterialization.setInitRefreshJobId(jobId.getId());
    materializationStore.save(newMaterialization);

    entry.setState(COMPACTING).setRefreshJobId(jobId);
    updateReflectionEntry(entry);

    logger.debug("Submitted OPTIMIZE TABLE job {} for {}", jobId.getId(), getId(materialization));
  }

  @WithSpan
  private void vacuumReflection(ReflectionEntry entry) {
    MaterializationInfo materializationInfo =
        dependencyManager.getMaterializationInfo(entry.getId());
    Span.current()
        .setAttribute("dremio.reflectionmanager.vacuum.reflection_id", entry.getId().getId());
    Span.current()
        .setAttribute(
            "dremio.reflectionmanager.vacuum.materialization_id",
            materializationInfo.getMaterializationId().getId());
    Span.current()
        .setAttribute(
            "dremio.reflectionmanager.vacuum.snapshot_id", materializationInfo.getSnapshotId());

    String vacuumBaseString =
        "VACUUM TABLE \"%s\".\"%s\".\"%s\" EXPIRE SNAPSHOTS OLDER_THAN = \'%s\'";
    String entryIdString = entry.getId().getId();
    String materializationIdString = materializationInfo.getMaterializationId().getId();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
    Date olderThanDate = new Date(System.currentTimeMillis() - getDeletionGracePeriod());
    String formattedOlderThanTime = formatter.format(olderThanDate);
    final String vacuumQuery =
        String.format(
            vacuumBaseString,
            ACCELERATOR_STORAGEPLUGIN_NAME,
            entryIdString,
            materializationIdString,
            formattedOlderThanTime);
    logger.debug(
        "Submitting VACUUM TABLE job for reflection {}.{}", entryIdString, materializationIdString);
    submitRefreshJob(
        jobsService,
        catalogService,
        entry,
        materializationInfo.getMaterializationId(),
        vacuumQuery,
        com.dremio.service.job.proto.QueryType.ACCELERATOR_OPTIMIZE,
        JobStatusListener.NO_OP);
    dependencyManager.updateMaterializationInfo(
        entry.getId(),
        materializationInfo.getMaterializationId(),
        materializationInfo.getSnapshotId(),
        true);
  }

  private void compactionJobSucceeded(
      ReflectionEntry entry,
      Materialization materialization,
      com.dremio.service.job.JobDetails job) {
    // update materialization seriesId/seriesOrdinal to point to the new refresh
    final long seriesId = System.currentTimeMillis();
    materialization.setSeriesId(seriesId).setSeriesOrdinal(0);

    // create new refresh entry that points to the compacted data
    final JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    final JobInfo jobInfo = lastAttempt.getInfo();
    final JobDetails jobDetails = ReflectionUtils.computeJobDetails(lastAttempt);
    final List<DataPartition> dataPartitions = computeDataPartitions(jobInfo);
    final MaterializationMetrics metrics =
        ReflectionUtils.computeMetrics(
            job, jobsService, allocator, JobsProtoUtil.toStuff(job.getJobId()));
    final List<String> refreshPath =
        ReflectionUtils.getRefreshPath(
            JobsProtoUtil.toStuff(job.getJobId()),
            getAccelerationPlugin().getConfig().getPath(),
            jobsService,
            allocator);
    final boolean isIcebergRefresh =
        materialization.getIsIcebergDataset() != null && materialization.getIsIcebergDataset();
    final String icebergBasePath =
        ReflectionUtils.getIcebergReflectionBasePath(refreshPath, isIcebergRefresh);
    final Refresh refresh =
        ReflectionUtils.createRefresh(
            materialization.getReflectionId(),
            refreshPath,
            seriesId,
            0,
            new UpdateId(),
            jobDetails,
            metrics,
            dataPartitions,
            isIcebergRefresh,
            icebergBasePath);
    refresh.setCompacted(true);

    // no need to update entry lastSuccessfulRefresh, as it may only cause unnecessary refreshes on
    // dependant reflections

    materializationStore.save(materialization);
    materializationStore.save(refresh);

    // start a metadata refresh and delete compacted materialization, in parallel
    deleteCompactedMaterialization(entry);
    refreshMetadata(entry, materialization);
  }

  private void optimizeJobSucceeded(
      ReflectionEntry entry,
      Materialization materialization,
      com.dremio.service.job.JobDetails job) {
    // Submit a LOAD MATERIALIZATION job so the new snapshots can be used.
    final Materialization newMaterialization =
        materializationStore.getRunningMaterialization(entry.getId());

    UpdateId updateId = materializationStore.getMostRecentRefresh(entry.getId()).getUpdateId();
    final JobAttempt lastAttempt = JobsProtoUtil.getLastAttempt(job);
    final JobInfo jobInfo = lastAttempt.getInfo();
    final JobDetails jobDetails = ReflectionUtils.computeJobDetails(lastAttempt);
    List<Snapshot> snapshots =
        Lists.newArrayList(
            getIcebergTable(entry.getId(), materialization.getBasePath()).snapshots());
    List<Snapshot> optimizeSnapshots =
        snapshots.stream()
            .filter(s -> s.timestampMillis() >= jobInfo.getStartTime())
            .collect(Collectors.toList());
    MaterializationMetrics metrics;
    MaterializationMetrics oldMetrics = materializationStore.getMetrics(materialization).left;

    // OPTIMIZE creates up to two snapshots. The first does data compaction, and the second rewrites
    // manifest files.
    // It's possible that OPTIMIZE only creates one snapshot. Check the snapshots to get the metrics
    // from REWRITE DATA if it happened.
    if (!optimizeSnapshots.isEmpty()
        && optimizeSnapshots.get(0).operation().equals("replace")
        && optimizeSnapshots.get(0).summary() != null
        && optimizeSnapshots.get(0).summary().containsKey("added-data-files")) {
      // Get the metrics from the REWRITE DATA snapshot to calculate metrics.
      Map<String, String> optimizedMetrics = optimizeSnapshots.get(0).summary();
      Long footprint =
          oldMetrics.getFootprint()
              - Long.parseLong(optimizedMetrics.get("removed-files-size"))
              + Long.parseLong(optimizedMetrics.get("added-files-size"));
      int numFiles =
          oldMetrics.getNumFiles()
              - Integer.parseInt(optimizedMetrics.get("deleted-data-files"))
              + Integer.parseInt(optimizedMetrics.get("added-data-files"));
      // The below code finds the mean, instead of the median. the job doesn't give us individual
      // file sizes, so this is the closest thing we have.
      // TODO DX64438: medianFileSize is used by the old COMPACT MATERIALIZATION code, and will no
      // longer be needed after it is removed.
      Long medianFileSize = footprint / numFiles;
      metrics =
          new MaterializationMetrics()
              .setFootprint(footprint)
              .setOriginalCost(JobsProtoUtil.getLastAttempt(job).getInfo().getOriginalCost())
              .setMedianFileSize(medianFileSize)
              .setNumFiles(numFiles);
    } else {
      // Use the old metrics instead since OPTIMIZE didn't REWRITE DATA.
      metrics = oldMetrics;
    }

    final List<String> refreshPath =
        ImmutableList.of(
            ACCELERATOR_STORAGEPLUGIN_NAME,
            newMaterialization.getReflectionId().getId(),
            newMaterialization.getBasePath());
    final List<DataPartition> dataPartitions = computeDataPartitions(jobInfo);
    final Refresh refresh =
        ReflectionUtils.createRefresh(
            newMaterialization.getReflectionId(),
            refreshPath,
            newMaterialization.getSeriesId(),
            newMaterialization.getSeriesOrdinal(),
            updateId,
            jobDetails,
            metrics,
            dataPartitions,
            true,
            newMaterialization.getBasePath());
    refresh.setCompacted(true);
    materializationStore.save(refresh);

    refreshMetadata(entry, newMaterialization);
  }

  void optimizeJobCanceledOrFailed(
      ReflectionEntry entry, Materialization m, JobAttempt lastAttempt) {
    final Table table = getIcebergTable(m.getReflectionId(), m.getBasePath());

    // OPTIMIZE will create two commits if successful - one for REWRITE DATA, and one for REWRITE
    // MANIFESTS.
    // If either of these fail the job fails. If the job fails or is cancelled due to a concurrent
    // DML operation or other reason,
    // we should just rollback to the iceberg table to the snapshot of the materialization we called
    // OPTIMIZE on.
    // Delete the materialization that OPTIMIZE created.
    if (table.currentSnapshot().snapshotId() != m.getPreviousIcebergSnapshot()) {
      table.manageSnapshots().rollbackTo(m.getPreviousIcebergSnapshot()).commit();
    }

    entry.setState(ACTIVE);
    updateReflectionEntry(entry);

    // Mark the materialization created by OPTIMIZE as CANCELED or FAILED.
    final Materialization optimizedMaterialization =
        materializationStore.getRunningMaterialization(entry.getId());
    if (optimizedMaterialization != null) {
      if (lastAttempt.getState().equals(JobState.CANCELED)) {
        optimizedMaterialization.setState(MaterializationState.CANCELED);
      } else if (lastAttempt.getState().equals(JobState.FAILED)) {
        final String message =
            Optional.ofNullable(lastAttempt.getInfo().getFailureInfo())
                .orElse("OPTIMIZE TABLE job failed without reporting an error message");
        optimizedMaterialization
            .setState(MaterializationState.FAILED)
            .setFailure(new Failure().setMessage(message));
        entry.setLastFailure(new Failure().setMessage(message));
        updateReflectionEntry(entry);
      }
      materializationStore.save(optimizedMaterialization);
    }
  }

  private void deleteCompactedMaterialization(ReflectionEntry entry) {
    final Materialization compacted =
        materializationStore.getLastMaterializationCompacted(entry.getId());
    if (compacted == null) {
      logger.warn("Couldn't find any compacted materialization for {}", getId(entry));
      return;
    }

    deleteMaterialization(compacted);
  }

  private boolean shouldCompact(ReflectionEntry entry, Refresh refresh) {
    final long medianFileSize = refresh.getMetrics().getMedianFileSize();
    final int numFiles = refresh.getMetrics().getNumFiles();

    logger.debug(
        "Refresh {} wrote {} files with a median size of {} bytes",
        getId(entry),
        numFiles,
        medianFileSize);
    return numFiles > optionManager.getOption(COMPACTION_TRIGGER_NUMBER_FILES)
        && medianFileSize < optionManager.getOption(COMPACTION_TRIGGER_FILE_SIZE) * 1024 * 1024;
  }

  private void metadataRefreshJobSucceeded(ReflectionEntry entry, Materialization materialization) {

    try {
      descriptorCache.update(materialization);
      materialization = materializationStore.get(materialization.getId());
    } catch (Exception | AssertionError e) {
      logger.warn("Failed to update materialization cache for {}", getId(materialization), e);
      materialization
          .setState(MaterializationState.FAILED)
          .setFailure(
              new Failure()
                  .setMessage(
                      String.format("Materialization cache update failed: %s", e.getMessage())));
      entry.setLastFailure(
          new Failure()
              .setMessage(String.format("Materialization cache update failed: %s", e.getMessage()))
              .setStackTrace(Throwables.getStackTraceAsString(e)));
      updateReflectionEntry(entry);
    }

    if (materialization.getState() != MaterializationState.FAILED) {
      handleMaterializationDone(entry, materialization);
    }

    // we need to check the state of the materialization again because handleMaterializationDone()
    // can change its state
    if (materialization.getState() == MaterializationState.FAILED) {
      // materialization failed
      reportFailure(entry, ACTIVE);
    }

    materializationStore.save(materialization);
    updateReflectionEntry(entry);
    if (entry.getState().equals(ACTIVE)
        && materialization.getState().equals(MaterializationState.DONE)) {
      maybeOptimizeIncrementalReflectionFiles(entry, materialization);
    }
  }

  private void refreshMetadata(ReflectionEntry entry, Materialization materialization) {
    final String sql =
        String.format(
            "LOAD MATERIALIZATION METADATA \"%s\".\"%s\"",
            materialization.getReflectionId().getId(), materialization.getId().getId());

    final JobId jobId =
        submitRefreshJob(
            jobsService,
            catalogService,
            entry,
            materialization.getId(),
            sql,
            com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE,
            new WakeUpManagerWhenJobDone(wakeUpCallback, "metadata refresh job done"));

    entry.setState(METADATA_REFRESH).setRefreshJobId(jobId);
    updateReflectionEntry(entry);

    logger.debug(
        "Submitted LOAD MATERIALIZATION job {} for {}", jobId.getId(), getId(materialization));
  }

  private void startRefresh(ReflectionEntry entry) {
    final long jobSubmissionTime = System.currentTimeMillis();
    // we should always update lastSubmittedRefresh to avoid an immediate refresh if we fail to
    // start a refresh job
    entry.setLastSubmittedRefresh(jobSubmissionTime);

    if (DremioEdition.get() != DremioEdition.MARKETPLACE
        && sabotContext.getCoordinatorModeInfoProvider().get().isInSoftwareMode()) {
      if (sabotContext.getExecutors().isEmpty()
          && System.currentTimeMillis() - sabotContext.getEndpoint().getStartTime()
              < START_WAIT_MILLIS) {
        logger.warn("No executors available to refresh {}", getId(entry));
        reportFailure(entry, ACTIVE);
        entry.setLastFailure(
            new Failure().setMessage("No executors available to refresh reflection"));
        updateReflectionEntry(entry);
        return;
      }
    }

    try {

      final JobId refreshJobId =
          refreshStartHandler.startJob(entry, jobSubmissionTime, getIcebergSnapshot(entry));

      entry.setState(REFRESHING).setRefreshJobId(refreshJobId);
      updateReflectionEntry(entry);

    } catch (Exception | AssertionError e) {
      // we failed to start the refresh
      logger.warn("Failed to start REFRESH REFLECTION job for {}", getId(entry), e);
      // did we create a RUNNING materialization entry ?
      final Materialization m = materializationStore.getRunningMaterialization(entry.getId());
      if (m != null) {
        // yes. Let's make sure we mark it as FAILED
        m.setState(MaterializationState.FAILED)
            .setFailure(
                new Failure()
                    .setMessage(
                        String.format(
                            "Failed to start REFRESH REFLECTION job: %s", e.getMessage())));
        materializationStore.save(m);
      }
      reportFailure(entry, ACTIVE);
      entry.setLastFailure(
          new Failure()
              .setMessage(
                  String.format("Failed to start REFRESH REFLECTION job: %s", e.getMessage()))
              .setStackTrace(Throwables.getStackTraceAsString(e)));
      updateReflectionEntry(entry);
    }
  }

  /**
   * Get current iceberg snapshot corresponding to the reflection entry Return null if it is not an
   * iceberg reflection
   */
  private Long getIcebergSnapshot(ReflectionEntry entry) {
    Long icebergSnapshot = null;
    final FluentIterable<Refresh> refreshes =
        materializationStore.getRefreshesByReflectionId(entry.getId());
    if (refreshes != null && !refreshes.isEmpty()) {
      final Refresh latestRefresh = refreshes.get(refreshes.size() - 1);
      icebergSnapshot = getIcebergSnapshot(latestRefresh, entry.getId());
    }
    return icebergSnapshot;
  }

  /**
   * Get current iceberg snapshot corresponding to the reflection entry Return null if it is not an
   * iceberg reflection
   */
  public Long getIcebergSnapshot(Refresh refresh, ReflectionId reflectionId) {
    Long icebergSnapshot = null;
    if (refresh != null && refresh.getIsIcebergRefresh() != null && refresh.getIsIcebergRefresh()) {
      final Table table = getIcebergTable(reflectionId, refresh.getBasePath());
      icebergSnapshot = table.currentSnapshot().snapshotId();
    }
    return icebergSnapshot;
  }

  private void reportFailure(ReflectionEntry entry, ReflectionState newState) {
    if (entry.getDontGiveUp()) {
      logger.debug("Due to dontGiveUp, ignoring failure on {}", getId(entry));
      entry.setState(newState).setNumFailures(entry.getNumFailures() + 1);
      updateReflectionEntry(entry);
      return;
    }

    final int numFailures = entry.getNumFailures() + 1;
    final long failuresThreshold = optionManager.getOption(LAYOUT_REFRESH_MAX_ATTEMPTS);
    final boolean markAsFailed = numFailures >= failuresThreshold;
    entry.setNumFailures(numFailures).setState(markAsFailed ? FAILED : newState);
    updateReflectionEntry(entry);

    if (markAsFailed) {
      logger.debug("Max consecutive failures {} reached on {}", numFailures, getId(entry));
      // remove the reflection from the dependency manager to update the dependencies of all its
      // dependent reflections
      dependencyManager.delete(entry.getId());
    }
  }

  private ReflectionEntry create(ReflectionGoal goal) {
    logger.debug("Creating reflection entry for {}", getId(goal));
    // We currently store meta data in the Reflection Entry that possibly should not be there such
    // as boost
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

  public Table getIcebergTable(ReflectionId reflectionId, String basePath) {
    final AccelerationStoragePlugin accelerationPlugin = getAccelerationPlugin();
    return ReflectionUtils.getIcebergTable(reflectionId, basePath, accelerationPlugin);
  }

  /**
   * Dist path may be updated after coordinator startup so it's important to not cache the
   * accelerator storage plugin or its dist path.
   *
   * @return
   */
  private AccelerationStoragePlugin getAccelerationPlugin() {
    return catalogService.getSource(ACCELERATOR_STORAGEPLUGIN_NAME);
  }

  /**
   * Helper method that creates new entry in ReflectionEntriesStore and in MaterializationInfo cache
   * of DependencyManager.
   */
  void createReflectionEntry(ReflectionEntry entry) {
    reflectionStore.save(entry);
    dependencyManager.createMaterializationInfo(entry);
  }

  /**
   * Helper method that updates ReflectionEntry info in ReflectionEntriesStore and in
   * MaterializationInfo cache of DependencyManager.
   */
  void updateReflectionEntry(ReflectionEntry entry) {
    reflectionStore.save(entry);
    dependencyManager.updateMaterializationInfo(entry);
  }

  /**
   * Helper method that deletes entry from ReflectionEntriesStore and from MaterializationInfo cache
   * of DependencyManager.
   */
  void deleteReflectionEntry(ReflectionId id) {
    reflectionStore.delete(id);
    dependencyManager.deleteMaterializationInfo(id);
  }
}
