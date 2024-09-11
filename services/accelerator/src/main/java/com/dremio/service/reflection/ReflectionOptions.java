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

import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import java.util.concurrent.TimeUnit;

/** System options that affect the reflection service */
@Options
public interface ReflectionOptions {

  // how often should the reflection manager wakeup automatically without any external trigger
  PositiveLongValidator REFLECTION_MANAGER_REFRESH_DELAY_MILLIS =
      new PositiveLongValidator(
          "reflection.manager.refresh.delay_millis", Long.MAX_VALUE, TimeUnit.SECONDS.toMillis(10));
  // how long deleted reflection goals/materializations are kept in the store/on disk before they
  // are effectively deleted
  PositiveLongValidator REFLECTION_DELETION_GRACE_PERIOD =
      new PositiveLongValidator(
          "reflection.deletion.grace_seconds", Long.MAX_VALUE, TimeUnit.HOURS.toSeconds(4));
  // how many entries should be deleted every time the reflection manager wakes up
  PositiveLongValidator REFLECTION_DELETION_NUM_ENTRIES =
      new PositiveLongValidator("reflection.deletion.num_entries", Long.MAX_VALUE, 5);
  // how often should the materialization zombie check be done
  PositiveLongValidator MATERIALIZATION_ORPHAN_REFRESH =
      new PositiveLongValidator(
          "materialization.orphan.refresh_seconds", Long.MAX_VALUE, TimeUnit.HOURS.toSeconds(4));
  BooleanValidator MATERIALIZATION_CACHE_ENABLED =
      new BooleanValidator("dremio.materialization.cache.enabled", true);
  // how often should the materialization cache be refreshed
  PositiveLongValidator MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS =
      new PositiveLongValidator(
          "reflection.materialization.cache.refresh.delay_millis",
          Long.MAX_VALUE,
          TimeUnit.SECONDS.toMillis(30));
  // how long the planner can block waiting on materialization cache initialization before
  // continuing without reflections
  PositiveLongValidator MATERIALIZATION_CACHE_INIT_TIMEOUT_SECONDS =
      new PositiveLongValidator(
          "reflection.materialization.cache.init_timeout_seconds", Long.MAX_VALUE, 60);
  int MAX_RETRY_HOURS = 3;
  // how long materialization cache will keep retrying a materialization before marking it as FAILED
  PositiveLongValidator MATERIALIZATION_CACHE_RETRY_MINUTES =
      new PositiveLongValidator(
          "reflection.materialization.cache.retry.minutes",
          TimeUnit.HOURS.toMinutes(MAX_RETRY_HOURS),
          TimeUnit.MINUTES.toMinutes(60));
  // allows users to set sub-hour refresh and grace periods
  BooleanValidator ENABLE_SUBHOUR_POLICIES =
      new BooleanValidator("accelerator.enable.subhour.policies", false);
  // control how many voted datasets are promoted every 24 hours
  PositiveLongValidator MAX_AUTOMATIC_REFLECTIONS =
      new PositiveLongValidator("reflection.auto.max", Integer.MAX_VALUE, 10);
  // should the voting service create aggregation reflections
  BooleanValidator ENABLE_AUTOMATIC_AGG_REFLECTIONS =
      new BooleanValidator("reflection.auto.agg.enable", false);
  // should the voting service create raw reflections
  BooleanValidator ENABLE_AUTOMATIC_RAW_REFLECTIONS =
      new BooleanValidator("reflection.auto.raw.enable", false);
  // set to true to prevent external events from waking up the reflection manager
  BooleanValidator REFLECTION_PERIODIC_WAKEUP_ONLY =
      new BooleanValidator("reflection.manager.wakeup.periodic_only", false);
  BooleanValidator REFLECTION_ENABLE_SUBSTITUTION =
      new BooleanValidator("reflection.enable.substitutions", true);
  // if a reflection has no known dependencies how long should we wait before we attempt to refresh
  // again
  // this typically occurs when the refresh job fails in planning
  PositiveLongValidator NO_DEPENDENCY_REFRESH_PERIOD_SECONDS =
      new PositiveLongValidator(
          "reflection.no_dependency.refresh_period_seconds",
          Long.MAX_VALUE,
          TimeUnit.MINUTES.toSeconds(30));
  // should compaction be enabled
  BooleanValidator ENABLE_COMPACTION = new BooleanValidator("reflection.compaction.enabled", false);
  // at least how many files there should be to trigger compaction
  PositiveLongValidator COMPACTION_TRIGGER_NUMBER_FILES =
      new PositiveLongValidator("reflection.compaction.trigger.num_files", Long.MAX_VALUE, 1);
  // Compaction will be triggered if the median file size is less than or equal to this parameter
  PositiveLongValidator COMPACTION_TRIGGER_FILE_SIZE =
      new PositiveLongValidator(
          "reflection.compaction.trigger.file_size_mb", Long.MAX_VALUE / (1024 * 1024), 16);
  // Enable caching of reflection whose dist storage is in cloud ( S3, AzureDataLake,
  // AzureFileSystem)
  BooleanValidator CLOUD_CACHING_ENABLED =
      new BooleanValidator("reflection.cloud.cache.enabled", true);
  StringValidator NESSIE_REFLECTIONS_NAMESPACE =
      new StringValidator("reflection.manager.nessie_iceberg_namespace", "dremio.reflections");
  BooleanValidator AUTO_REBUILD_PLAN =
      new BooleanValidator("reflection.manager.auto_plan_rebuild", true);
  // should reflection settings and refresh cache be enabled during reflection manager syncs
  BooleanValidator REFLECTION_MANAGER_SYNC_CACHE =
      new BooleanValidator("reflection.manager.sync.cache.enabled", true);
  // Allow default raw reflections to be used in REFRESH REFLECTION jobs
  BooleanValidator ACCELERATION_ENABLE_DEFAULT_RAW_REFRESH =
      new BooleanValidator("accelerator.enable_default_raw_reflection_refresh", true);
  // should incrementally refreshed default raw reflections containing filters/aggs be used
  BooleanValidator ENABLE_INCREMENTAL_DEFAULT_RAW_REFLECTIONS_WITH_AGGS =
      new BooleanValidator("reflection.manager.enable_incremental_default_raw_with_aggs", true);
  // should OPTIMIZE TABLE be run on incrementally refreshed reflections
  BooleanValidator ENABLE_OPTIMIZE_TABLE_FOR_INCREMENTAL_REFLECTIONS =
      new BooleanValidator(
          "reflection.manager.enable_optimize_table_for_incremental_reflections", true);
  // number of refreshes between OPTIMIZE runs
  PositiveLongValidator OPTIMIZE_REFLECTION_REQUIRED_REFRESHES_BETWEEN_RUNS =
      new PositiveLongValidator(
          "reflection.manager.optimize_refreshes_between_runs", Integer.MAX_VALUE, 5);
  // Enable snapshot based incremental as default refresh method for iceberg tables
  BooleanValidator REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED =
      new BooleanValidator("reflection.iceberg.snapshot_based_incremental.enabled", true);
  // Enable Snapshot Based Incremental Refresh by Partition for reflections
  BooleanValidator INCREMENTAL_REFRESH_BY_PARTITION =
      new BooleanValidator("reflection.enable_incremental_refresh_by_partition", true);
  // Enable snapshot based incremental as default refresh method for Unlimited Splits tables
  BooleanValidator REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL =
      new BooleanValidator("reflection.unlimited_splits.snapshot_based_incremental", true);
  // Maximum number of UNIONS allowed in snapshot based incremental refresh plan (due to optimize
  // commands on base table)
  PositiveLongValidator REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS =
      new PositiveLongValidator(
          "reflection.snapshot_based_incremental.max_unions", Long.MAX_VALUE, 50);
  // Enable automatic VACUUM jobs on incremental reflections
  BooleanValidator ENABLE_VACUUM_FOR_INCREMENTAL_REFLECTIONS =
      new BooleanValidator(
          "reflection.manager.enable_vacuum_table_for_incremental_reflections", true);
  // Enable REFRESH_PENDING state for a reflection when its direct or indirect reflection
  // dependencies are due for refresh or refreshing.
  BooleanValidator REFLECTION_MANAGER_REFRESH_PENDING_ENABLED =
      new BooleanValidator("reflection.manager.refresh_pending.enabled", true);
  // Timeout in minutes for REFRESH_PENDING state. If a reflection has been in REFRESH_PENDING state
  // longer than this period it will be forced to refresh.
  PositiveLongValidator REFLECTION_MANAGER_REFRESH_PENDING_TIMEOUT_MINUTES =
      new PositiveLongValidator("reflection.manager.refresh_pending.timeout", Long.MAX_VALUE, 30);
  // Enable using schedule policies on datasets and sources for reflection refresh
  BooleanValidator REFLECTION_SCHEDULE_POLICY_ENABLED =
      new BooleanValidator("reflection.manager.refresh_schedule_policy.enabled", true);
  // Enable LOAD MATERIALIZATION job to register metadata as an async process,
  // or register table synchronously in handleMaterializationDone when the flag is off
  BooleanValidator LOAD_MATERIALIZATION_JOB_ENABLED =
      new BooleanValidator("reflection.manager.load_materialization_job.enabled", false);
  // Avoid redundant refreshes using table snapshots
  BooleanValidator REFLECTION_MANAGER_AVOID_REDUNDANT_REFRESH =
      new BooleanValidator("reflection.manager.avoid_redundant_refresh", true);
  // Skip the async materialization deletion process of submitting a DROP TABLE job to clean up
  // records and physical data for incremental refresh.
  BooleanValidator SKIP_DROP_TABLE_JOB_FOR_INCREMENTAL_REFRESH =
      new BooleanValidator("reflection.manager.skip_drop_table_job_for_incremental_refresh", true);
  // The new code path in ReflectionSuggester and ReflectionAnalyzer allows us to suggest
  // reflections base on the reflection type user provides.
  BooleanValidator SUGGEST_REFLECTION_BASED_ON_TYPE =
      new BooleanValidator("reflection.manager.suggest_reflection_based_on_type", true);
  // Retry using retry backoff by default. Immediate retry when the support key is off.
  BooleanValidator ENABLE_EXPONENTIAL_BACKOFF_FOR_RETRY_POLICY =
      new BooleanValidator("reflection.manager.enable_exponential_backoff_for_retry_policy", true);
  // Backoff retry policy as a feature.
  BooleanValidator BACKOFF_RETRY_POLICY =
      new BooleanValidator("reflection.manager.backoff_retry_policy", true);
  // Enable the "reflection.max_num_reflections_limit" support key
  BooleanValidator MAX_NUM_REFLECTIONS_LIMIT_ENABLED =
      new BooleanValidator("reflection.max_num_reflections_limit.enabled", false);
  // The maximum number of reflections allowed by Reflection Manager.
  LongValidator MAX_NUM_REFLECTIONS_LIMIT =
      new RangeLongValidator("reflection.max_num_reflections_limit", 0L, Long.MAX_VALUE, 500);
}
