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


import java.util.concurrent.TimeUnit;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * System options that affect the reflection service
 */
@Options
public interface ReflectionOptions {

  // how often should the reflection manager wakeup automatically without any external trigger
  PositiveLongValidator REFLECTION_MANAGER_REFRESH_DELAY_MILLIS = new PositiveLongValidator("reflection.manager.refresh.delay_millis", Long.MAX_VALUE, TimeUnit.SECONDS.toMillis(10));
  // how long deleted reflection goals/materializations are kept in the store/on disk before they are effectively deleted
  PositiveLongValidator REFLECTION_DELETION_GRACE_PERIOD = new PositiveLongValidator("reflection.deletion.grace_seconds", Long.MAX_VALUE, TimeUnit.HOURS.toSeconds(4));
  // how many entries should be deleted every time the reflection manager wakes up
  PositiveLongValidator REFLECTION_DELETION_NUM_ENTRIES = new PositiveLongValidator("reflection.deletion.num_entries", Long.MAX_VALUE, 5);
  BooleanValidator MATERIALIZATION_CACHE_ENABLED = new BooleanValidator("dremio.materialization.cache.enabled", true);
  // how often should the materialization cache be refreshed
  PositiveLongValidator MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS = new PositiveLongValidator("reflection.materialization.cache.refresh.delay_millis", Long.MAX_VALUE, TimeUnit.SECONDS.toMillis(30));
  // allows users to set sub-hour refresh and grace periods
  BooleanValidator ENABLE_SUBHOUR_POLICIES = new BooleanValidator("accelerator.enable.subhour.policies", false);
  // control how many voted datasets are promoted every 24 hours
  PositiveLongValidator MAX_AUTOMATIC_REFLECTIONS = new PositiveLongValidator("reflection.auto.max", Integer.MAX_VALUE, 10);
  // should the voting service create aggregation reflections
  BooleanValidator ENABLE_AUTOMATIC_AGG_REFLECTIONS = new BooleanValidator("reflection.auto.agg.enable", false);
  // should the voting service create raw reflections
  BooleanValidator ENABLE_AUTOMATIC_RAW_REFLECTIONS = new BooleanValidator("reflection.auto.raw.enable", false);
  // set to true to prevent external events from waking up the reflection manager
  BooleanValidator REFLECTION_PERIODIC_WAKEUP_ONLY = new BooleanValidator("reflection.manager.wakeup.periodic_only", false);
  BooleanValidator REFLECTION_ENABLE_SUBSTITUTION = new BooleanValidator("reflection.enable.substitutions", true);
  // if a reflection has no known dependencies how long should we wait before we attempt to refresh again
  PositiveLongValidator NO_DEPENDENCY_REFRESH_PERIOD_SECONDS = new PositiveLongValidator("reflection.no_dependency.refresh_period_seconds", Long.MAX_VALUE, TimeUnit.MINUTES.toSeconds(30));
  // should compaction be enabled
  BooleanValidator ENABLE_COMPACTION = new BooleanValidator("reflection.compaction.enabled", false);
  // at least how many files there should be to trigger compaction
  PositiveLongValidator COMPACTION_TRIGGER_NUMBER_FILES = new PositiveLongValidator("reflection.compaction.trigger.num_files", Long.MAX_VALUE, 1);
  // Compaction will be triggered if the median file size is less than or equal to this parameter
  PositiveLongValidator COMPACTION_TRIGGER_FILE_SIZE = new PositiveLongValidator("reflection.compaction.trigger.file_size_mb", Long.MAX_VALUE/(1024*1024), 16);
  // Enable caching of reflection whose dist storage is in cloud ( S3, AzureDataLake, AzureFileSystem)
  BooleanValidator CLOUD_CACHING_ENABLED = new BooleanValidator("reflection.cloud.cache.enabled", true);
  // If disabled, only vds schema and expanded sql definition will be considered when deciding to do an incremental refresh
  BooleanValidator STRICT_INCREMENTAL_REFRESH = new BooleanValidator("reflection.manager.strict_incremental_refresh.enabled", false);
  // If enabled, uses Iceberg format for reflection datasets
  BooleanValidator REFLECTION_USE_ICEBERG_DATASET = new BooleanValidator("reflection.manager.use_iceberg_dataset.enabled", false);
}
