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

import com.dremio.exec.planner.common.PlannerMetrics;

public class ReflectionMetrics {

  // Shared prefix used by all reflection metrics
  private static String PREFIX = "reflections";

  // v1 metric names
  // Gauges for reflection counts in each state
  public static final String RM_UNKNOWN = "unknown";
  // Counter for when a reflection is retrying or retry pending from failures
  public static final String RM_RETRYING = "retrying";
  public static final String RM_FAILED = "failed";
  public static final String RM_ACTIVE = "active";
  public static final String RM_REFRESHING = "refreshing";
  // Counter for when a reflection is deleted
  public static final String RM_DELETED = "deleted";

  // Metric names
  // Histogram for materialization cache sync time
  public static final String MAT_CACHE_SYNC = "materialization_cache_sync";
  // Gauge for entries in materialization cache
  public static final String MAT_CACHE_ENTRIES = "materialization_cache_entries";
  // Counter for any materialization cache error (may retry)
  public static final String MAT_CACHE_ERRORS = "materialization_cache_errors";
  // Counter for materialization cache retry failures (permanent failures only)
  public static final String MAT_CACHE_RETRY_FAILED = "materialization_cache_retry_failed";
  // Reflection manager sync histogram
  public static final String RM_SYNC = "manager_sync";

  // Metric tags
  // Whether the materialization cache error was on startup
  public static final String TAG_MAT_CACHE_INITIAL = "initial";
  // Whether the source was in a bad state
  public static final String TAG_SOURCE_DOWN = "source_down";
  // Whether the dataset is versioned
  public static final String TAG_VERSIONED = "versioned";

  public static String createName(String suffix) {
    return PlannerMetrics.createName(PREFIX, suffix);
  }
}
