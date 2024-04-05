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
  public static final String RM_UNKNOWN = "unknown";
  public static final String RM_FAILED = "failed";
  public static final String RM_ACTIVE = "active";
  public static final String RM_REFRESHING = "refreshing";

  // Metric names
  public static final String MAT_CACHE_SYNC = "materialization_cache_sync";
  public static final String MAT_CACHE_ENTRIES = "materialization_cache_entries";
  public static final String MAT_CACHE_ERRORS = "materialization_cache_errors";
  public static final String RM_SYNC = "manager_sync";

  // Metric tags
  public static final String TAG_MAT_CACHE_INITIAL = "initial";
  public static final String TAG_SOURCE_DOWN = "source_down";

  public static String createName(String suffix) {
    return PlannerMetrics.createName(PREFIX, suffix);
  }
}
