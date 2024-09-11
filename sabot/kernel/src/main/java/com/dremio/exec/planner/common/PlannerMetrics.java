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
package com.dremio.exec.planner.common;

import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_KEY;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_VALUE_ERROR;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_VALUE_SUCCESS;

import com.google.common.base.Joiner;
import io.micrometer.core.instrument.Meter;

public class PlannerMetrics {

  // Shared prefix used by all planner metrics
  public static final String PREFIX = "planner";

  // Shared prefix used by job metrics
  public static final String PREFIX_JOBS = "jobs";

  // Metric names
  public static final String VIEW_SCHEMA_LEARNING = "view_schema_learning";
  public static final String JOB_FAILED = "failed";

  public static final String PLAN_CACHE_SYNC = "plan_cache_sync";
  public static final String PLAN_CACHE_ENTRIES = "plan_cache_entries";
  public static final String AMBIGUOUS_COLUMN = "ambiguous_column";
  // Metric tags
  public static final String TAG_REASON = "reason";
  public static final String ERROR_TYPE_KEY = "error_type";
  public static final String ERROR_ORIGIN_KEY = "error_origin";
  public static final String UNKNOWN_ERROR_TYPE = "UNKNOWN_ERROR_TYPE";
  public static final String CANCEL_USER_INITIATED = "CANCEL_USER_INITIATED";
  public static final String CANCEL_EXECUTION_RUNTIME_EXCEEDED =
      "CANCEL_EXECUTION_RUNTIME_EXCEEDED";
  public static final String CANCEL_RESOURCE_UNAVAILABLE = "CANCEL_RESOURCE_UNAVAILABLE";
  public static final String CANCEL_UNCLASSIFIED = "CANCEL_UNCLASSIFIED";
  public static final String CANCEL_HEAP_MONITOR = "CANCEL_HEAP_MONITOR";
  public static final String CANCEL_DIRECT_MEMORY_EXCEEDED = "CANCEL_DIRECT_MEMORY_EXCEEDED";
  public static final String COORDINATOR_CANCEL_HEAP_MONITOR =
      "COORD" + "INATOR_CANCEL_HEAP_MONITOR";
  public static final String COORDINATOR_CANCEL_DIRECT_MEMORY_EXCEEDED =
      "COORDINATOR_CANCEL_DIRECT_MEMORY_EXCEEDED";

  public static final String EXECUTOR_CANCEL_HEAP_MONITOR = "EXECUTOR_CANCEL_HEAP_MONITOR";
  public static final String EXECUTOR_CANCEL_DIRECT_MEMORY_EXCEEDED =
      "EXECUTOR_CANCEL_DIRECT_MEMORY_EXCEEDED";

  public static String createName(String prefix, String suffix) {
    return Joiner.on(".").join(prefix, suffix);
  }

  public static <T extends Meter> T withOutcome(Meter.MeterProvider<T> p, boolean success) {
    return p.withTag(
        TAG_OUTCOME_KEY, success ? TAG_OUTCOME_VALUE_SUCCESS : TAG_OUTCOME_VALUE_ERROR);
  }
}
