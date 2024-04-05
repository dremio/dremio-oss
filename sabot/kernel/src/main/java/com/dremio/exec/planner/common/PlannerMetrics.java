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

import com.google.common.base.Joiner;
import io.micrometer.core.instrument.Meter;

public class PlannerMetrics {

  // Shared prefix used by all planner metrics
  public static final String PREFIX = "planner";

  // Shared prefix used by job metrics
  public static final String PREFIX_JOBS = "jobs";

  // Metric names
  public static final String VIEW_SCHEMA_LEARNING = "view_schema_learning";
  public static final String JOB_CANCELLED = "cancelled";

  public static final String PLAN_CACHE_SYNC = "plan_cache_sync";
  public static final String PLAN_CACHE_ENTRIES = "plan_cache_entries";

  // Metric tags
  private static final String TAG_OUTCOME = "outcome";
  public static final String TAG_REASON = "reason";

  public static String createName(String prefix, String suffix) {
    return Joiner.on(".").join(prefix, suffix);
  }

  public static <T extends Meter> T withOutcome(Meter.MeterProvider<T> p, boolean success) {
    return p.withTag(TAG_OUTCOME, success ? "success" : "error");
  }
}
