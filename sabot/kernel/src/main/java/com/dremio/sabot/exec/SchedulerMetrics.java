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
package com.dremio.sabot.exec;

import com.dremio.telemetry.api.metrics.MeterProviders;
import com.dremio.telemetry.api.metrics.SimpleCounter;
import com.google.common.base.Joiner;

public class SchedulerMetrics {
  public static String SCHEDULER_METRIC_PREFIX = "scheduler";
  private static volatile SimpleCounter longSlicesCounter;

  public static void registerActiveFragmentsCurrentCount(FragmentExecutors fragmentExecutors) {
    MeterProviders.newGauge(
        Joiner.on(".").join(SCHEDULER_METRIC_PREFIX, "active_fragments_current"),
        "Current number of active fragments",
        fragmentExecutors::size);
  }

  public static SimpleCounter getLongSlicesCounter() {
    if (longSlicesCounter == null) {
      synchronized (SchedulerMetrics.class) {
        if (longSlicesCounter != null) {
          return longSlicesCounter;
        }
        longSlicesCounter =
            SimpleCounter.of(
                Joiner.on(".").join(SCHEDULER_METRIC_PREFIX, "long_slices"),
                "Number of slices that ran twice the slice time");
      }
    }
    return longSlicesCounter;
  }
}
