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
package com.dremio.sabot.op.metrics;

import com.dremio.sabot.exec.context.MetricDef;

public class MongoStats {
  public enum Metric implements MetricDef {
    TOTAL_RECORDS_READ,
    NUM_LOCAL_RECORDS_READ,
    NUM_REMOTE_RECORDS_READ,
    TOTAL_BYTES_READ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }
}
