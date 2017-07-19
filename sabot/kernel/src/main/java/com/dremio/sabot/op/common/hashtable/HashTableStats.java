/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.common.hashtable;

import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorStats;

public class HashTableStats {
  public int numBuckets;
  public int numEntries;
  public int numResizing;
  public long resizingTime;

  private final OperatorStats stats;

  public HashTableStats(OperatorStats stats) {
    this.stats = stats;
  }

  public void update(HashTable table){
    if (table == null) {
      return;
    }
    table.getStats(this);
    stats.setLongStat(Metric.NUM_BUCKETS, numBuckets);
    stats.setLongStat(Metric.NUM_ENTRIES, numEntries);
    stats.setLongStat(Metric.NUM_RESIZING, numResizing);
    stats.setLongStat(Metric.RESIZING_TIME_NANOS, resizingTime);
  }

  public static enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_NANOS,
    PIVOT_TIME_NANOS,
    INSERT_TIME_NANOS,
    ACCUMULATE_TIME_NANOS,
    REVERSE_TIME_NANOS,
    UNPIVOT_TIME_NANOS,
    VECTORIZED,
    PROBE_PIVOT_NANOS,
    PROBE_MATCH_NANOS,
    PROBE_LIST_NANOS,
    PROBE_FIND_NANOS,
    PROBE_COPY_NANOS,
    BUILD_COPY_NANOS,
    BUILD_COPY_NOMATCH_NANOS,
    LINK_TIME_NANOS
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

}


