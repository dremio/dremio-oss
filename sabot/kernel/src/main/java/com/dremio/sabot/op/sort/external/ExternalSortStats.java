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
package com.dremio.sabot.op.sort.external;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.MetricDef.AggregationType;
import com.dremio.exec.proto.UserBitShared.MetricDef.DisplayType;
import com.dremio.sabot.exec.context.MetricDef;

public class ExternalSortStats {

  public enum Metric implements MetricDef {
    SPILL_COUNT(
        DisplayType.DISPLAY_BY_DEFAULT,
        AggregationType.SUM,
        "Number of times operator spilled to disk"),
    MERGE_COUNT, // number of times spills were merged
    PEAK_BATCHES_IN_MEMORY, // maximum number of batches kept in memory
    MAX_BATCH_SIZE, // maximum size of batch spilled amongst all batches spilled from all disk runs
    AVG_BATCH_SIZE, // average size of batch spilled amongst all batches spilled from all disk runs
    SPILL_TIME_NANOS(
        DisplayType.DISPLAY_BY_DEFAULT,
        AggregationType.SUM,
        "Time spent spilling to diskRuns while sorting"),
    MERGE_TIME_NANOS(
        DisplayType.DISPLAY_BY_DEFAULT,
        AggregationType.SUM,
        "Time spent merging disk runs and spilling"),
    TOTAL_SPILLED_DATA_SIZE(
        DisplayType.DISPLAY_BY_DEFAULT, AggregationType.SUM, "Total data spilled by sort operator"),
    BATCHES_SPILLED(
        DisplayType.DISPLAY_BY_DEFAULT, AggregationType.SUM, "Total batches spilled to disk"),

    // OOB related metrics
    OOB_SENDS, // Number of times operator informed others of spilling
    OOB_RECEIVES, // Number of times operator received a notification of spilling.
    OOB_DROP_LOCAL, // Number of times operator dropped self-referencing spilling notification
    OOB_DROP_WRONG_STATE, // Number of times operator dropped spilling notification as it was in
    // wrong state to spill
    OOB_DROP_UNDER_THRESHOLD, // Number of times OOB dropped spilling notification as it was under
    // the threshold.
    OOB_SPILL, // Spill was done due to oob.

    UNCOMPRESSED_BYTES_WRITTEN,
    IO_BYTES_WRITTEN,
    UNCOMPRESSED_BYTES_READ,
    IO_BYTES_READ,
    COMPRESSION_NANOS,
    DECOMPRESSION_NANOS,
    IO_WRITE_WAIT_NANOS,
    IO_READ_WAIT_NANOS,

    SPILL_COPY_NANOS,

    OOM_ALLOCATE_COUNT,
    OOM_COPY_COUNT,
    CAN_CONSUME_MILLIS,
    CAN_PRODUCE_MILLIS,
    SETUP_MILLIS,
    NO_MORE_TO_CONSUME_MILLIS;

    private final UserBitShared.MetricDef.DisplayType displayType;
    private final UserBitShared.MetricDef.AggregationType aggregationType;
    private final String displayCode;

    Metric() {
      this(
          UserBitShared.MetricDef.DisplayType.DISPLAY_NEVER,
          UserBitShared.MetricDef.AggregationType.SUM,
          "");
    }

    Metric(
        UserBitShared.MetricDef.DisplayType displayType,
        UserBitShared.MetricDef.AggregationType aggregationType,
        String displayCode) {
      this.displayType = displayType;
      this.aggregationType = aggregationType;
      this.displayCode = displayCode;
    }

    @Override
    public int metricId() {
      return ordinal();
    }

    @Override
    public UserBitShared.MetricDef.DisplayType getDisplayType() {
      return this.displayType;
    }

    @Override
    public UserBitShared.MetricDef.AggregationType getAggregationType() {
      return this.aggregationType;
    }

    @Override
    public String getDisplayCode() {
      return this.displayCode;
    }
  }
}
