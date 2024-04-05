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
package com.dremio.exec.physical.config;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class HashSenderCalculator {

  public static final BooleanValidator ENABLE_ADAPTIVE =
      new BooleanValidator("exec.partitioner.batch.adaptive", true);
  public static final PositiveLongValidator MAX_MEM_ALL =
      new PositiveLongValidator("exec.partitioner.mem.max", Integer.MAX_VALUE, 100 * 1024 * 1024);
  public static final PositiveLongValidator MAX_MEM_PER =
      new PositiveLongValidator("exec.partitioner.batch.size.max", Integer.MAX_VALUE, 1024 * 1024);

  public static BucketOptions captureBucketOptions(
      OptionManager options, LongValidator reserveValidator, BatchSchema schema) {
    return new BucketOptions(
        options.getOption(reserveValidator),
        options.getOption(ENABLE_ADAPTIVE),
        (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX),
        estimateRecordSize(options, schema),
        options.getOption(MAX_MEM_ALL),
        options.getOption(MAX_MEM_PER),
        (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN));
  }

  private static final int estimateRecordSize(OptionManager options, BatchSchema schema) {
    final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int varFieldSizeEstimate =
        (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize =
        schema.estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);
    return estimatedRecordSize;
  }

  public static class BucketOptions {

    private final long defaultReservation;
    private final boolean useAdaptiveSender;
    private final int configuredTargetRecordCount;
    private final int estimatedRecordSize;
    private final long maxBatchMemAll;
    private final long maxBatchMemEach;
    private final int minTargetBucketRecordCount;

    private BucketOptions(
        long defaultReservation,
        boolean useAdaptiveSender,
        int configuredTargetRecordCount,
        int estimatedRecordSize,
        long maxBatchMemAll,
        long maxBatchMemEach,
        int minTargetBucketRecordCount) {
      super();
      this.defaultReservation = defaultReservation;
      this.useAdaptiveSender = useAdaptiveSender;
      this.configuredTargetRecordCount = configuredTargetRecordCount;
      this.estimatedRecordSize = estimatedRecordSize;
      this.maxBatchMemAll = maxBatchMemAll;
      this.maxBatchMemEach = maxBatchMemEach;
      this.minTargetBucketRecordCount = minTargetBucketRecordCount;
    }

    /**
     * Calculate the target bucket size. If option <i>exec.partitioner.batch.adaptive</i> is set we
     * take a fixed value set in option (<i>exec.partitioner.batch.records</i>). Otherwise we
     * calculate the batch size based on record size.
     */
    public OpProps getResult(OpProps initialProps, int destCount) {

      if (!useAdaptiveSender) {
        return initialProps.cloneWithNewReserve(configuredTargetRecordCount * estimatedRecordSize);
      }

      final int targetOutgoingBatchSize =
          (int) Math.min(maxBatchMemEach, maxBatchMemAll / destCount);
      final int newBucketSize =
          Math.min(
              configuredTargetRecordCount,
              Math.max(targetOutgoingBatchSize / estimatedRecordSize, minTargetBucketRecordCount));
      final int quantizedBucketSize =
          Math.max(1, PhysicalPlanCreator.optimizeBatchSizeForAllocs(newBucketSize));
      long estimatedSingleBucketForAllDests =
          (long) estimatedRecordSize * quantizedBucketSize * destCount;
      long reserveSetting = Math.max(defaultReservation, estimatedSingleBucketForAllDests);
      return initialProps
          .cloneWithNewBatchSize(quantizedBucketSize)
          .cloneWithNewReserve(reserveSetting);
    }
  }
}
