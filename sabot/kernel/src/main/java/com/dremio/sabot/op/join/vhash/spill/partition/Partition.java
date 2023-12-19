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
package com.dremio.sabot.op.join.vhash.spill.partition;


import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.google.common.base.Preconditions;

/**
 * Partition of a hash-join.
 */
public interface Partition extends AutoCloseable {
  int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

  /**
   * Handle pivoted (only keys are pivoted) records of a build batch.
   *
   * @param startIdx start index in the batch
   * @param records number of input records
   * @return number of records successfully inserted.
   * @throws Exception
   */
  int buildPivoted(int startIdx, int records) throws Exception;

  /**
   * Check if the build side table is empty.
   *
   * @return true if empty.
   */
  boolean isBuildSideEmpty();

  /**
   * Indicates begin of probe with a fresh batch of records.
   *
   * @param startIdx start index in the batch
   * @param records number of input records
   */
  void probeBatchBegin(int startIdx, int records);

  /**
   * Handle pivoted records (only keys are pivoted) of a probe batch, and optionally, produce output records.
   *
   * @param startOutputIndex start index in output batch
   * @param maxOutputIndex max index in output batch
   * @return number of records written to output. If -ve, it means this batch needs to be processed some more.
   * @throws Exception
   */
  int probePivoted(int startOutputIndex, int maxOutputIndex) throws Exception;

  /**
   * Output unmatched build records.
   *
   * @param startOutputIndex start index in output batch
   * @param maxOutputIndex max index in output batch
   * @return number of records written to output. If -ve, it means there are still some unmatched records.
   * @throws Exception
   */
  int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception;

  /**
   * Prepares bloomfilters for each probe target (field keys) in PartitionColFilters.
   * Since this is an optimisation, errors are not propagated to the consumer,
   * instead, they marked as an empty optional.
   *
   * @param partitionColFilters Previously created bloomfilters, one per probe target.
   */
  void prepareBloomFilters(PartitionColFilters partitionColFilters);

  /**
   * Prepares ValueListFilters for each probe target (and for each field for composite keys).
   * Since this is an optimisation, errors are not propagated to the consumer, instead they
   * are ignored.
   *
   * @param nonPartitionColFilters Previously created value list builders, one list per probe target.
   */
  void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters);

  /**
   * Get stats.
   *
   * @return stats.
   */
  Stats getStats();

  default int hashTableSize() {
    Preconditions.checkState(false);
    return 0;
  }

  /**
   * Reset the partition.
   */
  default void reset() {}

  interface Stats {
    long getBuildNumEntries();

    long getBuildNumBuckets();

    long getBuildNumResizing();

    long getBuildResizingTimeNanos();

    long getBuildPivotTimeNanos();

    long getBuildHashComputationTimeNanos();

    long getBuildInsertTimeNanos();

    long getBuildLinkTimeNanos();

    long getBuildKeyCopyNanos();

    long getBuildCarryOverCopyNanos();

    long getBuildUnmatchedKeyCount();

    long getBuildCopyNonMatchNanos();

    long getProbePivotTimeNanos();

    long getProbeHashComputationTime();

    long getProbeFindTimeNanos();

    long getProbeListTimeNanos();

    long getProbeCopyNanos();

    long getProbeUnmatchedKeyCount();

    long getEvaluationCount();

    long getEvaluationMatchedCount();

    long getSetupNanos();
  }
}
