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

/**
 * Partition of a hash-join.
 */
public interface Partition extends AutoCloseable {
  int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

  /**
   * Handle pivoted (only keys are pivoted) records of a build batch.
   *
   * @param records number of records
   * @throws Exception
   */
  void buildPivoted(int records) throws Exception;

  /**
   * Check if the build side table is empty.
   *
   * @return true if empty.
   */
  boolean isBuildSideEmpty();

  /**
   * Handle pivoted records (only keys are pivoted) of a probe batch, and optionally, produce output records.
   *
   * @param records number of records in incoming batch
   * @param startOutputIndex start index in output batch
   * @param maxOutputIndex max index in output batch
   * @return number of records written to output. If -ve, it means this batch needs to be processed some more.
   * @throws Exception
   */
  int probePivoted(int records, int startOutputIndex, int maxOutputIndex) throws Exception;

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
   * Get stats.
   *
   * @return stats.
   */
  Stats getStats();

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
  }
}
