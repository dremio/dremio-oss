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
package com.dremio.sabot.op.join.nlje;

import com.google.common.base.MoreObjects;
import java.util.Arrays;

/**
 * Encapsulates the state of output for a particular probe batch cartesian producted with a build
 * batch index.
 */
class IndexRange extends DualRange {

  private final int probeBatchSize;
  private final int[] buildBatchSizes;
  private final int buildBatchIndex;

  private final IntRange totalProbeRange;
  private final IntRange currentProbeRange;

  public IndexRange(
      IntRange totalProbeRange,
      IntRange probeRange,
      int probeBatchSize,
      int[] buildBatchSizes,
      int buildBatchIndex) {
    this.totalProbeRange = totalProbeRange;
    this.currentProbeRange = probeRange;
    this.probeBatchSize = probeBatchSize;
    this.buildBatchSizes = buildBatchSizes;
    this.buildBatchIndex = buildBatchIndex;
  }

  public IndexRange(int maxInterCount, int[] buildBatchSizes) {
    int maxBuildCount = Arrays.stream(buildBatchSizes).max().orElse(0);
    this.probeBatchSize = Math.min(1 << 16, (int) (1.0d * maxInterCount) / maxBuildCount);
    this.buildBatchIndex = 0;
    this.totalProbeRange = IntRange.of(0, buildBatchSizes[buildBatchIndex]);
    this.currentProbeRange = IntRange.of(0, Math.min(totalProbeRange.end, probeBatchSize));
    this.buildBatchSizes = buildBatchSizes;
  }

  @Override
  public boolean hasNext() {
    return hasRemainingProbe() || hasRemainingBuild();
  }

  private boolean hasRemainingBuild() {
    return buildBatchIndex + 1 < buildBatchSizes.length;
  }

  private boolean hasRemainingProbe() {
    return currentProbeRange.end < totalProbeRange.end;
  }

  @Override
  public boolean isEmpty() {
    return currentProbeRange.isEmpty();
  }

  @Override
  public IndexRange nextOutput() {
    if (hasRemainingProbe()) {
      final IntRange nextProbe =
          IntRange.of(
              currentProbeRange.end,
              Math.min(totalProbeRange.end, currentProbeRange.end + probeBatchSize));
      return new IndexRange(
          totalProbeRange, nextProbe, probeBatchSize, buildBatchSizes, buildBatchIndex);
    }

    if (hasRemainingBuild()) {
      int buildBatchIndex = this.buildBatchIndex + 1;

      // reset probe.
      final IntRange initialProbe =
          IntRange.of(0, Math.min(probeBatchSize, totalProbeRange.size()));
      return new IndexRange(
          totalProbeRange, initialProbe, probeBatchSize, buildBatchSizes, buildBatchIndex);
    }

    return new IndexRange(
        totalProbeRange, IntRange.EMPTY, probeBatchSize, buildBatchSizes, buildBatchIndex);
  }

  public int getProbeStart() {
    return currentProbeRange.start;
  }

  public int getProbeEnd() {
    return currentProbeRange.end;
  }

  public int getBuildBatchIndex() {
    return buildBatchIndex;
  }

  public int getBuildBatchCount() {
    return buildBatchSizes[buildBatchIndex];
  }

  public int getOutputStart() {
    return currentProbeRange.end;
  }

  @Override
  public IndexRange asIndexRange() {
    return this;
  }

  @Override
  public boolean isIndexRange() {
    return true;
  }

  @Override
  public IndexRange startNextProbe(int probeRecords) {
    return new IndexRange(
        IntRange.of(0, probeRecords), IntRange.EMPTY, probeBatchSize, buildBatchSizes, 0);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("currentProbeRange", currentProbeRange)
        .add("totalProbeRange", totalProbeRange)
        .add("buildBatchIndex", buildBatchIndex)
        .toString();
  }

  @Override
  public void close() {}
}
