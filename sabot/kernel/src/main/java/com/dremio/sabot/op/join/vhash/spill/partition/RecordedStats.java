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

public class RecordedStats implements Partition.Stats {
  private final long buildNumEntries;
  private final long buildNumBuckets;
  private final long buildNumResizing;
  private final long buildResizingTimeNanos;
  private final long buildPivotTimeNanos;
  private final long buildHashComputationTimeNanos;
  private final long buildInsertTimeNanos;
  private final long buildLinkTimeNanos;
  private final long buildKeyCopyNanos;
  private final long buildCarryOverCopyNanos;
  private final long buildUnmatchedKeyCount;
  private final long buildCopyNonMatchNanos;
  private final long provePivotTimeNanos;
  private final long probeHashComputationTime;
  private final long probeFindTimeNanos;
  private final long probeListTimeNanos;
  private final long probeCopyNanos;
  private final long probeUnmatchedKeyCount;
  private final long evaluationCount;
  private final long evaluationMatchedCount;
  private final long evaluationSetupTimeNanos;

  RecordedStats() {
    buildNumEntries = 0;
    buildNumBuckets = 0;
    buildNumResizing = 0;
    buildResizingTimeNanos = 0;
    buildPivotTimeNanos = 0;
    buildHashComputationTimeNanos = 0;
    buildInsertTimeNanos = 0;
    buildLinkTimeNanos = 0;
    buildKeyCopyNanos = 0;
    buildCarryOverCopyNanos = 0;
    buildUnmatchedKeyCount = 0;
    buildCopyNonMatchNanos = 0;
    provePivotTimeNanos = 0;
    probeHashComputationTime = 0;
    probeFindTimeNanos = 0;
    probeListTimeNanos = 0;
    probeCopyNanos = 0;
    probeUnmatchedKeyCount = 0;
    evaluationCount = 0;
    evaluationMatchedCount = 0;
    evaluationSetupTimeNanos = 0;
  }

  RecordedStats(Partition.Stats save) {
    buildNumEntries = save.getBuildNumEntries();
    buildNumBuckets = save.getBuildNumBuckets();
    buildNumResizing = save.getBuildNumResizing();
    buildResizingTimeNanos = save.getBuildResizingTimeNanos();
    buildPivotTimeNanos = save.getBuildPivotTimeNanos();
    buildHashComputationTimeNanos = save.getBuildHashComputationTimeNanos();
    buildInsertTimeNanos = save.getBuildInsertTimeNanos();
    buildLinkTimeNanos = save.getBuildLinkTimeNanos();
    buildKeyCopyNanos = save.getBuildKeyCopyNanos();
    buildCarryOverCopyNanos = save.getBuildCarryOverCopyNanos();
    buildUnmatchedKeyCount = save.getBuildUnmatchedKeyCount();
    buildCopyNonMatchNanos = save.getBuildCopyNonMatchNanos();
    provePivotTimeNanos = save.getProbePivotTimeNanos();
    probeHashComputationTime = save.getProbeHashComputationTime();
    probeFindTimeNanos = save.getProbeFindTimeNanos();
    probeListTimeNanos = save.getProbeListTimeNanos();
    probeCopyNanos = save.getProbeCopyNanos();
    probeUnmatchedKeyCount = save.getProbeUnmatchedKeyCount();
    evaluationCount = save.getEvaluationCount();
    evaluationMatchedCount = save.getEvaluationMatchedCount();
    evaluationSetupTimeNanos = save.getSetupNanos();
  }

  @Override
  public long getBuildNumEntries() {
    return buildNumEntries;
  }

  @Override
  public long getBuildNumBuckets() {
    return buildNumBuckets;
  }

  @Override
  public long getBuildNumResizing() {
    return buildNumResizing;
  }

  @Override
  public long getBuildResizingTimeNanos() {
    return buildResizingTimeNanos;
  }

  @Override
  public long getBuildPivotTimeNanos() {
    return buildPivotTimeNanos;
  }

  @Override
  public long getBuildHashComputationTimeNanos() {
    return buildHashComputationTimeNanos;
  }

  @Override
  public long getBuildInsertTimeNanos() {
    return buildInsertTimeNanos;
  }

  @Override
  public long getBuildLinkTimeNanos() {
    return buildLinkTimeNanos;
  }

  @Override
  public long getBuildKeyCopyNanos() {
    return buildKeyCopyNanos;
  }

  @Override
  public long getBuildCarryOverCopyNanos() {
    return buildCarryOverCopyNanos;
  }

  @Override
  public long getBuildUnmatchedKeyCount() {
    return buildUnmatchedKeyCount;
  }

  @Override
  public long getBuildCopyNonMatchNanos() {
    return buildCopyNonMatchNanos;
  }

  @Override
  public long getProbePivotTimeNanos() {
    return provePivotTimeNanos;
  }

  @Override
  public long getProbeHashComputationTime() {
    return probeHashComputationTime;
  }

  @Override
  public long getProbeFindTimeNanos() {
    return probeFindTimeNanos;
  }

  @Override
  public long getProbeListTimeNanos() {
    return probeListTimeNanos;
  }

  @Override
  public long getProbeCopyNanos() {
    return probeCopyNanos;
  }

  @Override
  public long getProbeUnmatchedKeyCount() {
    return probeUnmatchedKeyCount;
  }

  @Override
  public long getEvaluationCount() {
    return evaluationCount;
  }

  @Override
  public long getEvaluationMatchedCount() {
    return evaluationMatchedCount;
  }

  @Override
  public long getSetupNanos() {
    return evaluationSetupTimeNanos;
  }
}
