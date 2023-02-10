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
package com.dremio.sabot.op.join.vhash.spill;

public class SpillStats {
  private long spillCount;
  private long heapSpillCount;
  private long replayCount;

  // write stats
  private long writeBuildBytes;
  private long writeBuildRecords;
  private long writeBuildBatches;
  private long writeProbeBytes;
  private long writeProbeRecords;
  private long writeProbeBatches;
  private long writeNanos;

  // read stats
  private long readBuildBytes;
  private long readBuildRecords;
  private long readBuildBatches;
  private long readBuildBatchesMerged;
  private long readProbeBytes;
  private long readProbeRecords;
  private long readProbeBatches;
  private long readProbeBatchesMerged;
  private long readNanos;
  private int oobSends;

  public long getSpillCount() {
    return spillCount;
  }

  public void incrementSpillCount() {
    spillCount++;
  }

  public long getHeapSpillCount() {
    return heapSpillCount;
  }

  public void incrementHeapSpillCount() {
    heapSpillCount++;
  }

  public void incrementOOBSends(){
    oobSends++;
  }

  public int getOOBSends(){
    return oobSends;
  }

  public long getReplayCount() {
    return replayCount;
  }

  public void incrementReplayCount() {
    replayCount++;
  }

  public long getWriteBuildBytes() {
    return writeBuildBytes;
  }

  public void addWriteBuildBytes(long writeBuildBytes) {
    this.writeBuildBytes += writeBuildBytes;
  }

  public long getWriteBuildRecords() {
    return writeBuildRecords;
  }

  public void addWriteBuildRecords(long writeBuildRecords) {
    this.writeBuildRecords += writeBuildRecords;
  }

  public long getWriteBuildBatches() {
    return writeBuildBatches;
  }

  public void addWriteBuildBatches(long writeBuildBatches) {
    this.writeBuildBatches += writeBuildBatches;
  }

  public long getWriteProbeBytes() {
    return writeProbeBytes;
  }

  public void addWriteProbeBytes(long writeProbeBytes) {
    this.writeProbeBytes += writeProbeBytes;
  }

  public long getWriteProbeRecords() {
    return writeProbeRecords;
  }

  public void addWriteProbeRecords(long writeProbeRecords) {
    this.writeProbeRecords += writeProbeRecords;
  }

  public long getWriteProbeBatches() {
    return writeProbeBatches;
  }

  public void addWriteProbeBatches(long writeProbeBatches) {
    this.writeProbeBatches += writeProbeBatches;
  }

  public long getWriteNanos() {
    return writeNanos;
  }

  public void addWriteNanos(long writeNanos) {
    this.writeNanos += writeNanos;
  }

  public long getReadBuildBytes() {
    return readBuildBytes;
  }

  public void addReadBuildBytes(long readBuildBytes) {
    this.readBuildBytes += readBuildBytes;
  }

  public long getReadBuildRecords() {
    return readBuildRecords;
  }

  public void addReadBuildRecords(long readBuildRecords) {
    this.readBuildRecords += readBuildRecords;
  }

  public long getReadBuildBatches() {
    return readBuildBatches;
  }

  public void addReadBuildBatches(long readBuildBatches) {
    this.readBuildBatches += readBuildBatches;
  }

  public long getReadBuildBatchesMerged() {
    return readBuildBatchesMerged;
  }

  public void addReadBuildBatchesMerged(long readBuildBatchesMerged) {
    this.readBuildBatchesMerged += readBuildBatchesMerged;
  }

  public long getReadProbeBytes() {
    return readProbeBytes;
  }

  public void addReadProbeBytes(long readProbeBytes) {
    this.readProbeBytes += readProbeBytes;
  }

  public long getReadProbeRecords() {
    return readProbeRecords;
  }

  public void addReadProbeRecords(long readProbeRecords) {
    this.readProbeRecords += readProbeRecords;
  }

  public long getReadProbeBatches() {
    return readProbeBatches;
  }

  public void addReadProbeBatches(long readProbeBatches) {
    this.readProbeBatches += readProbeBatches;
  }

  public long getReadProbeBatchesMerged() {
    return readProbeBatchesMerged;
  }

  public void addReadProbeBatchesMerged(long readProbeBatchesMerged) {
    this.readProbeBatchesMerged += readProbeBatchesMerged;
  }

  public long getReadNanos() {
    return readNanos;
  }

  public void addReadNanos(long readNanos) {
    this.readNanos += readNanos;
  }
}
