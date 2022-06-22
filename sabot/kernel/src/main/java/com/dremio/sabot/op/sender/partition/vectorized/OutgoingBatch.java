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
package com.dremio.sabot.op.sender.partition.vectorized;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Manages one outgoing receiver:<br>
 * Copy is done in 2 consecutive batches (A and B) so that we can delay flushing
 * until the end of a copy pass.
 * Handles batch flushing and vector allocations
 */
public class OutgoingBatch extends VectorContainer {
  private final AccountingExecTunnel tunnel;
  private final HashPartitionSender config;
  private final OperatorContext context;
  private final int oppositeMinorFragmentId;

  private final OperatorStats stats;

  // we need these to set the lastSet value for variable length vectors
  private final List<VarCharVector> varchars = Lists.newArrayList();
  private final List<VarBinaryVector> varbins = Lists.newArrayList();

  private final int batchIdx;
  private final int nextBatchIdx;
  private final int maxRecords;
  private boolean firstTimeAllocDone;

  /** tracks how many rows will be copied in current pass */
  private int preCopyIdx;
  /** true if receiver finished */
  private volatile boolean dropAll;

  OutgoingBatch(int batchIdx, int nextBatchIdx, int maxRecords, final VectorAccessible incoming,
                BufferAllocator allocator, AccountingExecTunnel tunnel, HashPartitionSender config,
                OperatorContext context, int oppositeMinorFragmentId, OperatorStats stats) {
    Preconditions.checkArgument(maxRecords <= Character.MAX_VALUE, "maxRecords cannot exceed " + Character.MAX_VALUE);
    this.batchIdx = batchIdx;
    this.nextBatchIdx = nextBatchIdx;
    this.maxRecords = maxRecords;

    this.tunnel = tunnel;
    this.config = config;
    this.context = context;
    this.oppositeMinorFragmentId = oppositeMinorFragmentId;

    this.stats = stats;

    for (VectorWrapper<?> v : incoming) {
      ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), allocator);
      outgoingVector.setInitialCapacity(maxRecords);
      add(outgoingVector);

      if (outgoingVector instanceof VarBinaryVector) {
        varbins.add(((VarBinaryVector) outgoingVector));
      } else if (outgoingVector instanceof VarCharVector) {
        varchars.add(((VarCharVector) outgoingVector));
      }
    }
  }

  int getNextBatchIdx() {
    return nextBatchIdx;
  }

  int getBatchIdx() {
    return batchIdx;
  }

  List<FieldVector> getFieldVectors() {
    return VectorContainer.getFieldVectors(this);
  }

  FieldVector getFieldVector(int fieldId) {
    return (FieldVector) wrappers.get(fieldId).getValueVector();
  }

  @Override
  public void allocateNew() {
    firstTimeAllocDone = true;
    super.allocateNew();
  }

  boolean isFirstTimeAllocDone() {
    return firstTimeAllocDone;
  }

  boolean isFull() {
    return preCopyIdx == maxRecords;
  }

  /**
   * Accounts for one row copy and returns (batchIdx, rowIdx) of the copy destination
   *
   * @return compound index ((batchIdx << 16) | rowIdx)
   */
  int preCopyRow() {
    assert preCopyIdx >= 0 && preCopyIdx < maxRecords : "invalid preCopyIdx: " + preCopyIdx;

    preCopyIdx++;
    return (batchIdx << 16) | (preCopyIdx-1);
  }

  /**
   * Receiver finished. Do not send anymore batches
   */
  void terminate() {
    dropAll = true;
  }

  public void flush() {
    if (dropAll) {
      // If we are in dropAll mode, we still want to copy the data, because we can't stop copying a single outgoing
      // batch with out stopping all outgoing batches. Other option is check for status of dropAll before copying
      // every single record in copy method which has the overhead for every record all the time. Resetting the output
      // count, reusing the same buffers and copying has overhead only for outgoing batches whose receiver has
      // terminated.
      preCopyIdx = 0;
      return;
    }

    if (preCopyIdx == 0) {
      return; // empty batch
    }

    stats.addLongStat(Metric.NUM_FLUSHES, 1);

    for (VarBinaryVector vector : varbins) {
      vector.setLastSet(preCopyIdx);
    }
    for (VarCharVector vector : varchars) {
      vector.setLastSet(preCopyIdx);
    }
    setAllCount(preCopyIdx);
    if (!hasSchema()) {
      buildSchema();
    }

    final ExecProtos.FragmentHandle handle = context.getFragmentHandle();
    FragmentWritableBatch writableBatch = FragmentWritableBatch.create(
      handle.getQueryId(),
      handle.getMajorFragmentId(),
      handle.getMinorFragmentId(),
      config.getReceiverMajorFragmentId(),
      this,
      oppositeMinorFragmentId);

    updateStats(writableBatch);

    stats.startWait();
    tunnel.sendRecordBatch(writableBatch);
    stats.stopWait();

    preCopyIdx = 0;
  }

  private void updateStats(FragmentWritableBatch writableBatch) {
    stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
    stats.addLongStat(Metric.BATCHES_SENT, 1);
    stats.addLongStat(Metric.RECORDS_SENT, writableBatch.getRecordCount());
  }
}
