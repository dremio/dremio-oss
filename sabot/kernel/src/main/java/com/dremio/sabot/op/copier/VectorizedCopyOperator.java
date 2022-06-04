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
package com.dremio.sabot.op.copier;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.SelectionVectorRemover;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.copier.FieldBufferCopier.Cursor;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;

public class VectorizedCopyOperator implements SingleInputOperator {

  private final OperatorContext context;

  private State state = State.NEEDS_SETUP;

  private VectorAccessible incoming;
  private VectorContainer output;
  private VectorContainer buffered;
  private SelectionVector2 sv2;
  private List<TransferPair> inToOutTransferPairs = new ArrayList<>();
  private List<TransferPair> bufferedToOutTransferPairs = new ArrayList<>();
  private boolean straightCopy;
  private List<FieldBufferCopier> copiers = new ArrayList<>();
  private Cursor[] cursors;
  // incoming has been consumed upto this index.
  private int incomingIndex;
  // buffered container has these many records.
  private int bufferedIndex;
  private boolean shouldBufferOutput;
  private boolean noMoreToConsume;

  // random vector to determine if no copy is needed.
  // could be null if no columns are projected from incoming.
  private ValueVector randomVector;

  public VectorizedCopyOperator(OperatorContext context, SelectionVectorRemover popConfig) throws OutOfMemoryException {
    this.context = context;
    this.incomingIndex = 0;
    this.bufferedIndex = 0;
    this.shouldBufferOutput = true;
    this.noMoreToConsume = false;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) {
    state.is(State.NEEDS_SETUP);

    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.FOUR_BYTE);
    this.straightCopy = incoming.getSchema() == null || incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE;
    this.incoming = incoming;
    this.output = context.createOutputVectorContainer(incoming.getSchema());
    this.output.buildSchema(SelectionVectorMode.NONE);
    this.buffered = context.createOutputVectorContainer(incoming.getSchema());
    this.buffered.buildSchema(SelectionVectorMode.NONE);

    this.sv2 = straightCopy ? null : incoming.getSelectionVector2();

    // set up transfer pairs from incoming to output
    for (VectorWrapper<?> vv : incoming) {
      TransferPair tp = vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
      inToOutTransferPairs.add(tp);
      randomVector = vv.getValueVector();
    }

    // set up transfer pairs from buffered to output
    for (VectorWrapper<?> vv : buffered) {
      TransferPair tp = vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
      bufferedToOutTransferPairs.add(tp);
    }

    // setup copiers from incoming to buffered.
    copiers = CopierFactory.getInstance(context.getConfig(), context.getOptions())
              .getTwoByteCopiers(VectorContainer.getFieldVectors(incoming), VectorContainer.getFieldVectors(buffered));
    if (straightCopy || randomVector == null) {
      shouldBufferOutput = false;
    }
    cursors = new Cursor[copiers.size()];
    resetCursors();

    state = State.CAN_CONSUME;

    return output;
  }

  private void resetCursors() {
    for (int i = 0; i < copiers.size(); i++) {
      cursors[i] = new Cursor();
    }
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void noMoreToConsume() {
    noMoreToConsume = true;
    if (bufferedIndex > 0) {
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
    }
  }

  @Override
  public void consumeData(int targetRecords) {
    state.is(State.CAN_CONSUME);

    if (shouldBufferOutput) {
      assert incomingIndex == 0;
      bufferedConsumeData();
    } else {
      unbufferedConsumeData();
    }
  }

  @Override
  public int outputData() {
    context.getStats().addLongStat(Metric.OUTPUT_BATCH_COUNT, 1); // useful for testing
    return shouldBufferOutput ? bufferedOutputData() : unbufferedOutputData();
  }

  private void unbufferedConsumeData() {
    // do nothing
    state = State.CAN_PRODUCE;
    return;
  }

  private int unbufferedOutputData() {
    final int count = incoming.getRecordCount();
    if (randomVector == null) {
      // if nothing is projected, just set the count and return
      assert bufferedIndex == 0;
      incomingIndex = count;
    } else {
      assert straightCopy;

      for (TransferPair tp : inToOutTransferPairs) {
        tp.transfer();
      }
    }
    state = State.CAN_CONSUME;
    output.setAllCount(count);
    return count;
  }

  private void bufferedConsumeData() {
    final int count = incoming.getRecordCount();

    // copy from incoming to buffered.
    final long addr = sv2.memoryAddress() + incomingIndex * 2;
    int appendCount = Integer.min(count - incomingIndex, context.getTargetBatchSize() - bufferedIndex);
    if (appendCount > 0) {
      int idx = 0;
      for (FieldBufferCopier copier : copiers) {
        copier.copy(addr, appendCount, cursors[idx]);
        ++idx;
      }
      incomingIndex += appendCount;
      bufferedIndex += appendCount;
    }

    if (bufferedIndex >= context.getTargetBatchSize()) {
      // buffered container is full
      state = State.CAN_PRODUCE;
    } else {
      // everything in incoming is consumed.
      assert incomingIndex == incoming.getRecordCount();
      incomingIndex = 0;
      state = State.CAN_CONSUME;
    }
  }

  private int bufferedOutputData() {
    // transfer from the buffered vector to the output vector.
    final int ouputCount = bufferedIndex;
    for (TransferPair tp : bufferedToOutTransferPairs) {
      tp.transfer();
    }
    output.setAllCount(ouputCount);

    // reset the buffered vector.
    bufferedIndex = 0;
    resetCursors();

    // handle pending data if any, in incoming.
    if (noMoreToConsume) {
      assert incomingIndex == 0;
      state = State.DONE;
    } else {
      bufferedConsumeData();
    }
    return ouputCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(buffered);
    AutoCloseables.close(output);
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  public enum Metric implements MetricDef {
    OUTPUT_BATCH_COUNT
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }
}
