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
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;

public class VectorizedCopyOperator implements SingleInputOperator {

  private final OperatorContext context;

  private State state = State.NEEDS_SETUP;

  private VectorAccessible incoming;
  private VectorContainer output;
  private SelectionVector2 sv2;
  private List<TransferPair> transferPairs = new ArrayList<>();
  private boolean straightCopy;
  private List<FieldBufferCopier> copiers = new ArrayList<FieldBufferCopier>();

  // random vector to determine if no copy is needed.
  // could be null if no columns are projected from incoming.
  private ValueVector randomVector;

  public VectorizedCopyOperator(OperatorContext context, SelectionVectorRemover popConfig) throws OutOfMemoryException {
    this.context = context;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) {
    state.is(State.NEEDS_SETUP);

    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.FOUR_BYTE);
    this.straightCopy = incoming.getSchema() == null || incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE;
    this.incoming = incoming;
    this.output = context.createOutputVectorContainer(incoming.getSchema());
    this.output.buildSchema(SelectionVectorMode.NONE);
    this.sv2 = straightCopy ? null : incoming.getSelectionVector2();

    for(VectorWrapper<?> vv : incoming){
      TransferPair tp = vv.getValueVector().makeTransferPair(output.addOrGet(vv.getField()));
      transferPairs.add(tp);
      randomVector = vv.getValueVector();
    }

    copiers = FieldBufferCopier.getCopiers(VectorContainer.getFieldVectors(incoming), VectorContainer.getFieldVectors(output));

    state = State.CAN_CONSUME;

    return output;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void noMoreToConsume() {
    state = State.DONE;
  }

  @Override
  public void consumeData(int targetRecords) {
    // do nothing.
    state.is(State.CAN_CONSUME);
    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() {
    final int count = incoming.getRecordCount();
    if (randomVector == null) {
      // if nothing is projected, just set the count and return
    } else if (straightCopy || count == randomVector.getValueCount()){
      for(TransferPair tp : transferPairs){
        tp.transfer();
      }
    } else {
      final long addr = sv2.memoryAddress();
      for (FieldBufferCopier copier : copiers) {
        copier.copy(addr, count);
      }
    }
    state = State.CAN_CONSUME;

    output.setAllCount(count);
    return count;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(output);
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }
}
