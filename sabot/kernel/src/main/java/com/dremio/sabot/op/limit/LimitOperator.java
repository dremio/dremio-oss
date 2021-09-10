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
package com.dremio.sabot.op.limit;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.Limit;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.ImmutableList;

public class LimitOperator implements SingleInputOperator {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LimitOperator.class);

  private State state = State.NEEDS_SETUP;
  private final boolean noEndLimit;

  private final OperatorContext context;
  private final VectorContainer outgoing;
  private int recordsToSkip;
  private int recordsLeft;
  private ImmutableList<TransferPair> transfers;
  private int recordCount;

  public LimitOperator(OperatorContext context, Limit popConfig){
    this.context = context;
    this.outgoing = context.createOutputVectorContainer();

    recordsToSkip = popConfig.getFirst();
    noEndLimit = popConfig.getLast() == null;

    if(!noEndLimit) {
      recordsLeft = popConfig.getLast() - recordsToSkip;
    }

  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    List<TransferPair> pairs = new ArrayList<>();
    for(VectorWrapper<?> w : accessible){
      TransferPair pair = w.getValueVector().getTransferPair(context.getAllocator());
      pairs.add(pair);
      outgoing.add(pair.getTo());
    }

    transfers = ImmutableList.copyOf(pairs);

    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    return outgoing;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    if(recordsToSkip == 0){
      if(noEndLimit) {
        transferAll(records);
        return;
      } else {
        if(recordsLeft > records){
          transferAll(records);
        }else {
          transferPartial(recordsLeft);
        }
      }

    } else if(recordsToSkip >= records){
      recordsToSkip -= records;
      // stay in consume since we have no records to output.
      return;
    } else {
      int recordsToCopy = (noEndLimit) ? (records - recordsToSkip) : Math.min(records - recordsToSkip, recordsLeft);
      copyPartial(recordsToSkip, recordsToCopy);
    }

  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.DONE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    if(recordsLeft > 0 || noEndLimit){
      state = State.CAN_CONSUME;
    } else {
      state = State.DONE;
    }
    return recordCount;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    outgoing.close();
  }

  @Override
  public State getState() {
    return state;
  }

  private void transferPartial(int length){
    recordCount = length;
    recordsLeft -= length;
    for(TransferPair p : transfers){
      p.transfer();
    }
    outgoing.setAllCount(recordCount);
    state = State.CAN_PRODUCE;
  }

  private void copyPartial(final int offset, final int length){
    outgoing.allocateNew();
    recordsToSkip -= offset;
    recordsLeft -= length;
    recordCount = length;
    for(final TransferPair p : transfers){
      for(int i = 0; i < length; i++){
        p.copyValueSafe(offset + i, i);
      }
    }
    outgoing.setAllCount(recordCount);
    state = State.CAN_PRODUCE;
  }

  private void transferAll(int length){
    recordCount = length;
    recordsLeft -= length;
    for(TransferPair p : transfers){
      p.transfer();
    }
    outgoing.setRecordCount(recordCount);
    state = State.CAN_PRODUCE;
  }

  public static class Creator implements SingleInputOperator.Creator<Limit>{

    @Override
    public SingleInputOperator create(OperatorContext context, Limit operator) throws ExecutionSetupException {
      return new LimitOperator(context, operator);
    }

  }
 }
