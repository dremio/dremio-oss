/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.union.all;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class UnionAllOperator implements DualInputOperator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllOperator.class);

  private State state = State.NEEDS_SETUP;
  private ImmutableList<TransferPair> leftTransfers;
  private ImmutableList<TransferPair> rightTransfers;
  private VectorContainer outgoing;
  private int recordCount;
  private boolean leftIsDone = false;

  private final OperatorContext context;
  private UnionAll config;

  public UnionAllOperator(OperatorContext context, UnionAll config) {
    this.context = context;
    this.config = config;
    outgoing = context.createOutputVectorContainer();
  }

  @Override
  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
    state.is(State.NEEDS_SETUP);

    List<Field> leftFields = left.getSchema().getFields();
    List<Field> rightFields = right.getSchema().getFields();

    Preconditions.checkArgument(leftFields.size() == rightFields.size(), "Field counts don't match. Left: \n%s\nRight:\n%s", left.getSchema(), right.getSchema());

    for(int i =0; i < leftFields.size(); i++){
      Field leftField = leftFields.get(i);
      Field rightField = rightFields.get(i);
      Preconditions.checkArgument(leftField.getType().equals(rightField.getType()), "Field types don't match. Left: \n%s\nRight:\n%s", left.getSchema(), right.getSchema());
    }

    List<TransferPair> leftTransfers = new ArrayList<>();
    List<TransferPair> rightTransfers = new ArrayList<>();

    final Iterator<VectorWrapper<?>> rightIter = right.iterator();
    for(VectorWrapper<?> leftWrapper : left){
      ValueVector vector = leftWrapper.getValueVector();
      TransferPair pair = vector.getTransferPair(context.getAllocator());
      leftTransfers.add(pair);
      outgoing.add(pair.getTo());

      assert rightIter.hasNext();
      ValueVector rightVector = rightIter.next().getValueVector();
      rightTransfers.add(rightVector.makeTransferPair(pair.getTo()));
    }

    this.leftTransfers = ImmutableList.copyOf(leftTransfers);
    this.rightTransfers = ImmutableList.copyOf(rightTransfers);
    outgoing.buildSchema(SelectionVectorMode.NONE);
    state = State.CAN_CONSUME_L;
    return outgoing;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);
    for(TransferPair p : leftTransfers){
      p.transfer();
    }
    recordCount = records;
    state = State.CAN_PRODUCE;
  }

  @Override
  public void noMoreToConsumeLeft() throws Exception {
    state.is(State.CAN_CONSUME_L);
    leftIsDone = true;
    state = State.CAN_CONSUME_R;
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);
    for(TransferPair p : rightTransfers){
      p.transfer();
    }
    recordCount = records;
    state = State.CAN_PRODUCE;
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);
    state = State.DONE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    state = leftIsDone ? State.CAN_CONSUME_R : State.CAN_CONSUME_L;
    outgoing.setRecordCount(recordCount);
    return recordCount;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @Override
  public void close() throws Exception {
    outgoing.close();
  }

  public static class Creator implements DualInputOperator.Creator<UnionAll>{

    @Override
    public DualInputOperator create(OperatorContext context, UnionAll config) throws ExecutionSetupException {
      return new UnionAllOperator(context, config);
    }

  }
}
