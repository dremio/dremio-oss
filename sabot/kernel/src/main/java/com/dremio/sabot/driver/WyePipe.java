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
package com.dremio.sabot.driver;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.driver.PipelineCreator.OpPipe;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.Operator;
import com.dremio.sabot.op.spi.Operator.Producer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * A pipe that has two sources (left and right) and one sink
 */
class WyePipe extends Pipe {
  private Pipe upstreamLeft;
  private Pipe upstreamRight;

  private final DualInputOperator downstream;
  private final Producer left;
  private final Producer right;

  public WyePipe(DualInputOperator sink, OpPipe left, OpPipe right) {
    this.downstream = sink;
    this.upstreamLeft = left.getPipe();
    this.upstreamRight = right.getPipe();
    Operator leftOp = left.getOperator();
    Operator rightOp = right.getOperator();
    Preconditions.checkArgument(leftOp instanceof Producer);
    Preconditions.checkArgument(rightOp instanceof Producer);

    this.left = (Producer) leftOp;
    this.right = (Producer) rightOp;
  }

  @Override
  public Result pump() {
    try {
      switch (downstream.getState()) {

      // want to consume from upstream, need to either consume or return not ready upstream.
      case CAN_CONSUME_L: {
        switch(left.getState().getMasterState()) {
        case CAN_PRODUCE:
          int records = left.outputData();
          if(records > 0){
            downstream.consumeDataLeft(records);
          }
          return Result.PUMPED;
        case DONE:
          downstream.noMoreToConsumeLeft();
          upstreamLeft = null;
          return upstreamRight == null ? Result.DONE : Result.NOT_READY_UPSTREAM;
        default:
          return Result.NOT_READY_UPSTREAM;
        }
      }


      // want to consume from upstream, need to either consume or return not ready upstream.
      case CAN_CONSUME_R: {
        switch(right.getState().getMasterState()) {
        case CAN_PRODUCE:
          int records = right.outputData();
          if(records > 0){
            downstream.consumeDataRight(records);
          }
          return Result.PUMPED;
        case DONE:
          downstream.noMoreToConsumeRight();
          upstreamRight = null;
          return Result.PUMPED;
        default:
          return Result.NOT_READY_UPSTREAM;
        }
      }

      // need to produce before doing anything else.
      case CAN_PRODUCE:
        return Result.NOT_READY_DOWNSTREAM;

      case DONE:
        return Result.DONE;

      case NEEDS_SETUP:
      default:
        throw new IllegalStateException();

      }
    }catch(Exception ex){
      throw new PipeFailure(this, ex);
    }
  }

  @Override
  public <DOWN, UP, EXCEP extends Exception> UP accept(Visitor<DOWN, UP, EXCEP> visitor, DOWN down) throws EXCEP {
    return visitor.visitWyePipe(this, down);
  }

  @Override
  public Pipe getRequiredUpstream() {
    switch (downstream.getState()) {
    case CAN_CONSUME_L:
      return upstreamLeft;
    case CAN_CONSUME_R:
      return upstreamRight;
    default:
      return null;
    }
  }

  @Override
  public VectorAccessible setup() throws Exception {
    final VectorAccessible leftData = upstreamLeft == null ? left.accept(new SetupVisitor(), null) : upstreamLeft.setup();
    final VectorAccessible rightData = upstreamRight == null ? right.accept(new SetupVisitor(), null) : upstreamRight.setup();
    return downstream.setup(leftData, rightData);
  }

  public static VectorAccessible setup(Pipe pipe, Operator operator) throws Exception {
    final VectorAccessible input = pipe != null ? pipe.setup() : null;

    final VectorAccessible output = operator.accept(new SetupVisitor(), input);
    try{
      output.getSchema();
    }catch(Exception ex){
      throw new Exception("SqlOperatorImpl didn't return a valid schema. " + operator, ex);
    }
    return output;
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("left", left)
      .add("right", right)
      .add("downstream", downstream)
      .toString();
  }
}
