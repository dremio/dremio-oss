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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.driver.PipelineCreator.OpPipe;
import com.dremio.sabot.op.spi.Operator;
import com.dremio.sabot.op.spi.Operator.Producer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A pipe that has one source and one sink.
 */
class StraightPipe extends Pipe {
  private Pipe upstream;

  private final Operator.SingleConsumer sink;
  private final Operator.Producer source;

  public StraightPipe(Operator.SingleConsumer sink, OpPipe source) {
    Preconditions.checkArgument(source.getOperator() instanceof Producer);
    this.upstream = source.getPipe();
    this.source = (Producer) source.getOperator();
    this.sink = sink;
  }

  @Override
  public <DOWN, UP, EXCEP extends Exception> UP accept(Visitor<DOWN, UP, EXCEP> visitor, DOWN down) throws EXCEP {
    return visitor.visitStraightPipe(this, down);
  }

  @Override
  public Result pump() {
    try{
      switch(sink.getState().getMasterState()){

      case CAN_CONSUME: {
        switch(source.getState().getMasterState()){
        case CAN_PRODUCE:
          final int recordCount = source.outputData();
          // consume data if more than zero records.
          if(recordCount > 0){
            sink.consumeData(recordCount);
          }
          return Result.PUMPED;
        case DONE:
          sink.noMoreToConsume();
          upstream = null;
          return Result.DONE;

        default:
          return Result.NOT_READY_UPSTREAM;

        }
      }

      // this pipe is blocked on downstream.
      case CAN_PRODUCE:
      case BLOCKED:
        return Result.NOT_READY_DOWNSTREAM;

      case DONE:
        return Result.DONE;

      case CAN_CONSUME_L:
      case CAN_CONSUME_R:
      case NEEDS_SETUP:
      default:
        throw new IllegalStateException("Invalid state: " + sink.getState());

      }

    }catch(Exception ex){
      Throwables.propagateIfPossible(ex, UserException.class);
      throw new PipeFailure(this, ex);
    }
  }

  @Override
  public Pipe getRequiredUpstream() {
    return upstream;
  }

  @Override
  public VectorAccessible setup() throws Exception {
    final VectorAccessible sourceData = upstream == null ? source.accept(new SetupVisitor(), null) : upstream.setup();
    return sink.accept(new SetupVisitor(), sourceData);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("sink", sink)
      .add("source", source)
      .toString();
  }


}
