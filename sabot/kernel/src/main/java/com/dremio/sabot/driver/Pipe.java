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
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.Operator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.base.Preconditions;

/**
 * An abstract interconnection of operators that allows pumping data between distinct operators.
 */
abstract class Pipe {

  static enum Result {NOT_READY_UPSTREAM, NOT_READY_DOWNSTREAM, PUMPED, DONE}

  protected Pipe downstream;

  public Pipe() {
    super();
  }

  public abstract Result pump();
  public abstract Pipe getRequiredUpstream();
  public abstract VectorAccessible setup() throws Exception;

  public void setDownstream(Pipe downstream){
    Preconditions.checkNotNull(downstream);
    Preconditions.checkArgument(this.downstream == null, "Downstream already set.");
    this.downstream = downstream;
  }

  public Pipe getDownstream() {
    return downstream;
  }

  public abstract <DOWN, UP, EXCEP extends Exception> UP accept(Visitor<DOWN, UP, EXCEP> visitor, DOWN down) throws EXCEP;

  public interface Visitor<DOWN, UP, EXCEP extends Exception> {
    UP visitWyePipe(WyePipe pipe, DOWN down)  throws EXCEP;
    UP visitStraightPipe(StraightPipe pipe, DOWN down) throws EXCEP;
  }

  @Override
  public abstract String toString();


  static class SetupVisitor implements Operator.OperatorVisitor<VectorAccessible, VectorAccessible, Exception> {

    @Override
    public VectorAccessible visitDualInput(DualInputOperator op, VectorAccessible extra) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorAccessible visitSingleInput(SingleInputOperator op, VectorAccessible extra) throws Exception {
      Preconditions.checkNotNull(extra);
      return op.setup(extra);
    }

    @Override
    public VectorAccessible visitProducer(ProducerOperator op, VectorAccessible extra) throws Exception {
      Preconditions.checkArgument(extra == null);
      return op.setup();
    }

    @Override
    public VectorAccessible visitTerminalOperator(TerminalOperator op, VectorAccessible extra) throws Exception {
      Preconditions.checkNotNull(extra);
      op.setup(extra);
      return null;
    }

  }
}
