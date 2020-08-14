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
package com.dremio.sabot.op.screen;

import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.op.spi.TerminalOperator;

/**
 * A TerminalOperator which does not send messages to Coordinator
 */
public class SilentScreenOperator implements TerminalOperator {

  private State state = State.NEEDS_SETUP;

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setup(VectorAccessible incoming) throws Exception {
    state = State.CAN_CONSUME;
  }

  @Override
  public void receivingFragmentFinished(ExecProtos.FragmentHandle handle) {
    throw new IllegalStateException("No operators should exist downstream of Screen.");
  }

  @Override
  public void noMoreToConsume() {
    state = State.DONE;
  }

  @Override
  public void consumeData(int records) {
    state.is(State.CAN_CONSUME);
  }

  @Override
  public void close() throws Exception {}
}
