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
package com.dremio.sabot.driver;

import java.util.List;
import java.util.Map;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.exec.context.SharedResourcesContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.dremio.sabot.task.Task.State;
import com.google.common.collect.ImmutableMap;

/**
 * Responsible for pumping data between operators. Moves up and down a set of
 * pipes picking the right ones to pump to output all data.
 */
public class Pipeline implements AutoCloseable {

  private Pipe currentPipe;
  private final List<Wrapped<?>> operators;
  private final Map<Integer, Wrapped<?>> operatorMap;
  private final TerminalOperator terminal;
  private final Pipe terminalPipe;
  private final SharedResourcesContext sharedResourcesContext;
  private boolean closed = false;

  /**
   * Create a new Pipeline. The pipeline manages a set of interconnected pipes, pumping data between them.
   * @param terminalPipe The terminal (most downstream) pipe in a set of pipes.
   * @param terminal The terminal operator, (this is the endpoint of the terminal pipe and is passed in so we are type safe). It is made available to inform this pipeline that one or more receivers are no longer interested in downstream messages.
   * @param operators A list of all the operators associated with this pipe. This is done to manage closing. We keep this typed (as opposed to List<AutoCloseable> to help with debugging.
   * @param sharedResourcesContext shared resources context
   */
  public Pipeline(Pipe terminalPipe, TerminalOperator terminal, List<Wrapped<?>> operators,
                  SharedResourcesContext sharedResourcesContext) {
    this.terminalPipe = terminalPipe;
    while (terminalPipe.getRequiredUpstream() != null) {
      terminalPipe = terminalPipe.getRequiredUpstream();
    }
    this.operators = operators;

    ImmutableMap.Builder<Integer, Wrapped<?>> builder = ImmutableMap.builder();
    for(Wrapped<?> w : operators) {
      builder.put(w.getOperatorId(), w);
    }
    this.operatorMap = builder.build();
    this.currentPipe = terminalPipe;
    this.terminal = terminal;
    this.sharedResourcesContext = sharedResourcesContext;
  }

  public void setup() throws Exception {
    terminalPipe.setup();
  }

  public TerminalOperator getTerminalOperator(){
    return terminal;
  }

  public void workOnOOB(OutOfBandMessage message) {
    Wrapped<?> wrapped = operatorMap.get(message.getOperatorId());
    switch(wrapped.getState().getMasterState()) {
    case BLOCKED:
    case CAN_CONSUME:
    case CAN_CONSUME_L:
    case CAN_CONSUME_R:
    case CAN_PRODUCE:
      wrapped.workOnOOB(message);
      break;
    case DONE:
    case NEEDS_SETUP:
    default:
      // noop since the operator can't do anything with the message.
    }
  }

  /**
   * Move data between operators. Move one batch of records at most.
   *
   * @return The state of the Pipeline after operation.
   * @throws Exception
   */
  public State pumpOnce() throws Exception {
    final State state = doPump();
    if (state == State.RUNNABLE && !sharedResourcesContext.isRunnable()) {
      return State.BLOCKED_ON_SHARED_RESOURCE;
    }
    return state;
  }

  private State doPump() throws Exception {
    while (true) {

      Pipe.Result result = currentPipe.pump();

      switch (result) {
      case DONE: {
        final Pipe downstream = currentPipe.getDownstream();
        if (downstream == null) {
          return State.DONE;
        } else {
          // this pipe is done but more pipes can be pumped, we're still runnable.
          currentPipe = downstream;
          return State.RUNNABLE;
        }
      }

      case NOT_READY_UPSTREAM: {
        // let's try to go upstream.
        final Pipe upstream = currentPipe.getRequiredUpstream();

        if (upstream == null) {
          // we can't move upstream (no upstream pipes). As such, we're blocked on this pipe.
          return State.BLOCKED_ON_UPSTREAM;

        } else {
          // move upstream and get them to pump data downstream.
          currentPipe = upstream;

          // we haven't done any work, continue loop.
          continue;
        }
      }

      case NOT_READY_DOWNSTREAM: {

        // let's try to go downstream.
        final Pipe downstream = currentPipe.getDownstream();
        if( downstream == null) {

          return State.BLOCKED_ON_DOWNSTREAM;

        } else {
          currentPipe = downstream;
          // we haven't done any work, continue loop.
          continue;
        }

      }
      case PUMPED: {
        // data was pumped
        final Pipe downstream = currentPipe.getDownstream();
        if (downstream != null) {
          // we pumped the data downstream. Let's continue pushing that data downstream.
          currentPipe = downstream;
        }
        return State.RUNNABLE;
      }

      }
    }
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    for(Wrapped<?> o : operators){
      sb.append(o.getInner().getClass().getSimpleName())
        .append(":")
        .append(o.getState().name())
        .append(", ");
    }
    return sb.toString();
  }

  @Override
  public void close() throws Exception {
    if(!closed){
      closed = true;
      AutoCloseables.close(operators);
    }
  }



}
