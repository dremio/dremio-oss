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
package com.dremio.sabot.op.spi;

/**
 * The base unit of Execution. An operator is expected to consume one or more
 * streams of data input and output those. They are single threaded and designed
 * to do small amounts of work each time they consume data. There are number of
 * different operators types that have different states. However, all stats onto
 * a set of MasterStates that describe what the operators status is.
 *
 * There are four main operators types, that expose one of or both of two main operator subtypes. They are as follows:
 *
 * - DualInputOperator (Producer): Examples include HashJoinOperator, UnionOperator.
 * - SingleInputOperator (Producer, SingleConsumer): Examples include FilterOperator, SortOperator
 * - ProducerOperator (Producer): Examples include ScanOperator, UnorderedReceiverOperator
 * - TerminalOperator (SingleConsumer): Examples include SingleSenderOperator, ScreenOperator
 */
public interface Operator extends AutoCloseable {

  public static String ERROR_STRING = "SqlOperatorImpl should have been in state %s but was in state %s.";

  /**
   * A union of all possible operator states. This allows some levels of code to
   * work with operators independent of their specific state type.
   */
  public enum MasterState {
    NEEDS_SETUP,
    CAN_CONSUME,
    CAN_CONSUME_L,
    CAN_CONSUME_R,
    CAN_PRODUCE,
    BLOCKED,
    DONE
  }

  interface OperatorState<T> {
    MasterState getMasterState();
    void is(T expected);
    String name();
  }

  <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP;

  OperatorState<?> getState();


  /**
   * A type of operator that can output data.
   */
  interface Producer extends Operator {

    /**
     * Informs the operator to populate output data.
     * @return The number of records output.
     */
    int outputData() throws Exception;
  }

  /**
   * A type of operator that has a single stream of data as input.
   */
  interface SingleConsumer extends Operator {

    /**
     * Informs operator that no more data will arrive. After this point, operator
     * must either expose CAN_POPULATE or DONE.
     */
    void noMoreToConsume() throws Exception;

    /**
     * Informs the operator to consume the data currently available in the
     * previously provided VectorAccessible. Will only be called once per data
     * available. Can only be called if SqlOperatorImpl is in a CAN_CONSUME state.
     */
    void consumeData(int records) throws Exception;

  }

  /**
   * A visitor pattern used to visit the four core operator types.
   *
   * @param <RETURN> A configurable return type.
   * @param <EXTRA> A configurable propagation type.
   * @param <EXCEP> A configurable exception type.
   */
  interface OperatorVisitor<RETURN, EXTRA, EXCEP extends Throwable> {
    RETURN visitDualInput(DualInputOperator op, EXTRA extra) throws EXCEP;
    RETURN visitSingleInput(SingleInputOperator op, EXTRA extra) throws EXCEP;
    RETURN visitProducer(ProducerOperator op, EXTRA extra) throws EXCEP;
    RETURN visitTerminalOperator(TerminalOperator op, EXTRA extra) throws EXCEP;
  }

}
