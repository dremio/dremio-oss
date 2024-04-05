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
package com.dremio.sabot.op.spi;

import com.dremio.exec.proto.UserBitShared.MetricDef.DisplayType;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import java.util.EnumSet;

/**
 * The base unit of Execution. An operator is expected to consume one or more streams of data input
 * and output those. They are single threaded and designed to do small amounts of work each time
 * they consume data. There are number of different operators types that have different states.
 * However, all stats onto a set of MasterStates that describe what the operators status is.
 *
 * <p>There are four main operators types, that expose one of or both of two main operator subtypes.
 * They are as follows:
 *
 * <p>- DualInputOperator (Producer): Examples include HashJoinOperator, UnionOperator. -
 * SingleInputOperator (Producer, SingleConsumer): Examples include FilterOperator, SortOperator -
 * ProducerOperator (Producer): Examples include ScanOperator, UnorderedReceiverOperator -
 * TerminalOperator (SingleConsumer): Examples include SingleSenderOperator, ScreenOperator
 */
public interface Operator extends AutoCloseable {

  public static String ERROR_STRING =
      "SqlOperatorImpl should have been in state %s but was in state %s.";

  /**
   * A union of all possible operator states. This allows some levels of code to work with operators
   * independent of their specific state type.
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

  <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value)
      throws EXCEP;

  OperatorState<?> getState();

  /**
   * Do work on an out of band message. This can be called as long as the operator is not in
   * NEEDS_SETUP or DONE state. The default implementation is operator ignoring messages.
   *
   * @param message The message to work on.
   */
  default void workOnOOB(OutOfBandMessage message) {}

  /** A type of operator that can output data. */
  interface Producer extends Operator {

    /**
     * Informs the operator to populate output data.
     *
     * @return The number of records output.
     */
    int outputData() throws Exception;
  }

  /** A type of operator that has a single stream of data as input. */
  interface SingleConsumer extends Operator {

    /**
     * Informs operator that no more data will arrive. After this point, operator must either expose
     * CAN_POPULATE or DONE.
     */
    void noMoreToConsume() throws Exception;

    /**
     * Informs the operator to consume the data currently available in the previously provided
     * VectorAccessible. Will only be called once per data available. Can only be called if
     * SqlOperatorImpl is in a CAN_CONSUME state.
     */
    void consumeData(int records) throws Exception;
  }

  /** A type of operator that can shrink their memory usage on request */
  interface ShrinkableOperator {
    /**
     * Returns the id of the shrinkable operator
     *
     * @return
     */
    int getOperatorId();

    /**
     * Report the maximum amount of memory that can potentially be shrunk
     *
     * @return amount in bytes that can be shrunk
     */
    long shrinkableMemory();

    /**
     * Informs the operator to shrink its memory usage
     *
     * @param size (bytes) - this is the amount of shrinkable memory reported by the operator
     * @return true if the operator is done spilling memory
     */
    boolean shrinkMemory(long size) throws Exception;

    default String getOperatorStateToPrint() {
      return "";
    }

    default boolean canUseTooMuchMemoryInAPump() {
      return false;
    }

    default void setLimit(long limit) {}

    default long getAllocatedMemory() {
      return 0;
    }
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

  default void addDisplayStatsWithZeroValue(OperatorContext context, EnumSet enumSet) {
    enumSet.forEach(
        stat -> {
          if (((MetricDef) stat).getDisplayType() == DisplayType.DISPLAY_BY_DEFAULT) {
            context.getStats().addLongStat((MetricDef) stat, 0);
          }
        });
  }
}
