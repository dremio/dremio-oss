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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.Operator.Producer;

/** An operator that produces data from a non-operator source (files, sockets, etc). */
public interface ProducerOperator extends Producer {

  enum State implements OperatorState<State> {
    NEEDS_SETUP(MasterState.NEEDS_SETUP),
    CAN_PRODUCE(MasterState.CAN_PRODUCE),
    BLOCKED(MasterState.BLOCKED),
    DONE(MasterState.DONE);

    final MasterState master;

    State(MasterState master) {
      this.master = master;
    }

    @Override
    public void is(State expected) {
      assert expected == this : String.format(Operator.ERROR_STRING, expected.name(), this.name());
    }

    @Override
    public MasterState getMasterState() {
      return master;
    }
  }

  @Override
  ProducerOperator.State getState();

  /**
   * Setups operator. Can only be called once. SqlOperatorImpl must be in NEEDS_SETUP state.
   *
   * @return The VectorAccessible to be used for result return.
   */
  VectorAccessible setup() throws Exception;

  interface Creator<T extends PhysicalOperator> {
    ProducerOperator create(
        FragmentExecutionContext fragmentExecContext, OperatorContext context, T config)
        throws ExecutionSetupException;
  }

  interface ReceiverCreator<T extends PhysicalOperator> {
    ProducerOperator create(BatchStreamProvider streams, OperatorContext context, T config)
        throws ExecutionSetupException;
  }
}
