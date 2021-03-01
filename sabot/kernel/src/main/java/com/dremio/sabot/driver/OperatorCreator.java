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

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;

/**
 * Interface for generating operator instances.
 */
public interface OperatorCreator {
  <T extends PhysicalOperator> SingleInputOperator getSingleInputOperator(OperatorContext context, T operator) throws Exception;
  default <T extends PhysicalOperator> SingleInputOperator getSingleInputOperator(FragmentExecutionContext fec,
                                                                                  OperatorContext context, T operator) throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }
  <T extends PhysicalOperator> DualInputOperator getDualInputOperator(OperatorContext context, T operator) throws Exception;
  <T extends PhysicalOperator> TerminalOperator getTerminalOperator(TunnelProvider provider, OperatorContext context, T operator) throws Exception;
  <T extends PhysicalOperator> ProducerOperator getProducerOperator(FragmentExecutionContext fec, OperatorContext context, T operator) throws Exception;
  <T extends PhysicalOperator> ProducerOperator getReceiverOperator(BatchStreamProvider buffers, OperatorContext context, T operator) throws Exception;
}
