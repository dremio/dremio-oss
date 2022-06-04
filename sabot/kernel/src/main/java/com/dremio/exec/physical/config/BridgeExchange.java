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
package com.dremio.exec.physical.config;

import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.physical.base.AbstractExchange;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;

/**
 * A special exchange that facilitates CTEs
 * - the sender writes to local files,
 * - the receiver and other bridge reader(s) can read from the same local files.
 */
public class BridgeExchange extends AbstractExchange {
  private final OptionManager optionManager;
  private final String bridgeSetId; // unique identifier within the query.

  public BridgeExchange(OpProps props, OpProps senderProps, OpProps receiverProps, BatchSchema schema, PhysicalOperator child, OptionManager optionManager,
                        String bridgeSetId) {
    super(props, senderProps, receiverProps, schema, child, optionManager);
    this.optionManager = optionManager;
    this.bridgeSetId = bridgeSetId;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new BridgeExchange(props, senderProps, receiverProps, schema, child,
      getOptionManager(), getBridgeSetId());
  }

  @Override
  public final <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitBridgeExchange(this, value);
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, EndpointsIndex.Builder builder) throws PhysicalOperatorSetupException {
    return new BridgeFileWriterSender(senderProps, schema, child, bridgeSetId);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, EndpointsIndex.Builder builder) {
    return new BridgeFileReaderReceiver(receiverProps, schema, senderMajorFragmentId, bridgeSetId);
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  OptionManager getOptionManager() {
    return optionManager;
  }

  @Override
  public ParallelizationDependency getParallelizationDependency() {
    // Since the write happens to local files, the parallelization of the sender/receiver and other bridge reader(s)
    // must be exactly the same.
    return ParallelizationDependency.RECEIVER_MUST_MATCH_SENDER;
  }
}
