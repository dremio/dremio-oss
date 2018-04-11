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
package com.dremio.exec.physical.config;

import java.util.List;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.physical.base.AbstractExchange;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalOperatorUtil;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.planner.fragment.ParallelizationInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("union-exchange")
public class UnionExchange extends AbstractExchange{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionExchange.class);

  public UnionExchange(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<NodeEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");

    return ParallelizationInfo.create(1, 1, getDefaultAffinityMap(senderFragmentEndpoints));
  }

  @Override
  public void setupSenders(List<NodeEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<NodeEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(receiverLocations.size() == 1,
        "Union Exchange only supports a single receiver endpoint.");

    super.setupReceivers(receiverLocations);
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, FunctionLookupContext context) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocations.get(0), getSchema(context));
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, FunctionLookupContext context) {
    return new UnorderedReceiver(senderMajorFragmentId, PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations), false, getSchema(context));
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnionExchange(child);
  }

}
