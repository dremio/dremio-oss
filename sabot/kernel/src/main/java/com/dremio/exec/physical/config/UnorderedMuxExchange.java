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

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import java.util.List;

/** UnorderedMuxExchange is a version of MuxExchange where the incoming batches are not sorted. */
public class UnorderedMuxExchange extends AbstractMuxExchange {

  private final OptionManager optionManager;

  public UnorderedMuxExchange(
      OpProps props,
      OpProps senderProps,
      OpProps receiverProps,
      BatchSchema schema,
      PhysicalOperator child,
      OptionManager optionManager,
      int fragmentsPerEndpoint) {
    super(props, senderProps, receiverProps, schema, child, optionManager, fragmentsPerEndpoint);
    this.optionManager = optionManager;
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, EndpointsIndex.Builder indexBuilder) {
    createSenderReceiverMapping(indexBuilder);

    List<MinorFragmentIndexEndpoint> senders = receiverToSenderMapping.get(minorFragmentId);
    return new UnorderedReceiver(receiverProps, schema, senderMajorFragmentId, senders, false);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnorderedMuxExchange(
        props, senderProps, receiverProps, schema, child, optionManager, fragmentsPerEndpoint);
  }
}
