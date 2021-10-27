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

import java.util.Collections;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;

/**
 * UnorderedDeMuxExchange is a version of DeMuxExchange where the incoming batches are not sorted.
 */
public class UnorderedDeMuxExchange extends AbstractDeMuxExchange {

  private final OptionManager optionManager;

  public UnorderedDeMuxExchange(
    OpProps props,
    OpProps senderProps,
    OpProps receiverProps,
    BatchSchema schema,
    PhysicalOperator child,
    LogicalExpression expr,
    OptionManager optionManager) {
    super(props, senderProps, receiverProps, schema, child, expr, optionManager);
    this.optionManager = optionManager;
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, EndpointsIndex.Builder indexBuilder) {
    createSenderReceiverMapping(indexBuilder);

    MinorFragmentIndexEndpoint sender = receiverToSenderMapping.get(minorFragmentId);
    if (sender == null) {
      throw new IllegalStateException(String.format("Failed to find sender for receiver [%d]", minorFragmentId));
    }

    return new UnorderedReceiver(receiverProps, schema, senderMajorFragmentId, Collections.singletonList(sender), false);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnorderedDeMuxExchange(props, senderProps, receiverProps, schema, child, expr, optionManager);
  }
}
