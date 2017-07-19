/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Receiver;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * UnorderedDeMuxExchange is a version of DeMuxExchange where the incoming batches are not sorted.
 */
@JsonTypeName("unordered-demux-exchange")
public class UnorderedDeMuxExchange extends AbstractDeMuxExchange {

  public UnorderedDeMuxExchange(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("expr") LogicalExpression expr) {
    super(child, expr);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId, FunctionLookupContext context) {
    createSenderReceiverMapping();

    MinorFragmentEndpoint sender = receiverToSenderMapping.get(minorFragmentId);
    if (sender == null) {
      throw new IllegalStateException(String.format("Failed to find sender for receiver [%d]", minorFragmentId));
    }

    return new UnorderedReceiver(this.senderMajorFragmentId, Collections.singletonList(sender), false, getSchema(context));
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnorderedDeMuxExchange(child, expr);
  }
}
