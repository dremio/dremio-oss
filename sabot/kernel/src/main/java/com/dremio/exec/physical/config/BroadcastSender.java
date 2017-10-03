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

import java.util.List;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.base.AbstractSender;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("broadcast-sender")
public class BroadcastSender extends AbstractSender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BroadcastSender.class);

  @JsonCreator
  public BroadcastSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                         @JsonProperty("child") PhysicalOperator child,
                         @JsonProperty("destinations") List<MinorFragmentEndpoint> destinations,
                         @JsonProperty("schema") BatchSchema schema) {
    super(oppositeMajorFragmentId, child, destinations, schema);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new BroadcastSender(oppositeMajorFragmentId, child, destinations, schema);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitBroadcastSender(this, value);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.BROADCAST_SENDER_VALUE;
  }
}
