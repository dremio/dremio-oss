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

import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.physical.base.AbstractReceiver;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import java.util.List;

// The goal of this operator is to produce outgoing batches with records
// ordered according to the supplied expression.  Each incoming batch
// is guaranteed to be in order, so the operator simply merges the incoming
// batches.  This is accomplished by building and depleting a priority queue.
@JsonTypeName("merging-receiver")
public class MergingReceiverPOP extends AbstractReceiver {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MergingReceiverPOP.class);

  private final List<Ordering> orderings;
  private final List<MinorFragmentIndexEndpoint> senders;

  @JsonCreator
  public MergingReceiverPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("senderMajorFragmentId") int senderMajorFragmentId,
      @JsonProperty("senders") List<MinorFragmentIndexEndpoint> senders,
      @JsonProperty("spooling") boolean spooling,
      @JsonProperty("orderings") List<Ordering> orderings) {
    super(props, schema, senderMajorFragmentId, spooling);
    this.senders = senders;
    this.orderings = orderings;
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitMergingReceiver(this, value);
  }

  @Override
  public final PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    // rewriting is unnecessary since the inputs haven't changed.
    return this;
  }

  public List<Ordering> getOrderings() {
    return orderings;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.MERGING_RECEIVER_VALUE;
  }

  @Override
  @JsonProperty("senders")
  public List<MinorFragmentIndexEndpoint> getProvidingEndpoints() {
    return senders;
  }
}
