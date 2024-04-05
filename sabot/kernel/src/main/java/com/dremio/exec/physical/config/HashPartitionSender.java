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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.base.AbstractSender;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.OpWithMinorSpecificAttrs;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonTypeName("hash-partition-sender")
public class HashPartitionSender extends AbstractSender implements OpWithMinorSpecificAttrs {
  private static final String DESTINATIONS_ATTRIBUTE_KEY = "hash-partition-sender-destinations";

  private List<MinorFragmentIndexEndpoint> destinations;
  private final LogicalExpression expr;

  private final boolean adaptiveHash;

  public HashPartitionSender(
      OpProps props,
      BatchSchema schema,
      PhysicalOperator child,
      int receiverMajorFragmentId,
      List<MinorFragmentIndexEndpoint> destinations,
      LogicalExpression expr,
      boolean adaptiveHash) {
    super(props, schema, child, receiverMajorFragmentId);
    this.destinations = destinations;
    this.expr = expr;
    this.adaptiveHash = adaptiveHash;
  }

  public HashPartitionSender(
      OpProps props,
      BatchSchema schema,
      PhysicalOperator child,
      int receiverMajorFragmentId,
      List<MinorFragmentIndexEndpoint> destinations,
      LogicalExpression expr) {
    this(props, schema, child, receiverMajorFragmentId, destinations, expr, false);
  }

  @JsonCreator
  public HashPartitionSender(
      @JsonProperty("props") OpProps props,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("receiverMajorFragmentId") int receiverMajorFragmentId,
      @JsonProperty("expr") LogicalExpression expr,
      @JsonProperty("adaptiveHash") boolean adaptiveHash) {
    this(props, schema, child, receiverMajorFragmentId, null, expr, adaptiveHash);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashPartitionSender(
        props, schema, child, receiverMajorFragmentId, destinations, expr, adaptiveHash);
  }

  public LogicalExpression getExpr() {
    return expr;
  }

  public boolean getAdaptiveHash() {
    return adaptiveHash;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitHashPartitionSender(this, value);
  }

  @JsonIgnore
  @Override
  public List<MinorFragmentIndexEndpoint> getDestinations() {
    return destinations;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HASH_PARTITION_SENDER_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    writer.writeMinorFragmentIndexEndpoints(getProps(), DESTINATIONS_ATTRIBUTE_KEY, destinations);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    this.destinations =
        reader.readMinorFragmentIndexEndpoints(getProps(), DESTINATIONS_ATTRIBUTE_KEY);
  }
}
