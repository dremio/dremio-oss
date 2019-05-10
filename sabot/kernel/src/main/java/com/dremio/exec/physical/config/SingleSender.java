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

import java.util.Collections;
import java.util.List;

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
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Sender that pushes all data to a single destination node.
 */
@Options
@JsonTypeName("single-sender")
public class SingleSender extends AbstractSender implements OpWithMinorSpecificAttrs {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSender.class);
  private static final String DESTINATION_ATTRIBUTE_KEY = "single-sender-destination";
  private MinorFragmentIndexEndpoint destination;

  public SingleSender(
      OpProps props,
      BatchSchema schema,
      PhysicalOperator child,
      int receiverMajorFragmentId,
      MinorFragmentIndexEndpoint destination
      ) {
    super(props, schema, child, receiverMajorFragmentId);
    this.destination = destination;
  }

  @JsonCreator
  public SingleSender(
    @JsonProperty("props") OpProps props,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("child") PhysicalOperator child,
    @JsonProperty("receiverMajorFragmentId") int receiverMajorFragmentId
    )  {
    super(props, schema, child, receiverMajorFragmentId);
  }

  @JsonIgnore
  @Override
  public List<MinorFragmentIndexEndpoint> getDestinations() {
    return Collections.singletonList(destination);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleSender(props, schema, child, receiverMajorFragmentId, destination);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSingleSender(this, value);
  }

  @JsonIgnore
  public int getReceiverMinorFragmentId() {
    return destination.getMinorFragmentId();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.SINGLE_SENDER_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    writer.writeMinorFragmentIndexEndpoint(this, DESTINATION_ATTRIBUTE_KEY, destination);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    this.destination = reader.readMinorFragmentIndexEndpoint(this, DESTINATION_ATTRIBUTE_KEY);
  }
}
