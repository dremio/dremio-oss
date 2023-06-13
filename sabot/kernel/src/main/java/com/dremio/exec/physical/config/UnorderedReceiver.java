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

import java.util.List;

import com.dremio.exec.physical.base.AbstractReceiver;
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
import com.google.common.base.Preconditions;

@JsonTypeName("unordered-receiver")
public class UnorderedReceiver extends AbstractReceiver implements OpWithMinorSpecificAttrs {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnorderedReceiver.class);
  private static final String SENDERS_ATTRIBUTE_KEY = "unordered-receiver-senders";
  private List<MinorFragmentIndexEndpoint> senders;

  public UnorderedReceiver(
    OpProps props,
    BatchSchema schema,
    int senderMajorFragmentId,
    List<MinorFragmentIndexEndpoint> senders,
    boolean spooling)
  {
    super(props, schema, senderMajorFragmentId, spooling);
    this.senders = senders;
  }

  @JsonCreator
  public UnorderedReceiver(
      @JsonProperty("props") OpProps props,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("senderMajorFragmentId") int senderMajorFragmentId,
      @JsonProperty("spooling") boolean spooling
      ) {
    this(props, schema, senderMajorFragmentId, null, spooling);
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return true;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnorderedReceiver(this, value);
  }

  @Override
  public final PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new UnorderedReceiver(props, getSchema(), getSenderMajorFragmentId(), senders, isSpooling());
  }

  @Override
  @JsonIgnore
  public List<MinorFragmentIndexEndpoint> getProvidingEndpoints() {
    return senders;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.UNORDERED_RECEIVER_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    writer.writeMinorFragmentIndexEndpoints(getProps(), SENDERS_ATTRIBUTE_KEY, senders);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    this.senders = reader.readMinorFragmentIndexEndpoints(getProps(), SENDERS_ATTRIBUTE_KEY);
  }
}
