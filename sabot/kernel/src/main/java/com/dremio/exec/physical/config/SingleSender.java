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

import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.base.AbstractSender;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Sender that pushes all data to a single destination node.
 */
@JsonTypeName("single-sender")
public class SingleSender extends AbstractSender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSender.class);

  /**
   * Create a SingleSender which sends data to fragment identified by given MajorFragmentId and MinorFragmentId,
   * and running at given endpoint
   *
   * @param oppositeMajorFragmentId MajorFragmentId of the receiver fragment.
   * @param oppositeMinorFragmentId MinorFragmentId of the receiver fragment.
   * @param child Child operator
   * @param destination SabotNode endpoint where the receiver fragment is running.
   */
  @JsonCreator
  public SingleSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                      @JsonProperty("receiver-minor-fragment") int oppositeMinorFragmentId,
                      @JsonProperty("child") PhysicalOperator child,
                      @JsonProperty("destination") NodeEndpoint destination,
                      @JsonProperty("schema") BatchSchema schema) {
    super(oppositeMajorFragmentId, child,
        Collections.singletonList(new MinorFragmentEndpoint(oppositeMinorFragmentId, destination)), schema);
  }

  /**
   * Create a SingleSender which sends data to fragment with MinorFragmentId as <i>0</i> in given opposite major
   * fragment.
   *
   * @param oppositeMajorFragmentId MajorFragmentId of the receiver fragment.
   * @param child Child operator
   * @param destination SabotNode endpoint where the receiver fragment is running.
   */
  public SingleSender(int oppositeMajorFragmentId, PhysicalOperator child, NodeEndpoint destination, BatchSchema schema) {
    this(oppositeMajorFragmentId, 0 /* default opposite minor fragment id*/, child, destination, schema);
  }

  @Override
  @JsonIgnore // Destination endpoint is exported via getDestination() and getOppositeMinorFragmentId()
  public List<MinorFragmentEndpoint> getDestinations() {
    return destinations;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleSender(oppositeMajorFragmentId, getOppositeMinorFragmentId(), child, getDestination(), schema);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSingleSender(this, value);
  }

  @JsonProperty("destination")
  public NodeEndpoint getDestination() {
    return getDestinations().get(0).getEndpoint();
  }

  @JsonProperty("receiver-minor-fragment")
  public int getOppositeMinorFragmentId() {
    return getDestinations().get(0).getId();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.SINGLE_SENDER_VALUE;
  }

}
