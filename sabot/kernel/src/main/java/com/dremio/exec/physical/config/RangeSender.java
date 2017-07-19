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
import java.util.List;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.base.AbstractSender;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("range-sender")
public class RangeSender extends AbstractSender{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RangeSender.class);

  List<EndpointPartition> partitions;

  @JsonCreator
  public RangeSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                     @JsonProperty("child") PhysicalOperator child,
                     @JsonProperty("partitions") List<EndpointPartition> partitions,
                     @JsonProperty("schema") BatchSchema schema) {
    super(oppositeMajorFragmentId, child, Collections.<MinorFragmentEndpoint>emptyList(), schema);
    this.partitions = partitions;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new RangeSender(oppositeMajorFragmentId, child, partitions, schema);
  }

  public static class EndpointPartition{
    private final PartitionRange range;
    private final NodeEndpoint endpoint;

    @JsonCreator
    public EndpointPartition(@JsonProperty("range") PartitionRange range, @JsonProperty("endpoint") NodeEndpoint endpoint) {
      super();
      this.range = range;
      this.endpoint = endpoint;
    }
    public PartitionRange getRange() {
      return range;
    }
    public NodeEndpoint getEndpoint() {
      return endpoint;
    }
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.RANGE_SENDER_VALUE;
  }
}
