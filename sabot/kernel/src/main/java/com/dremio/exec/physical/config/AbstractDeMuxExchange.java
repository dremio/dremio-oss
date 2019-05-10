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

import java.util.List;
import java.util.Map;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.AbstractExchange;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.ParallelizationInfo;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * DeMuxExchange is opposite of MuxExchange. It is used when the sender has overhead that is proportional to the
 * number of receivers. DeMuxExchange is run one instance per SabotNode endpoint which collects and distributes data
 * belonging to local receiving fragments running on the same SabotNode.
 *
 * Example:
 * On a 3 node cluster, if the sender has 10 receivers on each node each sender requires 30 buffers. By inserting
 * DeMuxExchange, we create one receiver per node which means total of 3 receivers for each sender. If the number of
 * senders is 10, we use 10*3 buffers instead of 10*30. DeMuxExchange has a overhead of buffer space that is equal to
 * number of local receivers. In this case each DeMuxExchange needs 10 buffers, so total of 3*10 buffers.
 */
public abstract class AbstractDeMuxExchange extends AbstractExchange {
  protected final LogicalExpression expr;

  // Ephemeral info used when creating execution fragments.
  protected Map<Integer, MinorFragmentIndexEndpoint> receiverToSenderMapping;
  protected ArrayListMultimap<Integer, MinorFragmentIndexEndpoint> senderToReceiversMapping;
  private boolean isSenderReceiverMappingCreated;

  public AbstractDeMuxExchange(
      OpProps props,
      OpProps senderProps,
      OpProps receiverProps,
      BatchSchema schema,
      PhysicalOperator child,
      LogicalExpression expr
      ) {
    super(props, senderProps, receiverProps, schema, child);
    this.expr = expr;
  }

  @JsonProperty("expr")
  public LogicalExpression getExpression(){
    return expr;
  }

  @Override
  public ParallelizationInfo getSenderParallelizationInfo(List<NodeEndpoint> receiverFragmentEndpoints) {
    Preconditions.checkArgument(receiverFragmentEndpoints != null && receiverFragmentEndpoints.size() > 0,
        "Receiver fragment endpoint list should not be empty");

    // We want to run one demux sender per SabotNode endpoint.
    // Identify the number of unique SabotNode endpoints in receiver fragment endpoints.
    List<NodeEndpoint> nodeEndpoints = ImmutableSet.copyOf(receiverFragmentEndpoints).asList();

    List<EndpointAffinity> affinities = Lists.newArrayList();
    for(NodeEndpoint ep : nodeEndpoints) {
      affinities.add(new EndpointAffinity(ep, Double.POSITIVE_INFINITY));
    }

    return ParallelizationInfo.create(affinities.size(), affinities.size(), affinities);
  }

  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<NodeEndpoint> senderFragmentEndpoints) {
    return ParallelizationInfo.UNLIMITED_WIDTH_NO_ENDPOINT_AFFINITY;
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, EndpointsIndex.Builder indexBuilder) {
    createSenderReceiverMapping(indexBuilder);

    List<MinorFragmentIndexEndpoint> receivers = senderToReceiversMapping.get(minorFragmentId);
    if (receivers == null || receivers.size() <= 0) {
      throw new IllegalStateException(String.format("Failed to find receivers for sender [%d]", minorFragmentId));
    }

    return new HashPartitionSender(senderProps, schema, child, receiverMajorFragmentId, receivers, expr);
  }

  /**
   * In DeMuxExchange, sender fragment parallelization and endpoint assignment depends on receiver fragment endpoint
   * assignments.
   */
  @Override
  public ParallelizationDependency getParallelizationDependency() {
    return ParallelizationDependency.SENDER_DEPENDS_ON_RECEIVER;
  }

  protected void createSenderReceiverMapping(EndpointsIndex.Builder indexBuilder) {
    if (isSenderReceiverMappingCreated) {
      return;
    }

    senderToReceiversMapping = ArrayListMultimap.create();
    receiverToSenderMapping = Maps.newHashMap();

    // Find the list of receiver fragment ids assigned to each SabotNode endpoint
    ArrayListMultimap<NodeEndpoint, Integer> endpointReceiverList = ArrayListMultimap.create();

    int receiverFragmentId = 0;
    for(NodeEndpoint receiverLocation : receiverLocations) {
      endpointReceiverList.put(receiverLocation, receiverFragmentId);
      receiverFragmentId++;
    }

    int senderFragmentId = 0;
    for(NodeEndpoint senderLocation : senderLocations) {
      final List<Integer> receiverMinorFragmentIds = endpointReceiverList.get(senderLocation);

      for(Integer receiverId : receiverMinorFragmentIds) {
        receiverToSenderMapping.put(receiverId, indexBuilder.addFragmentEndpoint(senderFragmentId, senderLocation));

        senderToReceiversMapping.put(senderFragmentId,
            indexBuilder.addFragmentEndpoint(receiverId, receiverLocations.get(receiverId)));
      }
      senderFragmentId++;
    }

    isSenderReceiverMappingCreated = true;
  }
}
