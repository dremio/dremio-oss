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

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Multiplexing Exchange (MuxExchange) is used when results from multiple minor fragments belonging to the same
 * major fragment running on a node need to be collected at one fragment on the same node before distributing the
 * results further. This helps when the sender that is distributing the results has overhead that is proportional to
 * the number of sender instances. An example of such sender is PartitionSender. Each instance of PartitionSender
 * allocates "r" buffers where "r" is the number of receivers.
 *
 * Ex. SabotNode A is assigned 10 minor fragments belonging to the same major fragment. Each of these fragments
 * has a PartitionSender instance which is sending data to 300 receivers. Each PartitionSender needs 300 buffers,
 * so total of 10*300 buffers are needed. With MuxExchange, all 10 fragments send the data directly (without
 * partitioning) to MuxExchange which uses the PartitionSender to partition the incoming data and distribute
 * to receivers. MuxExchange has only one instance per SabotNode per major fragment which means only one instance of
 * PartitionSender per SabotNode per major fragment. With MuxExchange total number of buffers used by PartitionSender
 * for the 10 fragments is 300 instead of earlier number 10*300.
 */
public abstract class AbstractMuxExchange extends AbstractExchange {

  // Ephemeral info used when creating execution fragments.
  protected Map<Integer, MinorFragmentIndexEndpoint> senderToReceiverMapping;
  protected ArrayListMultimap<Integer, MinorFragmentIndexEndpoint> receiverToSenderMapping;
  private boolean isSenderReceiverMappingCreated;

  public AbstractMuxExchange(OpProps props, OpProps senderProps, OpProps receiverProps, BatchSchema schema, PhysicalOperator child, OptionManager optionManager) {
    super(props, senderProps, receiverProps, schema, child, optionManager);
  }

  @Override
  public ParallelizationInfo.WidthConstraint getReceiverParallelizationWidthConstraint() {
    return ParallelizationInfo.WidthConstraint.AFFINITY_LIMITED;
  }

  @Override
  public Supplier<Collection<EndpointAffinity>> getReceiverEndpointAffinity(Supplier<Collection<NodeEndpoint>> senderFragmentEndpointsSupplier) {
    return () -> {
      Collection<NodeEndpoint> senderFragmentEndpoints = senderFragmentEndpointsSupplier.get();
      Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");

      // We want to run one mux receiver per SabotNode endpoint.
      // Identify the number of unique SabotNode endpoints in sender fragment endpoints.
      List<NodeEndpoint> nodeEndpoints = ImmutableSet.copyOf(senderFragmentEndpoints).asList();

      List<EndpointAffinity> affinities = Lists.newArrayList();
      for(NodeEndpoint ep : nodeEndpoints) {
        affinities.add(new EndpointAffinity(ep, Double.POSITIVE_INFINITY));
      }
      return affinities;
    };
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child, EndpointsIndex.Builder builder) {
    createSenderReceiverMapping(builder);

    MinorFragmentIndexEndpoint receiver = senderToReceiverMapping.get(minorFragmentId);
    if (receiver == null) {
      throw new IllegalStateException(String.format("Failed to find receiver for sender [%d]", minorFragmentId));
    }
    return new SingleSender(senderProps, schema, child, receiverMajorFragmentId, receiver);
  }

  protected void createSenderReceiverMapping(EndpointsIndex.Builder indexBuilder) {
    if (isSenderReceiverMappingCreated) {
      return;
    }

    senderToReceiverMapping = Maps.newHashMap();
    receiverToSenderMapping = ArrayListMultimap.create();

    // Find the list of sender fragment ids assigned to each SabotNode endpoint.
    ArrayListMultimap<NodeEndpoint, Integer> endpointSenderList = ArrayListMultimap.create();

    int senderFragmentId = 0;
    for(NodeEndpoint senderLocation : senderLocations) {
      endpointSenderList.put(senderLocation, senderFragmentId);
      senderFragmentId++;
    }

    int receiverFragmentId = 0;
    for(NodeEndpoint receiverLocation : receiverLocations) {
      List<Integer> senderFragmentIds = endpointSenderList.get(receiverLocation);

      for(Integer senderId : senderFragmentIds) {
        senderToReceiverMapping.put(senderId,
          indexBuilder.addFragmentEndpoint(receiverFragmentId, receiverLocation));

        receiverToSenderMapping.put(receiverFragmentId,
          indexBuilder.addFragmentEndpoint(senderId, senderLocations.get(senderId)));
      }
      receiverFragmentId++;
    }

    isSenderReceiverMappingCreated = true;
  }
}
