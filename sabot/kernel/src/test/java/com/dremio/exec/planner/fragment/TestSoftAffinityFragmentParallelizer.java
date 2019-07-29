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
package com.dremio.exec.planner.fragment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * To test SoftAffinityFragmentParallelizer
 */
public class TestSoftAffinityFragmentParallelizer {

  // Create a set of test endpoints
  private static final CoordinationProtos.NodeEndpoint N1_EP1 = newNodeEndpoint("node1", 30010);
  private static final CoordinationProtos.NodeEndpoint N1_EP2 = newNodeEndpoint("node1", 30011);
  private static final CoordinationProtos.NodeEndpoint N2_EP1 = newNodeEndpoint("node2", 30010);
  private static final CoordinationProtos.NodeEndpoint N2_EP2 = newNodeEndpoint("node2", 30011);
  private static final CoordinationProtos.NodeEndpoint N3_EP1 = newNodeEndpoint("node3", 30010);
  private static final CoordinationProtos.NodeEndpoint N3_EP2 = newNodeEndpoint("node3", 30011);
  private static final CoordinationProtos.NodeEndpoint N4_EP2 = newNodeEndpoint("node4", 30011);

  private static final CoordinationProtos.NodeEndpoint newNodeEndpoint(String address, int port) {
    return CoordinationProtos.NodeEndpoint.newBuilder().setAddress(address).setFabricPort(port).build();
  }

  private static final ParallelizationParameters newParameters(final long threshold, final int maxWidthPerNode,
                                                               final int maxGlobalWidth, final double affinityFactor) {
    return new ParallelizationParameters() {
      @Override
      public long getSliceTarget() {
        return threshold;
      }

      @Override
      public int getMaxWidthPerNode() {
        return maxWidthPerNode;
      }

      @Override
      public int getMaxGlobalWidth() {
        return maxGlobalWidth;
      }

      @Override
      public double getAffinityFactor() {
        return affinityFactor;
      }

      @Override
      public boolean useNewAssignmentCreator() {
        return true;
      }

      @Override
      public double getAssignmentCreatorBalanceFactor() {
        return 1.5;
      }
    };
  }

  @Test
  public void testNodesSorting() throws Exception {

    List<CoordinationProtos.NodeEndpoint> activeEndpoints = ImmutableList.of(N2_EP1, N3_EP1, N4_EP2);

    EndpointAffinity N1_EP1A = new EndpointAffinity(N1_EP1, 0.5);
    EndpointAffinity N1_EP2A = new EndpointAffinity(N1_EP2, 0.5);
    EndpointAffinity N2_EP1A = new EndpointAffinity(N2_EP1, 0.5);

    Map<CoordinationProtos.NodeEndpoint, EndpointAffinity> endpointAffinityMap = ImmutableMap.of(N1_EP1, N1_EP1A,
      N2_EP1, N2_EP1A, N1_EP2, N1_EP2A);

    List<CoordinationProtos.NodeEndpoint> endpoints = SoftAffinityFragmentParallelizer.INSTANCE
      .findEndpoints(
      activeEndpoints,
      endpointAffinityMap, 1,
        newParameters(3, 5,
    10, 0.3D));

    assertNotNull(endpoints);
    assertEquals(1, endpoints.size());
    assertEquals(N2_EP1, endpoints.get(0));

    N2_EP1A = new EndpointAffinity(N2_EP1, 0.3);

    endpointAffinityMap = ImmutableMap.of(N1_EP1, N1_EP1A,
      N2_EP1, N2_EP1A, N1_EP2, N1_EP2A);

    endpoints = SoftAffinityFragmentParallelizer.INSTANCE
      .findEndpoints(
        activeEndpoints,
        endpointAffinityMap, 1,
        newParameters(3, 5,
          10, 0.3D));

    // Regardless of affinity, the result must be one of the endpoints in the activeEndpoints list
    assertNotNull(endpoints);
    assertEquals(1, endpoints.size());
    assertEquals(N2_EP1, endpoints.get(0));
  }
}
