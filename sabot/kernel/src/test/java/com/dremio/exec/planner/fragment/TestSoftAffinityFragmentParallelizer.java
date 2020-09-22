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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.common.nodes.EndpointHelper;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
                                                               final int maxGlobalWidth, final double affinityFactor,
                                                               final boolean shouldIgnoreLeafAffinity) {
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

      @Override
      public boolean shouldIgnoreLeafAffinity() {
        return shouldIgnoreLeafAffinity;
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
    10, 0.3D, false));

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
          10, 0.3D, false));

    // Regardless of affinity, the result must be one of the endpoints in the activeEndpoints list
    assertNotNull(endpoints);
    assertEquals(1, endpoints.size());
    assertEquals(N2_EP1, endpoints.get(0));
  }

  @Test
  public void testNodesIgnoreAffinity() throws Exception {

    List<CoordinationProtos.NodeEndpoint> activeEndpoints = ImmutableList.of(N1_EP1, N1_EP2, N2_EP1);

    EndpointAffinity N1_EP1_A = new EndpointAffinity(N1_EP1, 0.5);
    EndpointAffinity N1_EP2_A = new EndpointAffinity(N1_EP2, 0.5);
    EndpointAffinity N2_EP1_A = new EndpointAffinity(N2_EP1, 0.6);

    Map<CoordinationProtos.NodeEndpoint, EndpointAffinity> endpointAffinityMap = ImmutableMap.of(N1_EP1, N1_EP1_A,
      N1_EP2, N1_EP2_A, N2_EP1, N2_EP1_A);

    ParallelizationParameters params = newParameters(3, 1,
      1, 0.3D, false);
    Map<CoordinationProtos.NodeEndpoint, EndpointAffinity> updatedAffinityMap = SoftAffinityFragmentParallelizer.INSTANCE
      .getEndpointAffinityMap(endpointAffinityMap, true, params);
    List<CoordinationProtos.NodeEndpoint> endpoints = SoftAffinityFragmentParallelizer.INSTANCE
      .findEndpoints(
        activeEndpoints,
        updatedAffinityMap, 1,
        params);

    assertNotNull(endpoints);
    assertEquals(1, endpoints.size());
    assertEquals(N2_EP1, endpoints.get(0));

    /*
     * Run 100*numNodes times. Ideally, each node should be selected 100 times. Assert that each node gets selected at
     * least 25 times.
     */
    ParallelizationParameters ignoreAffinityParams = newParameters(3, 1,
      1, 0.3D, true);

    Map<CoordinationProtos.NodeEndpoint, Integer> nodeEndpointFrequencyMap = new HashMap<>();
    int numExpected = 100;
    for (int i = 0; i < numExpected * endpoints.size(); ++i) {
      Map<CoordinationProtos.NodeEndpoint, EndpointAffinity> updated = SoftAffinityFragmentParallelizer.INSTANCE
        .getEndpointAffinityMap(endpointAffinityMap, true, ignoreAffinityParams);

      List<CoordinationProtos.NodeEndpoint> selected = SoftAffinityFragmentParallelizer.INSTANCE
        .findEndpoints(
          activeEndpoints,
          updated, 1,
          ignoreAffinityParams);

      assertEquals(1, selected.size());
      nodeEndpointFrequencyMap.merge(selected.get(0), 1, Integer::sum);
    }

    assertEquals(activeEndpoints.size(), nodeEndpointFrequencyMap.size());
    for (Integer frequency : nodeEndpointFrequencyMap.values()) {
      assertTrue("frequency " + frequency + ", expected minimum " + (numExpected / 10),
        frequency >= numExpected / 10);
    }
  }

  private void nodesFromSelectedWithAffinity(int totalNodes, int nodesInEngine, int affinityNodeStartIdx, int affinityNodeEndIdx) throws Exception {
    List<CoordinationProtos.NodeEndpoint> allEndpoints = new ArrayList<>();
    for (int i = 0; i < totalNodes; ++i) {
      allEndpoints.add(CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("node" + i)
        .setFabricPort(9000)
        .build());
    }

    Map<CoordinationProtos.NodeEndpoint, EndpointAffinity> endpointAffinityMap = new HashMap<>();
    for (int i = affinityNodeStartIdx; i <= affinityNodeEndIdx; ++i) {
      CoordinationProtos.NodeEndpoint ep = allEndpoints.get(i);
      endpointAffinityMap.put(ep, new EndpointAffinity(ep, 0.3));
    }
    List<CoordinationProtos.NodeEndpoint> endpointsInSelectedEngine = allEndpoints.subList(0, nodesInEngine);

    ParallelizationParameters params = newParameters(3, 1000,
      1000, 0.2D, false);

    List<CoordinationProtos.NodeEndpoint> endpoints = SoftAffinityFragmentParallelizer.INSTANCE
      .findEndpoints(
        endpointsInSelectedEngine,
        endpointAffinityMap, 100,
        params);

    assertEquals(100, endpoints.size());
    assertTrue("one or more executors " + EndpointHelper.getMinimalString(endpoints) +
        " selected from outside the engine " + EndpointHelper.getMinimalString(endpointsInSelectedEngine),
      ImmutableSet.copyOf(endpointsInSelectedEngine).containsAll(endpoints));
  }

  @Test
  public void testSelectionWithAllInEngineAffined() throws Exception {
    nodesFromSelectedWithAffinity(10, 3, 0, 5);
  }

  @Test
  public void testSelectionWithSomeInEngineAffined() throws Exception {
    nodesFromSelectedWithAffinity(10, 5, 3, 8);
  }

  @Test
  public void testSelectionWithAllAffined() throws Exception {
    nodesFromSelectedWithAffinity(10, 3, 0, 9);
  }

  @Test
  public void testSelectionWithNoneAffined() throws Exception {
    nodesFromSelectedWithAffinity(10, 3, 0, 0);
  }

  @Test
  public void testSelectionWithNoneInEngineAffined() throws Exception {
    nodesFromSelectedWithAffinity(10, 3, 4, 6);
  }
}
