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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

public class TestEndpointsIndex {

  @Test
  public void indexEndpointSingle() {
    EndpointsIndex.Builder indexBuilder = new EndpointsIndex.Builder();

    NodeEndpoint ep = NodeEndpoint.newBuilder()
        .setAddress("localhost")
        .setFabricPort(1700)
        .build();
    MinorFragmentEndpoint expected = new MinorFragmentEndpoint(16, ep);

    // add to index builder
    MinorFragmentIndexEndpoint indexEndpoint = indexBuilder.addFragmentEndpoint(16, ep);

    // retrieve from index
    EndpointsIndex index = new EndpointsIndex(indexBuilder.getAllEndpoints());
    MinorFragmentEndpoint out = index.getFragmentEndpoint(indexEndpoint);

    assertEquals(expected, out);
  }

  @Test
  public void indexEndpointMulti() {
    EndpointsIndex.Builder indexBuilder = new EndpointsIndex.Builder();

    List<NodeEndpoint> endpoints = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      endpoints.add(
        NodeEndpoint.newBuilder()
          .setAddress("testing " + i)
          .setFabricPort(6000 + (i % 8))
          .build());
    }

    List<MinorFragmentEndpoint> expected = new ArrayList<>();
    List<MinorFragmentIndexEndpoint> minorFragmentIndexEndpoints = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      int fragmentId = (i / 16) % 4;
      int endpointIdx = i % 16;

      expected.add(new MinorFragmentEndpoint(fragmentId, endpoints.get(endpointIdx)));

      // add to index builder
      MinorFragmentIndexEndpoint ep = indexBuilder.addFragmentEndpoint(fragmentId, endpoints.get(endpointIdx));
      minorFragmentIndexEndpoints.add(ep);
    }

    // retrieve from index
    List<NodeEndpoint> nodeEndpoints = indexBuilder.getAllEndpoints();
    assertEquals(16, nodeEndpoints.size());
    assertEquals(64, indexBuilder.getUniqueFragmentIndexEndpointCount());

    EndpointsIndex index = new EndpointsIndex(nodeEndpoints);
    List<MinorFragmentEndpoint> out = index.getFragmentEndpoints(minorFragmentIndexEndpoints);

    assertEquals(expected, out);
    assertEquals(64, index.getUniqueFragmentEndpointCount());
  }
}

