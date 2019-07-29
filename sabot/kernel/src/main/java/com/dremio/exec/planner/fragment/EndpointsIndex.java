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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.dremio.exec.physical.base.EndpointHelper;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**
 * Index for node endpoints.
 *
 * Used to deduplicate references to the same endpoints, and reduce heap usage and rpc size.
 */
public class EndpointsIndex {
  /*
   * The index can be accessed by multiple slicing threads in parallel, so it needs to be a
   * concurrent map.
   */
  private Map<MinorFragmentIndexEndpoint, MinorFragmentEndpoint> fragmentsEndpointMap = new ConcurrentHashMap<>();
  private final List<NodeEndpoint> endpoints;

  public EndpointsIndex(List<NodeEndpoint> endpoints) {
    this.endpoints = endpoints;
  }

  // For testing
  public EndpointsIndex() { this.endpoints = ImmutableList.of(); }

  /**
   * Resolve an endpoint index to an endpoint.
   * @param idx index of endpoint.
   * @return endpoint corresponding to the index.
   */
  public NodeEndpoint getNodeEndpoint(int idx) {
    return endpoints.get(idx);
  }

  /**
   * Resolve a fragment endpoint index to fragment endpoint.
   * @param ep fragment endpoint including minor fragment id and endpoint index.
   * @return fragment endpoint including minor fragment id and endpoint.
   */
  public MinorFragmentEndpoint getFragmentEndpoint(MinorFragmentIndexEndpoint ep) {
    // These tend to be repetitive. so, reuse object where possible.
    return fragmentsEndpointMap.computeIfAbsent(ep,
      k -> new MinorFragmentEndpoint(k.getMinorFragmentId(), endpoints.get(k.getEndpointIndex())));
  }

  public List<MinorFragmentEndpoint> getFragmentEndpoints(List<MinorFragmentIndexEndpoint> eps) {
    return eps.stream()
      .map(x -> getFragmentEndpoint(x))
      .collect(Collectors.toList());
  }

  @VisibleForTesting
  int getUniqueFragmentEndpointCount() {
    return fragmentsEndpointMap.size();
  }

  public static class Builder {
    // map of full endpoints, value is an index into minimalEndpoints.
    private Map<NodeEndpoint, Integer> fullEndpointMap = new HashMap<>();

    // list of minimal endpoints.
    private List<NodeEndpoint> minimalEndpoints = new ArrayList<>();

    // Index of all fragment index endpoints.
    private Map<Long, MinorFragmentIndexEndpoint> fragmentsIndexEndpointMap = new HashMap<>();

    public Builder() {
    }

    // Return the list of all collected endpoints.
    public List<NodeEndpoint> getAllEndpoints() {
      return minimalEndpoints;
    }

    public NodeEndpoint getMinimalEndpoint(NodeEndpoint fullEndpoint) {
      int index = lookupOrAdd(fullEndpoint);
      return minimalEndpoints.get(index);
    }

    private int lookupOrAdd(NodeEndpoint endpoint) {
      Integer index = fullEndpointMap.get(endpoint);
      if (index == null) {
        // miss : add a new entry.
        minimalEndpoints.add(EndpointHelper.getMinimalEndpoint(endpoint));
        index = minimalEndpoints.size() - 1;
        fullEndpointMap.put(endpoint, index);
      }
      return index;
    }

    public int addNodeEndpoint(NodeEndpoint endpoint){
      return lookupOrAdd(endpoint);
    }

    private long longFromTwoInts(int a, int b) {
      return ((long)a << 32) | (b & 0xffffffffL);
    }

    public MinorFragmentIndexEndpoint addFragmentEndpoint(int minorFragmentId, NodeEndpoint endpoint) {
      int endpointIdx = lookupOrAdd(endpoint);

      // These tend to be repetitive. So, index and reuse.
      return fragmentsIndexEndpointMap.computeIfAbsent(
        longFromTwoInts(minorFragmentId, endpointIdx),
        k ->
          MinorFragmentIndexEndpoint
            .newBuilder()
            .setMinorFragmentId(minorFragmentId)
            .setEndpointIndex(endpointIdx)
            .build()
      );
    }

    @VisibleForTesting
    int getUniqueFragmentIndexEndpointCount() {
      return fragmentsIndexEndpointMap.size();
    }

  }
}
