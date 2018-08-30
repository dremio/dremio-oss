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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.resource.ResourceAllocation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Utility methods to help with resource allocations
 */
public final class ResourceAllocationUtils {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourceAllocationUtils.class);

  /**
   * Helper method to convert List of allocations into a map of a map
   * @param allocations
   * @return
   */
  static Map<Integer, Map<NodeEndpoint, Long>> convertAllocations(List<ResourceAllocation> allocations) {
    Map<Integer, Map<NodeEndpoint, Long>> allocationsMap = Maps.newHashMap();
    for (ResourceAllocation allocation : allocations) {
      final NodeEndpoint nodeendpoint = allocation.getEndPoint();
      final Integer majorFragmentId = allocation.getMajorFragment();
      final Long memory = allocation.getMemory();
      if (allocationsMap.get(majorFragmentId) == null) {
        allocationsMap.put(majorFragmentId, Maps.newHashMap());
      }
      allocationsMap.get(majorFragmentId).put(nodeendpoint, memory);
    }
    return allocationsMap;
  }

  static Collection<NodeEndpoint> convertAllocationsToSet(List<ResourceAllocation> allocations) {
    Set<NodeEndpoint> endpoints = Sets.newHashSet();
    allocations.forEach((v) -> endpoints.add(v.getEndPoint()));
    return endpoints;
  }

  /**
   * To get quantity of resources per node
   * May need per major fragment though
   * @param planningSet
   * @return Map of MajorFragmentId to a Map of NodeEndPoint to number of those endpoints
   */
  static Map<Integer, Map<NodeEndpoint, Integer>> getEndPoints(PlanningSet planningSet) throws ExecutionSetupException {
    Map<Integer, Map<NodeEndpoint, Integer>> endpointsPerMajorMap = Maps.newHashMap();
    for(Wrapper wrapper : planningSet) {
      if (!wrapper.isEndpointsAssignmentDone()) {
        throw new ExecutionSetupException("Node assignment is not done for major Fragment: " +
          wrapper.getMajorFragmentId());
      }
      final Map<NodeEndpoint, Integer> resourceNodeEndPointsMap = Maps.newHashMap();
      final List<NodeEndpoint> endPointsPerMajor = wrapper.getAssignedEndpoints();
      for (NodeEndpoint endpoint : endPointsPerMajor) {
        resourceNodeEndPointsMap.put(endpoint, Optional.ofNullable(resourceNodeEndPointsMap.get(endpoint)).orElse(0)+1);
        endpointsPerMajorMap.put(wrapper.getMajorFragmentId(), resourceNodeEndPointsMap);
      }
    }
    return endpointsPerMajorMap;
  }

  static Collection<NodeEndpoint> getEndPointsToSet(PlanningSet planningSet) {
    Set<NodeEndpoint> endpoints = Sets.newHashSet();
    planningSet.forEach((v) -> endpoints.addAll(v.getAssignedEndpoints()));
    return endpoints;
  }


  /**
   * To help determine if we need to reparallelize based on the available resources
   * @param fromEndpointToAllocMap
   * @param fromEndpointMap
   * @return
   */
  @VisibleForTesting
  static Collection<NodeEndpoint> reParallelizeEndPoints(
    Collection<NodeEndpoint> fromEndpointToAllocMap,
    Collection<NodeEndpoint> fromEndpointMap) {

    Collection<NodeEndpoint> diffAllocNodeEndPoints = Lists.newArrayList(fromEndpointToAllocMap);
    Collection<NodeEndpoint> diffEndPointsAllocs = Lists.newArrayList(fromEndpointMap);

    diffAllocNodeEndPoints.removeAll(fromEndpointMap);
    diffEndPointsAllocs.removeAll(fromEndpointToAllocMap);

    if (diffAllocNodeEndPoints.isEmpty() &&  diffEndPointsAllocs.isEmpty()) {
      // return empty collection - nothing to do
      return diffAllocNodeEndPoints;
    }

    // return only allocs
    return fromEndpointToAllocMap;
   }
}
