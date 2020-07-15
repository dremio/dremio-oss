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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

/**
 * Implementation of {@link FragmentParallelizer} where fragment has zero or more endpoints with affinities. Width
 * per node is depended on the affinity to the endpoint and total width (calculated using costs). Based on various
 * factors endpoints which have no affinity can be assigned to run the fragments.
 */
public class SoftAffinityFragmentParallelizer implements FragmentParallelizer {
  public static final SoftAffinityFragmentParallelizer INSTANCE = new SoftAffinityFragmentParallelizer();

  private static final Ordering<EndpointAffinity> ENDPOINT_AFFINITY_ORDERING =
      Ordering.from(new Comparator<EndpointAffinity>() {
        @Override
        public int compare(EndpointAffinity o1, EndpointAffinity o2) {
          // Sort in descending order of affinity values
          return Double.compare(o2.getAffinity(), o1.getAffinity());
        }
      });

  private int getWidth(final Stats stats, final int minWidth, final int maxWidth,
                       final ParallelizationParameters parameters, int numEndpoints) {
    // 1. Find the parallelization based on cost. Use max cost of all operators in this fragment; this is consistent
    //    with the calculation that ExcessiveExchangeRemover uses.
    int width = (int) Math.ceil(stats.getMaxCost() / parameters.getSliceTarget());

    // 2. Cap the parallelization width by fragment level width limit and system level per query width limit
    width = Math.min(width, Math.min(maxWidth, parameters.getMaxGlobalWidth()));

    // 3. Cap the parallelization width by system level per node width limit
    width = Math.min(width, parameters.getMaxWidthPerNode() * numEndpoints);

    // 4. Make sure width is at least the min width enforced by operators
    width = Math.max(minWidth, width);

    // 4. Make sure width is at most the max width enforced by operators
    width = Math.min(maxWidth, width);

    // 5 Finally make sure the width is at least one
    width = Math.max(1, width);

    return width;
  }

  @Override
  public void parallelizeFragment(final Wrapper fragmentWrapper, final ParallelizationParameters parameters,
      final Collection<NodeEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    // Find the parallelization width of fragment
    final Stats stats = fragmentWrapper.getStats();
    final ParallelizationInfo parallelizationInfo = stats.getParallelizationInfo();

    final int width = getWidth(stats, parallelizationInfo.getMinWidth(), parallelizationInfo.getMaxWidth(),
      parameters, activeEndpoints.size());

    fragmentWrapper.setWidth(width);

    final List<NodeEndpoint> assignedEndpoints = findEndpoints(activeEndpoints,
        getEndpointAffinityMap(parallelizationInfo.getEndpointAffinityMap(),
          fragmentWrapper.getFragmentDependencies().isEmpty(),
          parameters),
        fragmentWrapper.getWidth(), parameters);
    fragmentWrapper.assignEndpoints(parameters, assignedEndpoints);
  }

  @Override
  public int getIdealFragmentWidth(final Wrapper fragment, final ParallelizationParameters parameters) {
    // Find the parallelization width of fragment
    final Stats stats = fragment.getStats();
    return getWidth(stats, stats.getMinWidth(), stats.getMaxWidth(), parameters, Integer.MAX_VALUE);
  }

  @VisibleForTesting
  Map<NodeEndpoint, EndpointAffinity> getEndpointAffinityMap(final Map<NodeEndpoint, EndpointAffinity> inputAffinityMap,
                                                             final boolean isLeafFragment,
                                                             final ParallelizationParameters parameters) {

    return parameters.shouldIgnoreLeafAffinity() && isLeafFragment ? Collections.emptyMap() : inputAffinityMap;
  }

  // Assign endpoints based on the given endpoint list, affinity map and width.
  @VisibleForTesting
  List<NodeEndpoint> findEndpoints(final Collection<NodeEndpoint> activeEndpoints,
      final Map<NodeEndpoint, EndpointAffinity> endpointAffinityMap, final int width,
      final ParallelizationParameters parameters)
    throws PhysicalOperatorSetupException {

    final List<NodeEndpoint> endpoints = Lists.newArrayList();

    if (endpointAffinityMap.size() > 0) {
      // Pick endpoints from the list of active endpoints, sorted by affinity (descending, i.e., largest affinity first)
      // In other words: find the active endpoints which have the highest affinity
      final Set<NodeEndpoint> activeEndpointsSet = ImmutableSet.copyOf(activeEndpoints);
      List<EndpointAffinity> sortedAffinityList = endpointAffinityMap.values()
          .stream()
          .filter((endpointAffinity) -> activeEndpointsSet.contains(endpointAffinity.getEndpoint()))
          .sorted(Comparator.comparing(EndpointAffinity::getAffinity).reversed())
          .collect(Collectors.toList());
      sortedAffinityList = Collections.unmodifiableList(sortedAffinityList);

      // Find the number of mandatory nodes (nodes with +infinity affinity).
      int numRequiredNodes = 0;
      for(EndpointAffinity ep : sortedAffinityList) {
        if (ep.isAssignmentRequired()) {
          numRequiredNodes++;
        } else {
          // As the list is sorted in descending order of affinities, we don't need to go beyond the first occurrance
          // of non-mandatory node
          break;
        }
      }

      if (width < numRequiredNodes) {
        throw new PhysicalOperatorSetupException("Can not parallelize the fragment as the parallelization width (" + width + ") is " +
            "less than the number of mandatory nodes (" + numRequiredNodes + " nodes with +INFINITE affinity).");
      }

      // Find the maximum number of slots which should go to endpoints with affinity (See DRILL-825 for details)
      int affinedSlots =
          Math.max(1, (int) (parameters.getAffinityFactor() * width / activeEndpoints.size())) * sortedAffinityList.size();

      // Make sure affined slots is at least the number of mandatory nodes
      affinedSlots = Math.max(affinedSlots, numRequiredNodes);

      // Cap the affined slots to max parallelization width
      affinedSlots = Math.min(affinedSlots, width);

      Iterator<EndpointAffinity> affinedEPItr = Iterators.cycle(sortedAffinityList);

      // Keep adding until we have selected "affinedSlots" number of endpoints.
      while(endpoints.size() < affinedSlots) {
        EndpointAffinity ea = affinedEPItr.next();
        endpoints.add(ea.getEndpoint());
      }
    }

    // add remaining endpoints if required
    if (endpoints.size() < width) {
      // Get a list of endpoints that are not part of the affinity endpoint list
      List<NodeEndpoint> endpointsWithNoAffinity;
      final Set<NodeEndpoint> endpointsWithAffinity = endpointAffinityMap.keySet();

      if (endpointAffinityMap.size() > 0) {
        endpointsWithNoAffinity = Lists.newArrayList();
        for (NodeEndpoint ep : activeEndpoints) {
          if (!endpointsWithAffinity.contains(ep)) {
            endpointsWithNoAffinity.add(ep);
          }
        }
      } else {
        endpointsWithNoAffinity = Lists.newArrayList(activeEndpoints); // Need to create a copy instead of an
        // immutable copy, because we need to shuffle the list (next statement) and Collections.shuffle() doesn't
        // support immutable copy as input.
      }

      // round robin with random start.
      Collections.shuffle(endpointsWithNoAffinity, ThreadLocalRandom.current());
      Iterator<NodeEndpoint> otherEPItr =
          Iterators.cycle(endpointsWithNoAffinity.size() > 0 ? endpointsWithNoAffinity : endpointsWithAffinity);
      while (endpoints.size() < width) {
        endpoints.add(otherEPItr.next());
      }
    }

    return endpoints;
  }
}
