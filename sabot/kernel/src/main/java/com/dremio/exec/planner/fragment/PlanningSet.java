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
package com.dremio.exec.planner.fragment;

import java.util.Iterator;
import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class PlanningSet implements Iterable<Wrapper> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSet.class);

  private final Map<Fragment, Wrapper> fragmentMap = Maps.newHashMap();
  private int majorFragmentIdIndex = 0;

  public Wrapper get(Fragment node) {
    Wrapper wrapper = fragmentMap.get(node);
    if (wrapper == null) {

      int majorFragmentId = 0;

      // If there is a sending exchange, we need to number other than zero.
      if (node.getSendingExchange() != null) {

        // assign the upper 16 nodes as the major fragment id.
        majorFragmentId = node.getSendingExchange().getChild().getOperatorId() >> 16;

        // if they are not assigned, that means we mostly likely have an externally generated plan.  in this case, come up with a major fragmentid.
        if (majorFragmentId == 0) {
          majorFragmentId = majorFragmentIdIndex;
        }
      }
      wrapper = new Wrapper(node, majorFragmentId);
      fragmentMap.put(node, wrapper);
      majorFragmentIdIndex++;
    }
    return wrapper;
  }

  @Override
  public Iterator<Wrapper> iterator() {
    return this.fragmentMap.values().iterator();
  }

  @Override
  public String toString() {
    return "FragmentPlanningSet:\n" + fragmentMap.values() + "]";
  }

  /**
   * Update Wrappers with allocations received from scheduler
   */
  public void updateWithAllocations(Map<Integer, Map<CoordinationProtos.NodeEndpoint, Long>> allocations) throws ExecutionSetupException {
    for (Wrapper wrapper : fragmentMap.values()) {
      if (!wrapper.isEndpointsAssignmentDone()) {
        throw new ExecutionSetupException("Node assignment is not done for major Fragment: " +
          wrapper.getMajorFragmentId());
      }
      int majorFragmentId = wrapper.getMajorFragmentId();
      final Map<CoordinationProtos.NodeEndpoint, Long> majorFragmentAllocations = allocations.get(majorFragmentId);
      Preconditions.checkNotNull(majorFragmentAllocations);
      wrapper.assignMemoryAllocations(allocations.get(majorFragmentId));
    }
  }
}
