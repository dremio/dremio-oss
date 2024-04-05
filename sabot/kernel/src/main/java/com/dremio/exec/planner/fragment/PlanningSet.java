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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class PlanningSet implements Iterable<Wrapper> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSet.class);

  private final Map<Fragment, Wrapper> fragmentMap = Maps.newHashMap();
  private int majorFragmentIdIndex = 0;

  public Map<Fragment, Wrapper> getFragmentWrapperMap() {
    return Collections.unmodifiableMap(fragmentMap);
  }

  public Wrapper get(Fragment node) {
    Wrapper wrapper = fragmentMap.get(node);
    if (wrapper == null) {

      int majorFragmentId = 0;

      // If there is a sending exchange, we need to number other than zero.
      if (node.getSendingExchange() != null) {

        // assign the upper 16 nodes as the major fragment id.
        majorFragmentId = node.getSendingExchange().getChild().getProps().getMajorFragmentId();

        // if they are not assigned, that means we mostly likely have an externally generated plan.
        // in this case, come up with a major fragmentid.
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

  /** Update Wrappers with query per-node memory limits */
  public void setMemoryAllocationPerNode(long memoryLimit) {
    for (Wrapper wrapper : fragmentMap.values()) {
      wrapper.setMemoryAllocationPerNode(memoryLimit);
    }
  }
}
