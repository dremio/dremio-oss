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
package com.dremio.exec.util;

import java.util.LinkedList;
import java.util.List;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.options.OptionManager;

public final class MemoryAllocationUtilities {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryAllocationUtilities.class);

  /**
   * Helper method to set memory allocations for sorts.
   *
   * @param plan physical plan
   * @param optionManager options
   * @param clusterInfo cluster resource information
   */
  public static void setupSortMemoryAllocations(
      final PhysicalPlan plan,
      final OptionManager optionManager,
      final ClusterResourceInformation clusterInfo) {
    // look for external sorts
    final List<ExternalSort> sortList = new LinkedList<>();
    for (final PhysicalOperator op : plan.getSortedOperators()) {
      if (op instanceof ExternalSort) {
        sortList.add((ExternalSort) op);
      }
    }

    // if there are any sorts, compute the maximum allocation, and set it on them
    if (sortList.size() > 0) {
      final long maxWidthPerNode = clusterInfo.getAverageExecutorCores(optionManager);
      final long maxAllocPerNode = Math.min(clusterInfo.getAverageExecutorMemory(),
          optionManager.getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).getNumVal());
      final long maxSortAlloc = maxAllocPerNode / (sortList.size() * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);

      for(final ExternalSort externalSort : sortList) {
        externalSort.setMaxAllocation(maxSortAlloc);
      }
    }
  }

  private MemoryAllocationUtilities() {
  }
}
