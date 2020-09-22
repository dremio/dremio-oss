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
package com.dremio.common.memory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.memory.AllocationOutcomeDetails;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.CommonUtil;

import com.dremio.common.collections.Tuple;

/**
 * Helper class to get summary of memory state on allocation failures.
 */
public final class MemoryDebugInfo {
  // (1:root, 2:queue, 3:query, 4:phase, 5:frag, 6:operator).
  private static final int NUM_LEVELS_FROM_ROOT_TO_OPERATOR = 6;
  // max nodes to dump at each level.
  private static final int MAX_NODES_PER_LEVEL = 10;

  private static void print(StringBuilder sb, BufferAllocator current, int currentLevel, int maxLevels) {
    if (currentLevel > maxLevels) {
      return;
    }

    Collection<BufferAllocator> childAllocators = current.getChildAllocators();
    CommonUtil.indent(sb, currentLevel)
        .append("Allocator(")
        .append(current.getName())
        .append(") ")
        .append(current.getInitReservation())
        .append('/')
        .append(current.getAllocatedMemory())
        .append('/')
        .append(current.getPeakMemoryAllocation())
        .append('/')
        .append(current.getLimit())
        .append(" (res/actual/peak/limit)")
        .append(" numChildAllocators:")
        .append(childAllocators.size())
        .append('\n');

    for (BufferAllocator child : pruneAllocatorList(childAllocators)) {
      print(sb, child,currentLevel + 1, maxLevels);
    }
  }

  private static Collection<BufferAllocator> pruneAllocatorList(Collection<BufferAllocator> allocators) {
    if (allocators.size() <= MAX_NODES_PER_LEVEL) {
      return allocators;
    }

    return allocators.stream()
      .map(allocator -> Tuple.of(allocator.getAllocatedMemory(), allocator))
      .sorted(Comparator.comparingLong(tuple -> -tuple.first))
      .limit(MAX_NODES_PER_LEVEL)
      .map(tuple -> tuple.second)
      .collect(Collectors.toList());
  }

  private static String getSummary(BufferAllocator start, int numLevels) {
    final StringBuilder sb = new StringBuilder();
    print(sb, start, 0, numLevels);
    return sb.toString();
  }

  public static String getSummaryFromRoot(BufferAllocator allocator) {
    // find the root allocator.
    BufferAllocator root = allocator;
    while (root.getParentAllocator() != null) {
      root = root.getParentAllocator();
    }

    return getSummary(root, NUM_LEVELS_FROM_ROOT_TO_OPERATOR);
  }

  public static String getDetailsOnAllocationFailure(OutOfMemoryException exception, BufferAllocator allocator) {
    BufferAllocator failedAtAllocator = null;

    /*
     * Check if the exception has details about the outcome. This may not always be present
     * eg. if the failure is due to fragmentation in netty slabs.
     */
    StringBuilder sb = new StringBuilder();
    Optional<AllocationOutcomeDetails> outcomeDetails = exception.getOutcomeDetails();
    if (outcomeDetails.isPresent()) {
      sb.append(outcomeDetails.get().toString());

      failedAtAllocator = outcomeDetails.get().getFailedAllocator();
    }

    /*
     * The failure could have occurred either due to :
     *
     * a. limit reached at a leaf/intermediate allocator (i.e failedAtAllocator)
     *    - In this case, we are interested in which of it's children is using the most. dump up to
     *      3 levels below this (including the failedAtAllocator).
     *
     * b. memory exhausted at root or severe fragmentation issues.
     *    - In this case, dump summary of allocators from root till the operator allocators.
     */

    String summary;
    if (failedAtAllocator == null) {
      summary = getSummaryFromRoot(allocator);
    } else if (failedAtAllocator.getParentAllocator() == null) {
      summary = getSummaryFromRoot(failedAtAllocator);
    } else {
      summary = getSummary(failedAtAllocator, 3);
    }

    sb.append("\nAllocator dominators:\n");
    sb.append(summary);
    return sb.toString();
  }
}
