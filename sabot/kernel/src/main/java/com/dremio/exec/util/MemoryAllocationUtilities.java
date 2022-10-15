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
package com.dremio.exec.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.util.PrettyPrintUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;


public final class MemoryAllocationUtilities {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryAllocationUtilities.class);

  private MemoryAllocationUtilities() {}

  /**
   * Helper method to set memory allocations for sorts.
   *
   * @param plan physical plan
   * @param optionManager options
   * @param clusterInfo cluster resource information
   * @param memoryAlloc amount of memory allocated to the query per node.
   */
  @Deprecated
  private static void legacySortMemorySetting (
      final PhysicalPlan plan,
      final OptionManager optionManager,
      final GroupResourceInformation clusterInfo,
      final long memoryAlloc) {
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
      final long avgMemoryPerNode = clusterInfo.getAverageExecutorMemory();
      Preconditions.checkState(maxWidthPerNode > 0 && avgMemoryPerNode > 0, "No executors are available");
      final long maxAllocPerNode = Math.min(clusterInfo.getAverageExecutorMemory(), memoryAlloc);
      final long maxSortAlloc = maxAllocPerNode / (sortList.size() * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);

      for(final ExternalSort externalSort : sortList) {
        externalSort.getProps().setMemLimit(maxSortAlloc);
      }
    }
  }


  public static void setupBoundedMemoryAllocations (
      final PhysicalPlan plan,
      final OptionManager optionManager,
      final GroupResourceInformation clusterInfo,
      final PlanningSet planningSet,
      final long allocatedMemoryPerQuery
      ) {


    if(!optionManager.getOption(ExecConstants.USE_NEW_MEMORY_BOUNDED_BEHAVIOR)) {
      legacySortMemorySetting(plan, optionManager, clusterInfo, allocatedMemoryPerQuery);
      return;
    }

    long querySetting = Math.min(allocatedMemoryPerQuery, clusterInfo.getAverageExecutorMemory());
    setMemory(optionManager, planningSet.getFragmentWrapperMap(), querySetting);

  }

  @VisibleForTesting
  static void setMemory(final OptionManager optionManager, Map<Fragment, Wrapper> fragments, long maxMemoryPerNodePerQuery) {
    final ArrayListMultimap<NodeEndpoint, PhysicalOperator> consideredOps = ArrayListMultimap.create();
    final ArrayListMultimap<NodeEndpoint, PhysicalOperator> nonConsideredOps = ArrayListMultimap.create();
    final boolean aggressive = optionManager.getOption(PlannerSettings.ENABLE_AGGRESSIVE_MEMORY_CALCULATION);
    final long adjustReservedBytes = (aggressive) ?
      optionManager.getOption(PlannerSettings.ADJUST_RESERVED_WHEN_AGGRESSIVE) * 1024L * 1024L : 0L;

    long queryMaxAllocation = Long.MAX_VALUE;
    for(Entry<Fragment, Wrapper> entry: fragments.entrySet()) {
      PhysicalOperator root = entry.getKey().getRoot();

      if (entry.getKey().getSendingExchange() != null && aggressive) { // exchange op on sender fragment
        for (NodeEndpoint e : entry.getValue().getAssignedEndpoints()) {
          nonConsideredOps.putAll(e, Arrays.asList(root));
        }
        root = ((Exchange) root).getChild();
      }
      FindConsideredOperators fco = (aggressive) ?
        new FindConsideredOperators(root.getProps().getMajorFragmentId()) : new FindConsideredOperators(-1);
      root.accept(fco, null);
      for(NodeEndpoint e : entry.getValue().getAssignedEndpoints()) {
        consideredOps.putAll(e, fco.getConsideredOperators());
      }
      for(NodeEndpoint e : entry.getValue().getAssignedEndpoints()) {
        nonConsideredOps.putAll(e, fco.getNonConsideredOperators());
      }
    }

    // We now have a list of operators per endpoint.
    boolean isFirst = true;
    for(NodeEndpoint ep : consideredOps.keySet()) {
      if (isFirst && logger.isDebugEnabled()) {
        logger.debug(getOpMemoryDetailsString(ep, consideredOps.get(ep), nonConsideredOps.get(ep)));
        isFirst = false;
      }
      long outsideReserve = nonConsideredOps.get(ep).stream().mapToLong(t -> t.getProps().getMemReserve()).sum();

      List<PhysicalOperator> ops = consideredOps.get(ep);
      long consideredOpsReserve = ops.stream().mapToLong(t -> t.getProps().getMemReserve()).sum();
      // sum of initial allocations must not be less than the query limit
      if (outsideReserve + consideredOpsReserve > queryMaxAllocation) {
        logger.info(getOpMemoryDetailsString(ep, consideredOps.get(ep), nonConsideredOps.get(ep)));
        throw UserException.resourceError()
          .message("Query was cancelled because the initial memory requirement (%s) is greater than the job memory limit set by the administrator (%s).",
            PrettyPrintUtils.bytePrint(outsideReserve + consideredOpsReserve, true),
            PrettyPrintUtils.bytePrint(queryMaxAllocation, true))
          .build(logger);
      }

      final double totalWeights = ops.stream().mapToDouble(t -> t.getProps().getMemoryFactor()).sum();
      final long memoryForHeavyOperations = maxMemoryPerNodePerQuery - outsideReserve - adjustReservedBytes;
      if(memoryForHeavyOperations < 1) {
        logger.info(getOpMemoryDetailsString(ep, consideredOps.get(ep), nonConsideredOps.get(ep)));
        throw UserException.memoryError()
          .message("Query was cancelled because it exceeded the memory limits set by the administrator. " +
              "Expected at least %s bytes, but only had %s available.%s" +
              "Size requirement for memory intensive ops is %s bytes.%s" +
              "Missing memory = %s bytes, Number of Memory intensive ops = %d, Other ops = %d, Endpoint = %s",
            PrettyPrintUtils.bytePrint(outsideReserve, true),
            PrettyPrintUtils.bytePrint(maxMemoryPerNodePerQuery, true),
            System.lineSeparator(),
            PrettyPrintUtils.bytePrint(consideredOpsReserve, true),
            System.lineSeparator(),
            PrettyPrintUtils.bytePrint(Math.abs(memoryForHeavyOperations), true),
            consideredOps.size(), nonConsideredOps.size(), ep.getAddress())
          .build(logger);
      }
      final double baseWeight = memoryForHeavyOperations / totalWeights;
      ops.stream()
          .filter(op -> op.getProps().isMemoryBound())
          .forEach(op -> {
            long targetValue = (long) (baseWeight * op.getProps().getMemoryFactor());
            targetValue = Math.max(Math.min(targetValue, op.getProps().getMemLimit()), op.getProps().getMemReserve());
            long lowLimit = op.getProps().getMemLowLimit();
            long highLimit = op.getProps().getMemLimit();

            op.getProps().setMemLimit(targetValue);
            if (targetValue < lowLimit) {
              op.getProps().setMemLimit(lowLimit);
            }
            if (targetValue > highLimit) {
              op.getProps().setMemLimit(highLimit);
            }
          });
    }

  }

  /**
   * Visit expensive operators and collect them for a particular suboperator tree.
   */
  @VisibleForTesting
  static class FindConsideredOperators extends AbstractPhysicalVisitor<Void, Void, RuntimeException> {

    private final int majorFragmentId;
    private final List<PhysicalOperator> nonConsidered = new ArrayList<>();
    private final List<PhysicalOperator> considered = new ArrayList<>();

    public FindConsideredOperators(int majorFragmentId) {
      this.majorFragmentId = majorFragmentId;
    }

    public List<PhysicalOperator> getNonConsideredOperators(){
      return nonConsidered;
    }

    public List<PhysicalOperator> getConsideredOperators(){
      return considered;
    }

    @Override
    public Void visitOp(PhysicalOperator op, Void value) throws RuntimeException {

      if (majorFragmentId >= 0 && majorFragmentId != op.getProps().getMajorFragmentId()) {
        return null;
      }

      if( (op.getProps().isMemoryExpensive()) ) {
        considered.add(op);
      } else {
        nonConsidered.add(op);
      }
      return super.visitChildren(op, value);
    }

  }

  private static class DebugOpMemInfo {
    private final int majorID;
    private final int operatorID;
    private final long reservePerInstance;
    private final boolean isMemoryHeavy;
    private final String name;
    private int parallelism;

    DebugOpMemInfo(PhysicalOperator op) {
      this.name = op.getClass().getSimpleName();
      this.majorID = op.getProps().getMajorFragmentId();
      this.operatorID = op.getProps().getLocalOperatorId();
      this.reservePerInstance = op.getProps().getMemReserve();
      this.parallelism = 0;
      this.isMemoryHeavy = op.getProps().isMemoryExpensive();
    }

    void incrementParallelism() {
      ++parallelism;
    }

    int getId() {
      return OpProps.buildOperatorId(majorID, operatorID);
    }

    @Override
    public String toString() {
      return String.format("Op %d-%d name %s reserve %d parallelism %d totalReserve %d isMemoryHeavy %b",
        majorID, operatorID, name, reservePerInstance, parallelism, reservePerInstance * parallelism,
        isMemoryHeavy);
    }
  }

  private static String getOpMemoryDetailsString(NodeEndpoint ep, List<PhysicalOperator> considered, List<PhysicalOperator> notConsidered) {
    Map<Integer, DebugOpMemInfo> opToDetailMap = new HashMap<>();

    // group at operator level
    for (PhysicalOperator op : considered) {
      DebugOpMemInfo info = opToDetailMap.computeIfAbsent(op.getId(), x -> new DebugOpMemInfo(op));
      info.incrementParallelism();
    }
    for (PhysicalOperator op : notConsidered) {
      DebugOpMemInfo info = opToDetailMap.computeIfAbsent(op.getId(), x -> new DebugOpMemInfo(op));
      info.incrementParallelism();
    }

    StringBuilder builder = new StringBuilder();
    builder.append("Memory Allocation details for executor end-point ");
    builder.append(EndpointHelper.getMinimalString(ep));
    builder.append(" numOperators ");
    builder.append(opToDetailMap.size());
    builder.append(" cumulativeParallelism ");
    builder.append(considered.size() + notConsidered.size());
    builder.append('\n');

    List<DebugOpMemInfo> opList = new ArrayList<>(opToDetailMap.values());
    opList.sort(Comparator.comparingInt(DebugOpMemInfo::getId));
    for (DebugOpMemInfo entry : opList) {
      builder.append('\t');
      builder.append(entry);
      builder.append('\n');
    }
    return builder.toString();
  }
}
