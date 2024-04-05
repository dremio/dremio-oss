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

import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.AssignmentCreator;
import com.dremio.exec.store.schedule.AssignmentCreator2;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.store.schedule.HardAssignmentCreator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapping class that allows us to add additional information to each fragment node for planning
 * purposes.
 */
public class Wrapper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Wrapper.class);

  private final Fragment node;
  private final int majorFragmentId;
  private int width = -1;
  private final Stats stats;
  private boolean statsComputed;
  private boolean endpointsAssigned;
  private long initialAllocation = 0;
  private long maxMemoryAllocationPerNode = Long.MAX_VALUE;
  private final IdentityHashMap<GroupScan, ListMultimap<Integer, CompleteWork>> splitSets =
      new IdentityHashMap<>();
  private boolean strictDependency =
      false; // true if the parallelization must exactly match the dependency's.

  // List of fragments this particular fragment depends on for determining its parallelization and
  // endpoint assignments.
  private final List<Wrapper> fragmentDependencies = Lists.newArrayList();

  // a list of assigned endpoints. Technically, there could repeated endpoints in this list if we'd
  // like to assign the
  // same fragment multiple times to the same endpoint.
  private final List<NodeEndpoint> endpoints = Lists.newLinkedList();

  private int assignedWeight = 0;

  public Wrapper(Fragment node, int majorFragmentId) {
    this.majorFragmentId = majorFragmentId;
    this.node = node;
    this.stats = new Stats();
  }

  public Map<GroupScan, ListMultimap<Integer, CompleteWork>> getSplitSets() {
    return splitSets;
  }

  public Stats getStats() {
    return stats;
  }

  public void resetAllocation() {
    initialAllocation = 0;
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    Preconditions.checkState(this.width == -1);
    this.width = width;
  }

  public void setAssignedWeight(int newWeight) {
    this.assignedWeight = newWeight;
  }

  public int getAssignedWeight() {
    return this.assignedWeight;
  }

  public Fragment getNode() {
    return node;
  }

  public long getInitialAllocation() {
    return initialAllocation;
  }

  public void addAllocation(PhysicalOperator pop) {
    initialAllocation += pop.getProps().getMemReserve();
    //    logger.debug("Incrementing initialAllocation by {} to {}. Pop: {}",
    // pop.getInitialAllocation(), initialAllocation, pop);
  }

  @VisibleForTesting
  public void overrideEndpoints(List<NodeEndpoint> endpoints) {
    endpointsAssigned = true;
    this.endpoints.addAll(endpoints);
  }

  public void assignEndpoints(
      ParallelizationParameters parameters, List<NodeEndpoint> assignedEndpoints)
      throws PhysicalOperatorSetupException {
    Preconditions.checkState(!endpointsAssigned);
    endpointsAssigned = true;

    endpoints.addAll(assignedEndpoints);

    final Map<GroupScan, List<CompleteWork>> splitMap = stats.getSplitMap();
    for (GroupScan scan : splitMap.keySet()) {
      final ListMultimap<Integer, CompleteWork> assignments;
      if (stats.getDistributionAffinity() == DistributionAffinity.HARD) {
        assignments = HardAssignmentCreator.INSTANCE.getMappings(endpoints, splitMap.get(scan));
      } else {
        if (parameters.useNewAssignmentCreator()) {
          assignments =
              AssignmentCreator2.getMappings(
                  endpoints, splitMap.get(scan), parameters.getAssignmentCreatorBalanceFactor());
        } else {
          assignments = AssignmentCreator.getMappings(endpoints, splitMap.get(scan));
        }
      }
      splitSets.put(scan, assignments);
    }

    // Set the endpoints for this (one at most) sending exchange.
    if (node.getSendingExchange() != null) {
      node.getSendingExchange().setupSenders(majorFragmentId, endpoints);
    }

    // Set the endpoints for each incoming exchange within this fragment.
    for (ExchangeFragmentPair e : node.getReceivingExchangePairs()) {
      e.getExchange().setupReceivers(majorFragmentId, endpoints);
    }
  }

  @Override
  public String toString() {
    return "FragmentWrapper [majorFragmentId="
        + majorFragmentId
        + ", width="
        + width
        + ", stats="
        + stats
        + "]";
  }

  public List<NodeEndpoint> getAssignedEndpoints() {
    Preconditions.checkState(endpointsAssigned);
    return ImmutableList.copyOf(endpoints);
  }

  public NodeEndpoint getAssignedEndpoint(int minorFragmentId) {
    Preconditions.checkState(endpointsAssigned);
    return endpoints.get(minorFragmentId);
  }

  public long getMemoryAllocationPerNode() {
    return maxMemoryAllocationPerNode;
  }

  public void setMemoryAllocationPerNode(long memoryLimit) {
    maxMemoryAllocationPerNode = memoryLimit;
  }

  /**
   * Add a parallelization dependency on given fragment.
   *
   * @param dependsOn
   */
  public void addFragmentDependency(Wrapper dependsOn) {
    fragmentDependencies.add(dependsOn);
  }

  public boolean isStrictDependency() {
    return strictDependency;
  }

  public void setStrictDependency() {
    strictDependency = true;
  }

  /**
   * Is the stats computation done for this fragment?
   *
   * @return
   */
  public boolean isStatsComputationDone() {
    return statsComputed;
  }

  /** Mark the end of stats computation */
  public void statsComputationDone() {
    statsComputed = true;
  }

  /**
   * Is the endpoints assignment done for this fragment?
   *
   * @return
   */
  public boolean isEndpointsAssignmentDone() {
    return endpointsAssigned;
  }

  /**
   * Get the list of fragements this particular fragment depends for determining its
   *
   * @return
   */
  public List<Wrapper> getFragmentDependencies() {
    return ImmutableList.copyOf(fragmentDependencies);
  }
}
