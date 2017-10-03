/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.fragment.ParallelizationInfo.ParallelizationInfoCollector;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.CompleteWork;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class Stats {
  private final ParallelizationInfoCollector collector = new ParallelizationInfoCollector();
  private double maxCost = 0.0;
  private DistributionAffinity distributionAffinity = DistributionAffinity.NONE;
  private final IdentityHashMap<GroupScan, List<CompleteWork>> splitMap = new IdentityHashMap<>();

  public void addParallelizationInfo(ParallelizationInfo parallelizationInfo) {
    collector.add(parallelizationInfo);
  }

  public void addCost(double cost){
    maxCost = Math.max(maxCost, cost);
  }

  public void addMaxWidth(int maxWidth) {
    collector.addMaxWidth(maxWidth);
  }

  public void addMinWidth(int minWidth) {
    collector.addMinWidth(minWidth);
  }

  private void setDistributionAffinity(final DistributionAffinity distributionAffinity) {
    if (this.distributionAffinity.isLessRestrictiveThan(distributionAffinity)) {
      this.distributionAffinity = distributionAffinity;
    }
  }

  public void addSplits(GroupScan scan, List<CompleteWork> splits) {

    setDistributionAffinity(scan.getDistributionAffinity());

    if (scan.getDistributionAffinity() == DistributionAffinity.HARD) {
      // If the newly added scan has hard affinity make sure, there is no other scan in this fragment.
      Preconditions.checkState(splitMap.isEmpty(),
          "Cannot have two scans with hard affinity requirements in the same fragment");

      final ObjectIntHashMap<NodeEndpoint> maxWidthPerNode = new ObjectIntHashMap<>();
      final ObjectDoubleHashMap<NodeEndpoint> affinityPerNode = new ObjectDoubleHashMap<>();
      for(CompleteWork split : splits) {
        final List<EndpointAffinity> splitAffinity = split.getAffinity();
        Preconditions.checkArgument(splitAffinity != null && splitAffinity.size() == 1,
            "Expected hard affinity work split to contain affinity to exactly one node");
        final EndpointAffinity endpointAffinity = splitAffinity.get(0);
        final NodeEndpoint endpoint = endpointAffinity.getEndpoint();
        final int maxWidth = endpointAffinity.getMaxWidth();
        final double affinity = endpointAffinity.getAffinity();
        maxWidthPerNode.putOrAdd(endpoint, maxWidth, maxWidth);
        affinityPerNode.putOrAdd(endpoint, affinity, affinity);
      }

      final List<EndpointAffinity> affinityList = Lists.newArrayList();
      for(ObjectIntCursor<NodeEndpoint> entry : maxWidthPerNode) {
        affinityList.add(new EndpointAffinity(entry.key, affinityPerNode.get(entry.key), true, entry.value));
      }

      collector.addEndpointAffinities(affinityList);
    } else {
      for(CompleteWork split : splits) {
        collector.addEndpointAffinities(split.getAffinity());
      }
    }

    Preconditions.checkArgument(splitMap.put(scan, splits) == null, "Duplicate split provided.");
  }

  Map<GroupScan, List<CompleteWork>> getSplitMap() {
    return splitMap;
  }

  public ParallelizationInfo getParallelizationInfo() {
    return collector.get();
  }

  @Override
  public String toString() {
    return "Stats [maxCost=" + maxCost +", parallelizationInfo=" + collector.toString() + "]";
  }

  public double getMaxCost() {
    return maxCost;
  }

  public DistributionAffinity getDistributionAffinity() {
    return distributionAffinity;
  }
}
