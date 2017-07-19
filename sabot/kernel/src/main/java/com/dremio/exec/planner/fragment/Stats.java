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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.fragment.ParallelizationInfo.ParallelizationInfoCollector;
import com.dremio.exec.store.schedule.CompleteWork;
import com.google.common.base.Preconditions;


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

  public void setDistributionAffinity(final DistributionAffinity distributionAffinity) {
    if (this.distributionAffinity.isLessRestrictiveThan(distributionAffinity)) {
      this.distributionAffinity = distributionAffinity;
    }
  }

  public void addEndpointAffinities(Iterator<CompleteWork> workList) {
    while(workList.hasNext()){
      CompleteWork work = workList.next();
      collector.addEndpointAffinities(work.getAffinity());
    }
  }

  public void addSplits(GroupScan scan, List<CompleteWork> splits) {
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
