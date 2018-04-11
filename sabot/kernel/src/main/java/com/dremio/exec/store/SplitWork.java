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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class SplitWork implements CompleteWork {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitWork.class);

  private final DatasetSplit split;
  private final ExecutionNodeMap nodeMap;
  private final DistributionAffinity affinityType;

  public SplitWork(DatasetSplit split, ExecutionNodeMap nodeMap, DistributionAffinity affinityType) {
    super();
    this.split = split;
    this.nodeMap = nodeMap;
    this.affinityType = affinityType;
  }

  @Override
  public int compareTo(CompleteWork o) {
    return Long.compare(getTotalBytes(), o.getTotalBytes());
  }

  @Override
  public long getTotalBytes() {
    return split.getSize();
  }


  public DatasetSplit getSplit() {
    return split;
  }

  @Override
  public List<EndpointAffinity> getAffinity() {
    if(split.getAffinitiesList() == null){
      return ImmutableList.<EndpointAffinity>of();
    }

    List<EndpointAffinity> endpoints = new ArrayList<>();
    for(Affinity a : split.getAffinitiesList()){
      NodeEndpoint endpoint = nodeMap.getEndpoint(a.getHost());
      if(endpoint != null){
        endpoints.add(new EndpointAffinity(endpoint, a.getFactor(), affinityType == DistributionAffinity.HARD, affinityType == DistributionAffinity.HARD ? 1 : Integer.MAX_VALUE));
      } else {
        if (affinityType == DistributionAffinity.HARD) {
          // Throw an error if there is no endpoint on host
          throw UserException.resourceError()
              .message("No executors are available for data with hard affinity. " +
                  "You may consider using \"registration.publish-host\" property if your network rules change.")
              .addContext("available executors %s", nodeMap.getHosts())
              .addContext("data affinity", a.getHost())
              .build(logger);
        }
      }
    }
    return endpoints;
  }

  public static Iterator<SplitWork> transform(Iterator<DatasetSplit> splits, final ExecutionNodeMap nodeMap, final DistributionAffinity affinityType){
    return Iterators.transform(splits, new Function<DatasetSplit, SplitWork>(){
      @Override
      public SplitWork apply(DatasetSplit split) {
        return new SplitWork(split, nodeMap, affinityType);
      }});
  }

  public static Iterator<SplitWork> transform(final TableMetadata dataset, ExecutionNodeMap nodeMap, DistributionAffinity affinityType){
    return transform(dataset.getSplits(), nodeMap, affinityType);
  }


}
