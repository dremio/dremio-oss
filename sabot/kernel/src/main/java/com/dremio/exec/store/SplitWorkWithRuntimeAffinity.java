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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.util.rhash.RendezvousPageHasher;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.google.common.collect.FluentIterable;

/**
 * Split work affinity is computed at run time in case its not provided
 */
public class SplitWorkWithRuntimeAffinity extends SplitWork {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitWorkWithRuntimeAffinity.class);
  private static final int MAX_AFFINITY_NODES = 3;

  private List<EndpointAffinity> runTimeAffinity = new ArrayList<>();

  public SplitWorkWithRuntimeAffinity(PartitionChunkMetadata partitionChunk, DatasetSplit datasetSplit, ExecutionNodeMap nodeMap,
                                      DistributionAffinity affinityType) {
    super(partitionChunk, datasetSplit, nodeMap, affinityType);
  }

  @Override
  public int compareTo(CompleteWork o) {
    return super.compareTo(o);
  }

  @Override
  public List<EndpointAffinity> getAffinity() {
    if (affinityType == DistributionAffinity.HARD) {
      return Collections.emptyList();
    }

    if (runTimeAffinity.size() != 0) {
      return runTimeAffinity;
    }

    logger.debug("Affinity not available will compute it now");

    RendezvousPageHasher hasher = new RendezvousPageHasher(nodeMap.getExecutors());

    //As path and offset is not available here
    //Affinnity is computed on dataSplit, which contains size of split and data-slices
    NodeEndpoint[] nodeEndPoints = hasher.getEndpoints(getDatasetSplit().getSplitExtendedProperty().toStringUtf8(),
            getDatasetSplit().getSize());
    double affinity = 1.0;
    for(NodeEndpoint nodeEndpoint : nodeEndPoints) {
      runTimeAffinity.add(new EndpointAffinity(nodeEndpoint, affinity, false, Integer.MAX_VALUE));
      affinity = affinity / 2;
    }

    return runTimeAffinity;
  }

  public static Iterator<SplitWork> transform(Iterator<PartitionChunkMetadata> splits, final ExecutionNodeMap nodeMap, final DistributionAffinity affinityType) {
    return FluentIterable.from(() -> splits)
      .transformAndConcat(partitionChunk -> FluentIterable.from(partitionChunk.getDatasetSplits())
        .transform(datasetSplit -> (SplitWork) new SplitWorkWithRuntimeAffinity(partitionChunk, datasetSplit, nodeMap, affinityType)))
      .iterator();
  }

  public static Iterator<SplitWork> transform(final TableMetadata dataset, ExecutionNodeMap nodeMap, DistributionAffinity affinityType){
    return transform(dataset.getSplits(), nodeMap, affinityType);
  }


}
