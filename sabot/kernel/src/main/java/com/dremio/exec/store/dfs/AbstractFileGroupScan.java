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
package com.dremio.exec.store.dfs;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

public abstract class AbstractFileGroupScan extends AbstractGroupScan {

  private final DistributionAffinity affinity;

  public AbstractFileGroupScan(TableMetadata dataset, List<SchemaPath> columns) {
    super(dataset, columns);
    this.affinity = dataset.getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public final int getMinParallelizationWidth() {
    if(affinity != DistributionAffinity.HARD){
      return 1;
    }

    final Set<String> nodes = new HashSet<>();
    Iterator<DatasetSplit> iter = dataset.getSplits();
    while(iter.hasNext()){
      DatasetSplit split = iter.next();
      for(Affinity a : split.getAffinitiesList()){
        nodes.add(a.getHost());
      }
    }

    return nodes.size();
  }

  @Override
  public final DistributionAffinity getDistributionAffinity() {
    return affinity;
  }
}
