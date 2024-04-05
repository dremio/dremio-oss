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
package com.dremio.exec.store.deltalake;

import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/** Hive version of DeltaLake commit log reader prel for added and removed paths */
@Options
public class HiveDeltaLakeCommitLogScanPrel extends DeltaLakeCommitLogScanPrel {
  public HiveDeltaLakeCommitLogScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableMetadata tableMetadata,
      boolean arrowCachingEnabled,
      boolean scanForAddedPaths) {
    super(cluster, traitSet, tableMetadata, arrowCachingEnabled, scanForAddedPaths);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    if (getTableMetadata().getReadDefinition().getManifestScanStats() != null) {
      return getTableMetadata().getReadDefinition().getManifestScanStats().getRecordCount();
    }
    return getTableMetadata().getSplitCount();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HiveDeltaLakeCommitLogScanPrel(
        getCluster(),
        getTraitSet(),
        getTableMetadata(),
        isArrowCachingEnabled(),
        isScanForAddedPaths());
  }
}
