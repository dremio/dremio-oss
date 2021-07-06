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
package com.dremio.exec.tablefunctions;

import java.util.List;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;

/**
 * Dremio logical RelNode implementation for an external query scan.
 */
public final class ExternalQueryScanCrel extends ExternalQueryRelBase implements CopyToCluster {
  public ExternalQueryScanCrel(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType,
                        StoragePluginId pluginId, String sql, BatchSchema schema) {
    super(cluster, traitSet, rowType, pluginId, sql, schema);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ExternalQueryScanCrel(
      getCluster(),
      getTraitSet(),
      getRowType(),
      pluginId,
      sql,
      batchSchema);
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new ExternalQueryScanCrel(
      copier.getCluster(),
      getTraitSet(),
      getRowType(),
      pluginId,
      sql,
      batchSchema);
  }
}
