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
package com.dremio.plugins.sysflight;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

abstract class SysTableFunctionQueryRelBase extends AbstractRelNode {

  private SysTableFunctionCatalogMetadata metadata;
  private RelDataType rowType;
  private final String user;

  public SysTableFunctionQueryRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      SysTableFunctionCatalogMetadata metadata,
      String user) {
    super(cluster, traitSet);
    this.metadata = metadata;
    this.user = user;
    this.rowType = rowType;
  }

  /**
   * Subclasses must override copy to avoid problems where duplicate scan operators are created due
   * to the same (reference-equality) Prel being used multiple times in the plan. The copy
   * implementation in AbstractRelNode just returns a reference to "this".
   */
  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return 1000;
  }

  @Override
  protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("source", getMetadata().getStoragePluginId().getName())
        .item("FunctionName", getMetadata().getSysTableFunction().getName())
        .item(
            "FunctionParameters",
            getMetadata()
                .getSysTableFunction()
                .getParameters()
                .toString()
                .replaceAll("\n", "")
                .replaceAll(" ", ""));
  }

  public SysTableFunctionCatalogMetadata getMetadata() {
    return metadata;
  }

  public String getUser() {
    return user;
  }
}
