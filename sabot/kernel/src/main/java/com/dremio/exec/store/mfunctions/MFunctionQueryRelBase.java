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
package com.dremio.exec.store.mfunctions;

import com.dremio.exec.store.MFunctionCatalogMetadata;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Rel base for Metadata Functions RelNodes Eg: select * from table(table_history('call_center'))
 * table_history,table_snapshot,table_manifesTs are metadata functions. Any new table format should
 * extend this to support these metadata functions
 */
abstract class MFunctionQueryRelBase extends AbstractRelNode {

  final MFunctionCatalogMetadata tableMetadata;
  final RelDataType rowType;
  final String user;
  final String metadataLocation;

  MFunctionQueryRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      MFunctionCatalogMetadata tableMetadata,
      String user,
      String metadataLocation) {
    super(cluster, traitSet);
    this.tableMetadata = tableMetadata;
    this.user = user;
    this.rowType = rowType;
    this.metadataLocation = metadataLocation;
  }

  public MFunctionCatalogMetadata getTableMetadata() {
    return tableMetadata;
  }

  public String getUser() {
    return user;
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

  /** Check if plugin provides this details. */
  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return 1000;
  }

  @Override
  protected RelDataType deriveRowType() {
    return rowType;
  }
}
