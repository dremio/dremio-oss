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
package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.record.BatchSchema;

public class EmptyRel extends AbstractRelNode implements Rel, CopyToCluster {

  private final BatchSchema schema;

  public EmptyRel(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, BatchSchema schema) {
    super(cluster, traitSet.replace(LOGICAL));
    this.schema = schema;
    this.rowType = rowType;
    this.digest = schema.toString();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return pw.item("schema", schema.toString());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeTinyCost();
  }

  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new EmptyRel(copier.getCluster(), getTraitSet(), copier.copyOf(getRowType()), schema);
  }
}
