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
package com.dremio.exec.tablefunctions.copyerrors;

import com.dremio.exec.planner.sql.handlers.query.CopyErrorContext;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

public abstract class CopyErrorsRelBase extends AbstractRelNode {
  private final CopyErrorContext context;
  private final CopyErrorsCatalogMetadata metadata;

  protected CopyErrorsRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      CopyErrorContext context,
      CopyErrorsCatalogMetadata metadata) {
    super(cluster, traitSet);
    this.context = Preconditions.checkNotNull(context, "CopyErrors content must be not null");
    this.metadata = metadata;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeTinyCost();
  }

  /**
   * Subclasses must override copy to avoid problems where duplicate scan operators are created due
   * to the same (reference-equality) Prel being used multiple times in the plan. The copy
   * implementation in AbstractRelNode just returns a reference to "this".
   */
  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

  @Override
  public RelOptTable getTable() {
    return context.getResolvedTargetTable();
  }

  @Override
  protected RelDataType deriveRowType() {
    return metadata.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (context.getResolvedTargetTable() != null) {
      pw.item("table", context.getResolvedTargetTable().getQualifiedName());
    }
    if (context.getCopyIntoJobId() != null) {
      pw.item("job_id", context.getCopyIntoJobId());
    }
    pw.item("strict_consistency", context.isStrictConsistency());
    return pw;
  }

  protected CopyErrorContext getContext() {
    return context;
  }

  protected CopyErrorsCatalogMetadata getMetadata() {
    return metadata;
  }
}
