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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Project;
import com.google.common.base.Preconditions;

public class ProjectForFlattenRel extends SingleRel implements Rel {

  private final List<RexNode> itemExprs;
  private final List<RexNode> projExprs;

  protected ProjectForFlattenRel(RelOptCluster cluster,
                                      RelTraitSet traits,
                                      RelNode child,
                                      RelDataType rowType,
                                      List<RexNode> projExprs,
                                      List<RexNode> itemExprs) {
    super(cluster, traits, child);
    this.projExprs = projExprs;
    this.itemExprs = itemExprs;
    this.rowType = rowType;
    Preconditions.checkArgument(itemExprs != null && !itemExprs.isEmpty());
  }

  public List<RexNode> getProjExprs() {
    return projExprs;
  }

  public List<RexNode> getItemExprs() {
    return itemExprs;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeInfiniteCost();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("projExprs", projExprs).item("itemExprs", this.itemExprs);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    LogicalOperator inputOp = implementor.visitChild(this, 0, getInput());
    Project.Builder builder = Project.builder();
    builder.setInput(inputOp);

    // This is not a complete description of this rel node.  Should also include the itemExprs.
    int index = 0;
    for (RexNode rex : projExprs) {
      LogicalExpression expr = RexToExpr.toExpr(implementor.getContext(), getInput().getRowType(), getCluster().getRexBuilder(), rex);
      builder.addExpr(new NamedExpression(expr, FieldReference.getWithQuotedRef(getRowType().getFieldNames().get(index))));
      index++;
    }
    return builder.build();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ProjectForFlattenRel(getCluster(), traitSet, sole(inputs), this.getRowType(), this.projExprs, this.itemExprs);
  }
}
