/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.common;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.common.MoreRelOptUtil.ContainsRexVisitor;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.PrelUtil;

/**
 * Base class for logical and physical Filters implemented in Dremio
 */
public abstract class FilterRelBase extends Filter {
  private final int numConjuncts;
  private final List<RexNode> conjunctions;
  private final boolean hasContains;
  // flatten is written like a scalar function, but cannot be evaluated in a contexts that regularly
  // handle scalar functions, because it produces more than one output row for each input row
  private final boolean hasFlatten;

  protected FilterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == convention;

    // save the number of conjuncts that make up the filter condition such
    // that repeated calls to the costing function can use the saved copy
    conjunctions = RelOptUtil.conjunctions(condition);
    numConjuncts = conjunctions.size();
    // assert numConjuncts >= 1;

    this.hasContains = ContainsRexVisitor.hasContainsCheckOrigin(this, this.getCondition(),-1);

    boolean foundFlatten = false;
    for (RexNode rex : this.getChildExps()) {
      MoreRelOptUtil.FlattenRexVisitor visitor = new MoreRelOptUtil.FlattenRexVisitor();
      if (rex.accept(visitor)) {
        foundFlatten = true;
      }
    }
    this.hasFlatten = foundFlatten;
  }

  public boolean canHaveContains() {
    return false;
  }

  public boolean hasContains() {
    return hasContains;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    if ((hasContains && !canHaveContains()) || hasFlatten) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();
    double inputRows = relMetadataQuery.getRowCount(child);
    double cpuCost = estimateCpuCost(relMetadataQuery);
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, 0, 0);
  }

  protected LogicalExpression getFilterExpression(ParseContext context){
    return RexToExpr.toExpr(context, getInput().getRowType(), getCluster().getRexBuilder(), getCondition());
  }

  /* Given the condition (C1 and C2 and C3 and ... C_n), here is how to estimate cpu cost of FILTER :
  *  Let's say child's rowcount is n. We assume short circuit evaluation will be applied to the boolean expression evaluation.
  *  #_of_comparison = n + n * Selectivity(C1) + n * Selectivity(C1 and C2) + ... + n * Selectivity(C1 and C2 ... and C_n)
  *  cpu_cost = #_of_comparison * DremioCost_COMPARE_CPU_COST;
  */
  private double estimateCpuCost(RelMetadataQuery relMetadataQuery) {
    RelNode child = this.getInput();
    final double rows = relMetadataQuery.getRowCount(child);
    double compNum = rows;
    double rowCompNum = child.getRowType().getFieldCount() * rows ;


    for (int i = 0; i< numConjuncts; i++) {
      RexNode conjFilter = RexUtil.composeConjunction(this.getCluster().getRexBuilder(), conjunctions.subList(0, i + 1), false);
      compNum += RelMdUtil.estimateFilteredRows(child, conjFilter, relMetadataQuery);
    }

    return compNum * DremioCost.COMPARE_CPU_COST + rowCompNum * DremioCost.COPY_COST;
  }
}
