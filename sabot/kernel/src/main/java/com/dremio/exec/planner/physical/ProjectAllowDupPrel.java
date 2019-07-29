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

package com.dremio.exec.planner.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.google.common.collect.Lists;

public class ProjectAllowDupPrel extends ProjectPrel {

  private ProjectAllowDupPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, child, exps, rowType);
  }

  @Override
  public ProjectAllowDupPrel copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    return ProjectAllowDupPrel.create(getCluster(), traitSet, input, exps, rowType);
  }

  @Override
  protected List<NamedExpression> getProjectExpressions(ParseContext context) {
    List<NamedExpression> expressions = Lists.newArrayList();
    for (Pair<RexNode, String> pair : Pair.zip(exps, getRowType().getFieldNames())) {
      LogicalExpression expr = RexToExpr.toExpr(context, getInput().getRowType(), getCluster().getRexBuilder(), pair.left);
      expressions.add(new NamedExpression(expr, FieldReference.getWithQuotedRef(pair.right)));
    }
    return expressions;
  }

  /**
   * Creates an instance of ProjectAllowDupPrel.
   *
   * @param cluster
   * @param traits
   * @param child
   * @param exps
   * @param rowType
   * @return new instance of ProjectAllowDupPrel
   */
  public static ProjectAllowDupPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
                                           RelDataType rowType) {
    final RelTraitSet adjustedTraits = adjustTraits(cluster, child, exps, traits);
    return new ProjectAllowDupPrel(cluster, adjustedTraits, child, exps, rowType);
  }
}
