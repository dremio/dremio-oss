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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MapSqlType;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.Lists;

/**
 * Converts Item Operator to Item + Inner map function if used on a map input
 */
public  class ConvertItemToInnerMapFunctionVisitor extends StatelessRelShuttleImpl {
  private final RewriteItemOperatorVisitor rewriteItemOperatorVisitor;

  public ConvertItemToInnerMapFunctionVisitor(RexBuilder rexBuilder) {
    rewriteItemOperatorVisitor = new RewriteItemOperatorVisitor(rexBuilder);
  }

  public static RelNode process(RelNode relNode){
    ConvertItemToInnerMapFunctionVisitor itemRewriteVisitor = new ConvertItemToInnerMapFunctionVisitor(relNode.getCluster().getRexBuilder());
    return relNode.accept(itemRewriteVisitor);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    final List<RexNode> projExpr = Lists.newArrayList();
    for(RexNode rexNode : project.getProjects()) {
      projExpr.add(rexNode.accept(rewriteItemOperatorVisitor));
    }

    project =  project.copy(project.getTraitSet(),
      project.getInput(),
      projExpr,
      project.getRowType());

    return visitChild(project, 0, project.getInput());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    final RexNode condition = filter.getCondition().accept(rewriteItemOperatorVisitor);
    filter = filter.copy(
      filter.getTraitSet(),
      filter.getInput(),
      condition);
    return visitChild(filter, 0, filter.getInput());
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    final RexNode conditionExpr = join.getCondition().accept(rewriteItemOperatorVisitor);
    join = join.copy(join.getTraitSet(),
      conditionExpr,
      join.getLeft(),
      join.getRight(),
      join.getJoinType(),
      join.isSemiJoinDone());

    return visitChildren(join);
  }

  public static class RewriteItemOperatorVisitor extends RexShuttle {
    private final RexBuilder rexBuilder;

    public RewriteItemOperatorVisitor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      final List<RexNode> clonedOperands = visitList(call.operands, new boolean[]{true});
      final SqlOperator sqlOperator = call.getOperator();
      if ("item".equalsIgnoreCase(sqlOperator.getName())
        && call.getOperands().get(0).getType() instanceof MapSqlType) {
          MapSqlType mapSqlType = (MapSqlType) call.getOperands().get(0).getType();
          return  rexBuilder.makeCall(
            mapSqlType.getValueType(),
            SqlStdOperatorTable.ITEM,
            Arrays.asList(
              rexBuilder.makeCall(
                DremioSqlOperatorTable.LAST_MATCHING_MAP_ENTRY_FOR_KEY,
                call.getOperands().get(0),
                call.getOperands().get(1)),
              rexBuilder.makeLiteral("value")
            )
          );

      }
      return rexBuilder.makeCall(call.getType(), sqlOperator, clonedOperands);
    }
  }
}
