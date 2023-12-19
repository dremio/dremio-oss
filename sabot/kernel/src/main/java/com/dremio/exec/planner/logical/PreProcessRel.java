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

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.exec.exception.UnsupportedOperatorCollector;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.google.common.collect.Lists;

/**
 * This class rewrites all the project expression that contain convert_to/ convert_from
 * to actual implementations.
 * Eg: convert_from(EXPR, 'JSON') is rewritten as convert_fromjson(EXPR)
 *
 * With the actual method name we can find out if the function has a complex
 * output type, and we will fire/ ignore certain rules (merge project rule) based on this fact.
 */
public final class PreProcessRel extends StatelessRelShuttleImpl {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PreProcessRel.class);
  private final UnsupportedOperatorCollector unsupportedOperatorCollector;
  private final UnwrappingExpressionVisitor unwrappingExpressionVisitor;
  private final ConvertItemToInnerMapFunctionVisitor.RewriteItemOperatorVisitor rewriteItemOperatorVisitor;


  public static PreProcessRel createVisitor(RexBuilder rexBuilder) {
    return new PreProcessRel(rexBuilder);
  }

  private PreProcessRel(RexBuilder rexBuilder) {
    super();
    this.unsupportedOperatorCollector = new UnsupportedOperatorCollector();
    this.unwrappingExpressionVisitor = new UnwrappingExpressionVisitor(rexBuilder);
    this.rewriteItemOperatorVisitor = new ConvertItemToInnerMapFunctionVisitor.RewriteItemOperatorVisitor(rexBuilder);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    final List<RexNode> projExpr = Lists.newArrayList();
    for (RexNode rexNode : project.getProjects()) {
      projExpr.add(rexNode.accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor));
    }

    project =  project.copy(project.getTraitSet(),
        project.getInput(),
        projExpr,
        project.getRowType());

    return visitChild(project, 0, project.getInput());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    final RexNode condition = filter.getCondition().accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor);
    filter = filter.copy(
        filter.getTraitSet(),
        filter.getInput(),
        condition);
    return visitChild(filter, 0, filter.getInput());
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    if (aggregate.getAggCallList().stream().anyMatch(call -> {
      if (call.getAggregation().getKind() == SqlKind.LISTAGG) {
        int inputArg = call.getArgList().get(0);
        return call.getCollation().getFieldCollations().size() > 1 ||
          (call.getCollation().getFieldCollations().size() == 1 &&
            call.getCollation().getFieldCollations().get(0).getFieldIndex() != inputArg);
      }
      return false;
    })) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
        "ORDER BY columns must be subset LISTAGG columns");
      throw new UnsupportedOperationException();
    }
    return super.visit(aggregate);
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    final RexNode conditionExpr = join.getCondition().accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor);
    join = join.copy(join.getTraitSet(),
        conditionExpr,
        join.getLeft(),
        join.getRight(),
        join.getJoinType(),
        join.isSemiJoinDone());

    return visitChildren(join);
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    for(RelNode child : union.getInputs()) {
      for(RelDataTypeField dataField : child.getRowType().getFieldList()) {
        if(dataField.getName().contains(StarColumnHelper.STAR_COLUMN)) {
          // see DRILL-2414
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
              "Union-All over schema-less tables must specify the columns explicitly");
          throw new UnsupportedOperationException();
        }
      }
    }

    return visitChildren(union);
  }

  public void convertException() throws SqlUnsupportedException {
    unsupportedOperatorCollector.convertException();
  }

  private static final class UnwrappingExpressionVisitor extends RexShuttle {
    private final RexBuilder rexBuilder;

    private UnwrappingExpressionVisitor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      final List<RexNode> clonedOperands = visitList(call.operands, new boolean[]{true});
      final SqlOperator sqlOperator = call.getOperator();
      return RexUtil.flatten(rexBuilder,
          rexBuilder.makeCall(
              call.getType(),
              sqlOperator,
              clonedOperands));
    }
  }
}
