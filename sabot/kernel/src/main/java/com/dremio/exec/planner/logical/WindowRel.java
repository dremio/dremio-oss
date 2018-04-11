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

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.Order;
import com.dremio.exec.planner.common.WindowRelBase;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class WindowRel extends WindowRelBase implements Rel {
  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param traits
   * @param child   Input relational expression
   * @param rowType Output row type
   * @param groups Windows
   */
  public WindowRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(cluster, traits, child, constants, rowType, groups);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WindowRel(getCluster(), traitSet, sole(inputs), constants, getRowType(), groups);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    final LogicalOperator inputOp = implementor.visitChild(this, 0, getInput());
    com.dremio.common.logical.data.Window.Builder builder = new com.dremio.common.logical.data.Window.Builder();
    final List<String> fields = getRowType().getFieldNames();
    final List<String> childFields = getInput().getRowType().getFieldNames();
    for (Group window : groups) {

      for(RelFieldCollation orderKey : window.orderKeys.getFieldCollations()) {
        builder.addOrdering(new Order.Ordering(orderKey.getDirection(), new FieldReference(fields.get(orderKey.getFieldIndex()))));
      }

      for (int group : BitSets.toIter(window.keys)) {
        FieldReference fr = new FieldReference(childFields.get(group));
        builder.addWithin(fr, fr);
      }

      int groupCardinality = window.keys.cardinality();
      for (Ord<AggregateCall> aggCall : Ord.zip(window.getAggregateCalls(this))) {
        FieldReference ref = new FieldReference(fields.get(groupCardinality + aggCall.i));
        LogicalExpression expr = toExpr(aggCall.e, childFields);
        builder.addAggregation(ref, expr);
      }
    }
    builder.setInput(inputOp);
    com.dremio.common.logical.data.Window frame = builder.build();
    return frame;
  }

  protected LogicalExpression toExpr(AggregateCall call, List<String> fn) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (Integer i : call.getArgList()) {
      args.add(new FieldReference(fn.get(i)));
    }

    // for count(1).
    if (args.isEmpty()) {
      args.add(new ValueExpressions.LongExpression(1l));
    }
    LogicalExpression expr = new FunctionCall(call.getAggregation().getName().toLowerCase(), args);
    return expr;
  }
}


