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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.logical.data.GroupingAggregate;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.planner.common.AggregateRelBase;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.torel.ConversionContext;
import com.google.common.collect.Lists;

/**
 * Aggregation implemented in Dremio.
 */
public class AggregateRel extends AggregateRelBase implements Rel {
  /** Creates a AggregateRel. */
  public AggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new AggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {

    GroupingAggregate.Builder builder = GroupingAggregate.builder();
    builder.setInput(implementor.visitChild(this, 0, getInput()));
    final List<String> childFields = getInput().getRowType().getFieldNames();
    final List<String> fields = getRowType().getFieldNames();

    for (int group : BitSets.toIter(groupSet)) {
      FieldReference fr = new FieldReference(childFields.get(group));
      builder.addKey(fr, fr);
    }

    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      FieldReference ref = new FieldReference(fields.get(groupSet.cardinality() + aggCall.i));
      LogicalExpression expr = toExpr(aggCall.e, childFields, implementor);
      builder.addExpr(ref, expr);
    }

    return builder.build();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    for (AggregateCall aggCall : getAggCallList()) {
      // For avg, stddev_pop, stddev_samp, var_pop and var_samp, the ReduceAggregatesRule is supposed
      // to convert them to use sum and count. Here, we make the cost of the original functions high
      // enough such that the planner does not choose them and instead chooses the rewritten functions.
      if (aggCall.getAggregation().getKind() == SqlKind.AVG
          || aggCall.getAggregation().getKind() == SqlKind.STDDEV_SAMP
          || aggCall.getAggregation().getKind() == SqlKind.STDDEV_POP
          || aggCall.getAggregation().getKind() == SqlKind.VAR_POP
          || aggCall.getAggregation().getKind() == SqlKind.VAR_SAMP) {
        return planner.getCostFactory().makeHugeCost();
      }
    }

    final double rowCount = relMetadataQuery.getRowCount(this);
    final double childRowCount = relMetadataQuery.getRowCount(this.getInput());
    // Aggregates with more aggregate functions cost a bit more
    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
    return ((Factory) planner.getCostFactory()).makeCost(rowCount,childRowCount * multiplier * DremioCost.FUNC_CPU_COST, 0, 0);
  }

  public static LogicalExpression toExpr(AggregateCall call, List<String> fn, LogicalPlanImplementor implementor) {
    List<LogicalExpression> args = Lists.newArrayList();
    for(Integer i : call.getArgList()) {
      args.add(new FieldReference(fn.get(i)));
    }

    // for count(1).
    if (args.isEmpty()) {
      args.add(new ValueExpressions.LongExpression(1l));
    }
    LogicalExpression expr = FunctionCallFactory.createExpression(call.getAggregation().getName().toLowerCase(), args);
    return expr;
  }

  public static AggregateRel convert(GroupingAggregate groupBy, ConversionContext value)
      throws InvalidRelException {
    throw new UnsupportedOperationException();
  }

}
