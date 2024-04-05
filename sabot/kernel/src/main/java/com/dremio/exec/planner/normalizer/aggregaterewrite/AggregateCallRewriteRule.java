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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.CompositeList;

/**
 * Planner rule that reduces aggregate functions in {@link Aggregate}s to simpler forms.
 *
 * <p>Modeled off of Calcite's AggregateReduceFunctionsRule.java Keep in sync for bug fixes, but it
 * shouldn't happen very often.
 */
public final class AggregateCallRewriteRule extends RelRule<RelRule.Config> {
  public static final AggregateCallRewriteRule INSTANCE = new AggregateCallRewriteRule();

  private static final ImmutableSet<AggregateCallConvertlet> convertlets =
      ImmutableSet.of(
          AggregateFilterToCaseConvertlet.INSTANCE,
          ApproxPercentileConvertlet.INSTANCE,
          CountDistinctConvertlet.INSTANCE,
          MedianConvertlet.INSTANCE,
          NDVConvertlet.INSTANCE);

  public AggregateCallRewriteRule() {
    super(
        Config.EMPTY
            .withDescription("AggregateCallRewriteRule")
            .withOperandSupplier(op -> op.operand(Aggregate.class).anyInputs()));
  }

  @Override
  public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  private static void reduceAggs(RelOptRuleCall ruleCall, Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    final List<RexNode> projList = new ArrayList<>();

    // pass through group key
    for (int i = 0; i < groupCount; ++i) {
      projList.add(rexBuilder.makeInputRef(oldAggRel, i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new agg function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(reduceAgg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount = inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(
          inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    newCalcRel(relBuilder, oldAggRel.getRowType(), projList);
    ruleCall.transformTo(relBuilder.build());
  }

  private static RexNode reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    ConvertletContext context =
        new ConvertletContext(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
    Optional<AggregateCallConvertlet> candidate =
        convertlets.stream().filter(convertlet -> convertlet.matches(context)).findAny();
    if (candidate.isPresent()) {
      return candidate.get().convertCall(context);
    }

    // preserve original call
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int nGroups = oldAggRel.getGroupCount();
    List<RelDataType> oldArgTypes =
        SqlTypeUtil.projectTypes(oldAggRel.getInput().getRowType(), oldCall.getArgList());
    return rexBuilder.addAggCall(oldCall, nGroups, newCalls, aggCallMapping, oldArgTypes);
  }

  /**
   * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored into Aggregate and
   * subclasses - but it's only needed for some subclasses.
   *
   * @param relBuilder Builder of relational expressions; at the top of its stack is its input
   * @param oldAggregate LogicalAggregate to clone.
   * @param newCalls New list of AggregateCalls
   */
  private static void newAggregateRel(
      RelBuilder relBuilder, Aggregate oldAggregate, List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()), newCalls);
  }

  /**
   * Add a calc with the expressions to compute the original agg calls from the decomposed ones.
   *
   * @param relBuilder Builder of relational expressions; at the top of its stack is its input
   * @param rowType The output row type of the original aggregate.
   * @param exprs The expressions to compute the original agg calls.
   */
  private static void newCalcRel(RelBuilder relBuilder, RelDataType rowType, List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }
}
