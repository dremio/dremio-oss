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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableList;

/**
 * The MEDIAN aggregate function can be rewritten as a PERCENTILE_CONT(0.5). For example, the following query:
 *
 *  SELECT MEDIAN(age) FROM people
 *
 *  can be rewritten as:
 *
 *  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) FROM people
 *
 *  In general median is a window function that returns the median value of a range of values.
 *  It is a specific case of PERCENTILE_CONT, so the following:
 *
 *  MEDIAN(median_arg)
 *
 *  is equivalent to:
 *
 *  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_arg)
 */
public class MedianRewriteRule extends RelOptRule {
  public static final MedianRewriteRule INSTANCE = new MedianRewriteRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private MedianRewriteRule(RelBuilderFactory factory) {
    super(
      operand(Aggregate.class, any()),
      factory,
      "MedianRewriteRule");
  }

  @Override
  public boolean matches(RelOptRuleCall relOptRuleCall) {
    final Aggregate aggregate = relOptRuleCall.rel(0);
    final List<AggregateCall> aggCallList = aggregate.getAggCallList();
    for (AggregateCall aggregateCall : aggCallList) {
      //Median/Percentil rewrite rule doesn't handle the filter at all currently.
      //So when it fires, it does an incorrect transformation.
      //Each rule needs to correctly transform, regardless of whatever other rules might be out there.
      //As long as the filter rewrite rule is in the same phase (or earlier) than the percentile rewrite,
      //it should be fine.
      if(aggregateCall.getAggregation() == DremioSqlOperatorTable.MEDIAN
          && !aggregateCall.hasFilter()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    final Aggregate aggregate = relOptRuleCall.rel(0);
    final RelNode originalInput = aggregate.getInput();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = relOptRuleCall.builder();

    // Constructing the "0.5" needed for "PERCENTILE_CONT(0.5)"
    final RexLiteral fiftyRexLiteral = rexBuilder.makeExactLiteral(new BigDecimal(.5));
    final List<RexNode> projNodes = MoreRelOptUtil.identityProjects(originalInput.getRowType());
    projNodes.add(fiftyRexLiteral);

    // Rewriting the aggregates
    List<AggregateCall> rewrittenAggregateCalls = getRewrittenAggregateCalls(relOptRuleCall, projNodes.size());

    // Constructing the GROUP BY call
    final RelBuilder.GroupKey groupKey = relBuilder
      .push(originalInput)
      .groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());

    // Forming the rewritten query
    final RelNode rewrittenQuery = relBuilder
      .project(projNodes)
      .aggregate(groupKey, rewrittenAggregateCalls)
      .build();

    // Casting since we need to honor the nullability of the double
    // The percentile functions expect a double return type. See if we need to add a cast
    final RelNode castProject = MoreRelOptUtil.createCastRel(rewrittenQuery, aggregate.getRowType());

    relOptRuleCall.transformTo(castProject);
  }

  private static List<AggregateCall> getRewrittenAggregateCalls(RelOptRuleCall relOptRuleCall, int numProjections) {
    final Aggregate aggregate = relOptRuleCall.rel(0);
    final RelNode originalInput = aggregate.getInput();

    // An aggregate could have multiple aggregate calls.
    // Rewrite all the ones that are a MEDIAN
    List<AggregateCall> rewrittenAggregateCalls = new ArrayList<AggregateCall>();
    List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
    for(int i = 0; i < aggregateCalls.size(); i++) {
      AggregateCall aggregateCall = aggregateCalls.get(i);
      AggregateCall rewrittenAggregateCall;

      if(aggregateCall.getAggregation() == DremioSqlOperatorTable.MEDIAN) {
        // Constructing the "WITHIN GROUP (ORDER BY median_arg)"
        int medianArgumentIndex = aggregateCall.getArgList().get(0);
        final RelCollation withinGroupRelCollation = RelCollations.of(new RelFieldCollation(medianArgumentIndex));

        // Construct PERCENTILE_CONT call
        rewrittenAggregateCall = AggregateCall.create(
          SqlStdOperatorTable.PERCENTILE_CONT,
          /*distinct*/ false,
          /*approximate*/false,
          // The .5 literal is at the end of the list.
          ImmutableList.of(numProjections - 1),
          /*filterArg*/-1,
          withinGroupRelCollation,
          /*groupCount*/1,
          originalInput,
          /*type*/null,
          aggregateCall.name);
      } else {
        rewrittenAggregateCall = aggregateCall;
      }

      rewrittenAggregateCalls.add(rewrittenAggregateCall);
    }

    return rewrittenAggregateCalls;
  }
}
