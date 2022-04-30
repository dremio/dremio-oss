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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;

/**
 * We currently do not natively support the following type of query:
 *
 * SELECT SUM(salary) FILTER (WHERE gender = 'F')
 * FROM EMP
 *
 * Luckily that query is just syntax sugar for the following query:
 *
 * SELECT SUM(gender = 'F' ? salary : 0)
 * FROM EMP
 *
 * We can implement that by applying a transformation on the query plan.
 * The original query has the query plan:
 *
 * LogicalAggregate(group=[{}], EXPR$0=[SUM($0) FILTER $1])
 *   LogicalProject(sales=[$0], $f1=[IS TRUE(=($1, 'F'))])
 *     ScanCrel(table=[cp."emp.json"], columns=[`salary`, `gender`], splits=[1])
 *
 * And we can rewrite it to:
 * LogicalAggregate(group=[{}], EXPR$0=[SUM($2)])
 *    LogicalProject(salary=[$0], $f1=[IS TRUE(=($1, 'F'))], $f2=[CASE WHEN $1 THEN $0 ELSE 0 END])
 *      ScanCrel(table=[cp."emp.json"], columns=[`salary`, `gender`], splits=[1])
 */
public final class AggregateFilterToCaseRule extends RelOptRule {
  public static final AggregateFilterToCaseRule INSTANCE = new AggregateFilterToCaseRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private AggregateFilterToCaseRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, "AggregateFilterToCaseRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> aggregateCallList = aggregate.getAggCallList();
    for (AggregateCall aggregateCall : aggregateCallList) {
      if(aggregateCall.hasFilter()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode originalInput = aggregate.getInput();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = (RelBuilder) call.builder();

    // Rewrite the aggregates and form the case statements
    final List<RexNode> projectNodes = MoreRelOptUtil.identityProjects(originalInput.getRowType());
    final Pair<List<AggregateCall>, List<RexCall>> rewrittenAggregatesAndCaseList = rewriteAggregateCalls(
      projectNodes.size(),
      aggregate,
      originalInput,
      rexBuilder);
    final List<AggregateCall> rewrittenAggregateCalls = rewrittenAggregatesAndCaseList.left;
    final List<RexCall> caseCalls = rewrittenAggregatesAndCaseList.right;
    projectNodes.addAll(caseCalls);

    // Constructing the GROUP BY call
    final RelBuilder.GroupKey groupKey = relBuilder
      .push(originalInput)
      .groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());

    // Forming the rewritten query
    final RelNode rewrittenQuery = relBuilder
      .project(projectNodes)
      .aggregate(groupKey, rewrittenAggregateCalls)
      .build();

    call.transformTo(rewrittenQuery);
  }

  private static Pair<List<AggregateCall>, List<RexCall>> rewriteAggregateCalls(
    int numberOfProjections,
    Aggregate aggregate,
    RelNode originalInput,
    RexBuilder rexBuilder) {
    final List<AggregateCall> rewrittenAggregateCalls = new ArrayList<>();
    final List<AggregateCall> aggregateCalls = aggregate.getAggCallList();

    final List<RexCall> caseRexCalls = new ArrayList<>();

    for(AggregateCall aggregateCall : aggregateCalls) {
      final AggregateCall rewrittenAggregateCall;
      if(!aggregateCall.hasFilter()) {
        rewrittenAggregateCall = aggregateCall;
      } else {
        // Create a new Aggregate Call without the filter,
        // but aggregating on the case statement (that we will add to the project below):
        rewrittenAggregateCall = AggregateCall.create(
          aggregateCall.getAggregation(),
          aggregateCall.isDistinct(),
          aggregateCall.isApproximate(),
          ImmutableList.of(numberOfProjections + caseRexCalls.size()),
          /* filterArg */ -1,
          aggregateCall.getCollation(),
          aggregate.getGroupCount(),
          originalInput,
          aggregateCall.type,
          aggregateCall.name);

        // Create a CASE call from the filter:
        RexCall caseRexCall = createCaseCallFromAggregateFilter(aggregateCall, originalInput, rexBuilder);
        caseRexCalls.add(caseRexCall);
      }

      rewrittenAggregateCalls.add(rewrittenAggregateCall);
    }

    return Pair.of(rewrittenAggregateCalls, caseRexCalls);
  }

  private static RexCall createCaseCallFromAggregateFilter(
    AggregateCall aggregateCall,
    RelNode originalInput,
    RexBuilder rexBuilder) {
    // CASE(<filterArg>, <aggregateArg>, <elseValue>)
    final RexNode filterArg = createFilterArgForCaseCall(aggregateCall, originalInput);
    final Pair<RexNode, RelDataType> aggregateArgAndRelDataType = createAggregateArgAndRelDataTypeForCaseCall(aggregateCall, originalInput, rexBuilder);
    final RexNode elseValue = rexBuilder.makeNullLiteral(aggregateArgAndRelDataType.right);

    final RexCall caseRexCall = (RexCall) rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      filterArg,
      aggregateArgAndRelDataType.left,
      elseValue);

    return caseRexCall;
  }

  private static RexNode createFilterArgForCaseCall(
    AggregateCall aggregateCall,
    RelNode originalInput) {
    final int filterArgIndex = aggregateCall.filterArg;
    final RelDataType filterArgRelDataType = originalInput
      .getRowType()
      .getFieldList()
      .get(filterArgIndex)
      .getValue();
    final RexNode filterArg = new RexInputRef(filterArgIndex, filterArgRelDataType);

    return filterArg;
  }

  private static Pair<RexNode, RelDataType> createAggregateArgAndRelDataTypeForCaseCall(
    AggregateCall aggregateCall,
    RelNode originalInput,
    RexBuilder rexBuilder) {
    // Some aggregates don't have an aggregate argument (like COUNT):
    final RexNode aggregateArg;
    final RelDataType aggregateArgRelDataType;
    if(aggregateCall.getArgList().size() == 0) {
      // COUNT(*) FILTER (WHERE gender = 'F')
      // => COUNT(gender = 'F' ? 'dummy' : null)
      aggregateArg = rexBuilder.makeLiteral(false);
      aggregateArgRelDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    } else {
      // AGG(<aggregateArg>) FILTER (WHERE <cond>)
      // => AGG(<cond> ? <aggregateArg> : null)
      final int aggregateArgumentIndex = aggregateCall.getArgList().get(0);
      aggregateArgRelDataType = originalInput
        .getRowType()
        .getFieldList()
        .get(aggregateArgumentIndex)
        .getValue();
      aggregateArg = new RexInputRef(aggregateArgumentIndex, aggregateArgRelDataType);
    }

    return Pair.of(aggregateArg, aggregateArgRelDataType);
  }
}
