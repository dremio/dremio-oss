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
package com.dremio.exec.planner.normalizer;

import com.dremio.exec.planner.normalizer.aggregaterewrite.HepRelPassthroughShuttle;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites:
 *
 * <pre>
 * LogicalAggregate(group=[{0}],EXPR$1=[SUM($1)],idArrayAsc=[ARRAY_AGG($2) WITHIN GROUP ([2]) FILTER $3])
 *  LogicalProject(state=[$3],pop=[$2],city=[$0],$f3=[IS TRUE(starts_with($0, 'A':VARCHAR(1)))])
 *    ScanCrel(table=[Samples."samples.dremio.com"."zips.json"], columns=[`city`, `loc`, `pop`, `state`, `_id`], splits=[1])
 *
 * TO:
 *
 * LogicalProject(state=[$0],EXPR$1=[$1],
 *     idArrayAsc=[
 *         $SCALAR_QUERY($cor0, {
 *             LogicalAggregate(group=[{}],idArrayAsc=[ARRAY_AGG($0)])
 *                LogicalProject(city=[$0])
 *                  LogicalFilter(condition=[AND(=($cor0.state, $3), starts_with($0, 'A':VARCHAR(1)))])
 *                    LogicalSort(sort0=[$0],dir0=[DESC])
 *                      LogicalProject(city=[$0],loc=[$1],pop=[$2],state=[$3],_id=[$4])
 *                        ScanCrel(table=[Samples."samples.dremio.com"."zips.json"],columns=[`city`, `loc`, `pop`, `state`, `_id`],splits=[1])
 *         })
 *     ]
 * )
 *  LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])
 *    LogicalProject(state=[$3],pop=[$2])
 *      ScanCrel(table=[Samples."samples.dremio.com"."zips.json"], columns=[`city`, `loc`, `pop`, `state`, `_id`], splits=[1])
 * </pre>
 *
 * Basically, we push the qualifications into a subquery.
 */
public final class QualifiedAggregateToSubqueryRule
    extends RelRule<QualifiedAggregateToSubqueryRule.Config> implements TransformationRule {
  public static final QualifiedAggregateToSubqueryRule INSTANCE = Config.DEFAULT.toRule();

  private QualifiedAggregateToSubqueryRule(QualifiedAggregateToSubqueryRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    // Decorrelation struggles with HepRelVertexes in RexSubquery
    final RelNode input = call.rel(1).accept(new HepRelPassthroughShuttle());

    RexBuilder rexBuilder = call.builder().getRexBuilder();

    List<RexNode> newProjects = new ArrayList<>();
    List<AggregateCall> newAggregateCalls = new ArrayList<>();

    // Group By Columns
    for (int i = 0; i < aggregate.getGroupCount(); i++) {
      RexNode passthrough = rexBuilder.makeInputRef(aggregate, i);
      newProjects.add(passthrough);
    }

    CorrelationId correlationId = aggregate.getCluster().createCorrel();
    RexNode correlatedFilterExpr = createCorrelatedFilter(rexBuilder, aggregate, correlationId);

    int groupCount = aggregate.getGroupCount();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (!needsRewrite(aggregateCall)) {
        RexNode newProject =
            rexBuilder.makeInputRef(
                aggregate.getRowType().getFieldList().get(groupCount + i).getType(),
                groupCount + newAggregateCalls.size());
        newProjects.add(newProject);
        newAggregateCalls.add(aggregateCall);
      } else {
        RelBuilder relBuilder = call.builder().push(input);

        // Add correlated filter, so we only get the results for the group we care about.
        relBuilder.filter(correlatedFilterExpr);

        if (aggregateCall.hasFilter()) {
          RexNode filterExpr = rexBuilder.makeInputRef(input, aggregateCall.filterArg);
          relBuilder.filter(filterExpr);
          aggregateCall = removeFilterQualification(aggregate, input, aggregateCall);
        }

        if (aggregateCall.ignoreNulls()) {
          throw new UnsupportedOperationException("IGNORE / RESPECT NULL is not implemented.");
        }

        if (aggregateCall.isDistinct()) {
          // If the collation does not match up with DISTINCT, then we can not perform this rewrite,
          // since we lose the sort key after applying distinct.
          RelCollation relCollation = aggregateCall.getCollation();
          if (!relCollation.getFieldCollations().isEmpty()) {
            if (relCollation.getFieldCollations().size() != 1) {
              return;
            }

            if (aggregateCall.getArgList().get(0)
                != relCollation.getFieldCollations().get(0).getFieldIndex()) {
              return;
            }
          }

          relBuilder
              // project out only the values that participate in distinct
              .project(
                  aggregateCall.getArgList().stream()
                      .map(arg -> rexBuilder.makeInputRef(relBuilder.peek(), arg))
                      .collect(Collectors.toList()))
              .distinct();

          aggregateCall = removeDistinctQualification(aggregate, input, aggregateCall);
        }

        // SORT has to come after DISTINCT
        if (!aggregateCall.getCollation().getFieldCollations().isEmpty()) {
          relBuilder.sort(relBuilder.fields(aggregateCall.getCollation()));
          aggregateCall = removeCollationQualification(aggregate, input, aggregateCall);
        }

        // project out only the values that are being aggregated
        relBuilder.project(
            aggregateCall.getArgList().stream()
                .map(arg -> rexBuilder.makeInputRef(relBuilder.peek(), arg))
                .collect(Collectors.toList()));

        if (needsRewrite(aggregateCall)) {
          return;
        }

        RexNode newProject;
        if (aggregateCall.getAggregation() == DremioSqlOperatorTable.ARRAY_AGG) {
          RelNode subquery = relBuilder.build();
          newProject = RexSubQuery.array(subquery, correlationId);
        } else {
          relBuilder.aggregate(relBuilder.groupKey(), ImmutableList.of(aggregateCall));
          RelNode subquery = relBuilder.build();
          newProject = RexSubQuery.scalar(subquery, correlationId);
        }

        newProjects.add(newProject);
      }
    }

    RelBuilder relBuilder = call.builder();

    if (aggregate.getGroupCount() == 0 && newAggregateCalls.isEmpty()) {
      relBuilder.values(new String[] {"col"}, 0);
    } else {
      relBuilder
          .push(input)
          .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), newAggregateCalls);
    }

    RelNode rewrittenQuery =
        relBuilder.project(newProjects, aggregate.getRowType().getFieldNames()).build();

    call.transformTo(rewrittenQuery);
  }

  private static RexNode createCorrelatedFilter(
      RexBuilder rexBuilder, Aggregate aggregate, CorrelationId correlationId) {
    RexNode correl = rexBuilder.makeCorrel(aggregate.getRowType(), correlationId);
    List<RexNode> correlatedFilterExprs = new ArrayList<>();
    for (int i = 0; i < aggregate.getGroupCount(); i++) {
      RexNode correlateColumn = rexBuilder.makeFieldAccess(correl, i);
      RexNode inputColumn = rexBuilder.makeInputRef(aggregate, i);
      RexNode correlatedFilterExpr =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, correlateColumn, inputColumn);
      correlatedFilterExprs.add(correlatedFilterExpr);
    }

    RexNode correlatedFilterExpr = RexUtil.composeConjunction(rexBuilder, correlatedFilterExprs);
    return correlatedFilterExpr;
  }

  private static AggregateCall removeFilterQualification(
      Aggregate aggregate, RelNode input, AggregateCall aggregateCall) {
    return AggregateCall.create(
        aggregateCall.getAggregation(),
        aggregateCall.isDistinct(),
        aggregateCall.isApproximate(),
        aggregateCall.ignoreNulls(),
        aggregateCall.getArgList(),
        -1,
        aggregateCall.getCollation(),
        aggregate.getGroupCount(),
        input,
        aggregateCall.getType(),
        aggregateCall.getName());
  }

  private static AggregateCall removeDistinctQualification(
      Aggregate aggregate, RelNode input, AggregateCall aggregateCall) {
    RelCollation relCollation = aggregateCall.getCollation();
    if (!relCollation.getFieldCollations().isEmpty()) {
      RelFieldCollation relFieldCollation = relCollation.getFieldCollations().get(0);
      // We projected out just the argument that participates the distinct,
      // and we asserted it matched up with the distinct qualification
      RelFieldCollation newRelFieldCollation = relFieldCollation.withFieldIndex(0);
      relCollation = RelCollations.of(newRelFieldCollation);
    }

    return AggregateCall.create(
        aggregateCall.getAggregation(),
        false,
        aggregateCall.isApproximate(),
        aggregateCall.ignoreNulls(),
        ImmutableList.of(0), // We projected out just the argument that participates in distinct.
        aggregateCall.filterArg,
        relCollation,
        aggregate.getGroupCount(),
        input,
        aggregateCall.getType(),
        aggregateCall.getName());
  }

  private static AggregateCall removeCollationQualification(
      Aggregate aggregate, RelNode input, AggregateCall aggregateCall) {
    return AggregateCall.create(
        aggregateCall.getAggregation(),
        aggregateCall.isDistinct(),
        aggregateCall.isApproximate(),
        aggregateCall.ignoreNulls(),
        aggregateCall.getArgList(),
        aggregateCall.filterArg,
        RelCollations.EMPTY,
        aggregate.getGroupCount(),
        input,
        aggregateCall.getType(),
        aggregateCall.getName());
  }

  private static boolean needsRewrite(AggregateCall aggCall) {
    // For now, we are only going to rewrite ARRAY_AGG to avoid performance regressions.
    if (!aggCall.getAggregation().equals(DremioSqlOperatorTable.ARRAY_AGG)) {
      return false;
    }

    if (aggCall.hasFilter()) {
      return true;
    }

    if (aggCall.ignoreNulls()) {
      return true;
    }

    if (!aggCall.getCollation().getFieldCollations().isEmpty()) {
      return true;
    }

    if (aggCall.isDistinct()) {
      return true;
    }

    return false;
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withOperandSupplier(
                b ->
                    b.operand(Aggregate.class)
                        .predicate(
                            aggregate ->
                                aggregate.getAggCallList().stream()
                                    .anyMatch(QualifiedAggregateToSubqueryRule::needsRewrite))
                        .oneInput(input -> input.operand(RelNode.class).anyInputs()))
            .as(Config.class);

    @Override
    default QualifiedAggregateToSubqueryRule toRule() {
      return new QualifiedAggregateToSubqueryRule(this);
    }
  }
}
