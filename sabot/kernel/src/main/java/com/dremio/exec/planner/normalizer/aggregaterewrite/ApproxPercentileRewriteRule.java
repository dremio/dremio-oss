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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.APPROX_PERCENTILE;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.TDIGEST_QUANTILE;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.expr.fn.tdigest.TDigest;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * This rule rewrites the approx_percentile to quantile on tdigest.
 * Here is a example: SELECT approx_percentile(col, 0.6) FROM table
 * It is rewritten from
     LogicalAggregate(group=[{}], EXPR$0=[APPROX_PERCENTILE($0, $1)])
       LogicalProject(employee_id=[$0], $f1=[0.6:DECIMAL(2, 1)])
         ScanCrel(table=[cp."employee.json"], columns=[`employee_id`, `full_name`, ..., `management_role`], splits=[1])
to
     LogicalProject($f0=[TDIGEST_QUANTILE(0.6:DECIMAL(2, 1), $0)])
       LogicalAggregate(group=[{}], EXPR$0=[TDIGEST($0, $1)])
         LogicalProject(employee_id=[$0], $f1=[0.6:DECIMAL(2, 1)])
           ScanCrel(table=[cp."employee.json"], columns=[`employee_id`, ..., `management_role`], splits=[1])

 */
public final class ApproxPercentileRewriteRule extends RelOptRule {
  public static final ApproxPercentileRewriteRule INSTANCE = new ApproxPercentileRewriteRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private ApproxPercentileRewriteRule(RelBuilderFactory factory) {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)),
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, "ApproxPercentileRewriteRule");
  }

  @Override
  public boolean matches(RelOptRuleCall relOptRuleCall) {
    final Aggregate aggregate = relOptRuleCall.rel(0);
    return aggregate
      .getAggCallList()
      .stream()
      .anyMatch(aggCall -> aggCall.getAggregation() == APPROX_PERCENTILE && ! aggCall.hasFilter());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate agg = call.rel(0);
    // An aggregate could have multiple aggregate calls.
    // Rewrite all the ones that are a APPROX_PERCENTILE
    List<AggregateCall> calls = new ArrayList<>();
    for(AggregateCall aggregateCall: agg.getAggCallList()) {
      if(aggregateCall.getAggregation() != APPROX_PERCENTILE) {
        calls.add(aggregateCall);
        continue;
      }
      calls.add(AggregateCall.create(
        new TDigest.SqlTDigestAggFunction(aggregateCall.getType()),
        aggregateCall.isDistinct(),
        true,
        Arrays.asList(aggregateCall.getArgList().get(0)),
        -1,
        aggregateCall.getType(),
        aggregateCall.getName()));
    }



    RelNode originalInput = agg.getInput();

    final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
    final RelBuilder builder = call.builder();

    LogicalProject currentRel = (LogicalProject)((HepRelVertex)originalInput).getCurrentRel();

    RexNode percentile = currentRel.getProjects().get(1);
    RexNode column_name = rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(DOUBLE),0);

    // Forming the rewritten query
    final RelNode rewrittenQuery = builder
      .push(originalInput)
      .aggregate(builder.groupKey(agg.getGroupSet()), calls )  // calls here contains SqlTDigestAggFunction
      .project(builder.call(TDIGEST_QUANTILE, percentile, column_name))
      .build();

    call.transformTo(rewrittenQuery);
  }
}
