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
package com.dremio.exec.planner.physical.rule.computation;

import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.rules.DremioOptimizationRelRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;

/**
 * Push downs computation in a hash joins extra conditions.
 *
 * <p>Rewrites <code>
 *   SELECT t1.c1, t1.c2, t2.c1, t2.c2
 *   FROM t1
 *   JOIN t2 ON t1.c1 = t2.c1 AND t1.c2 * 5 > t2.c2 - 1
 * </code> to <code>
 *   WITH
 *   v1 AS (
 *     SELECT t1.c1, t1.c2, t1.c2 * 5 AS c3
 *     FROM t1),
 *   v2 AS (
 *     SELECT t2.c1, t2.c2, t2.c2 - 1 AS c3
 *  *     FROM t2),
 *   SELECT v1.c1, v1.c2, v2.c1, v2.c2
 *   FROM v1
 *   JOIN v2 ON v1.c1 = v2.c1 AND v1.c3 > v2.c3
 * </code>
 */
public class HashJoinComputationExtractionRule
    extends DremioOptimizationRelRule<HashJoinComputationExtractionRule.Config> {
  public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

  public HashJoinComputationExtractionRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HashJoinPrel hashJoin = call.rel(0);
    final RexNode rexNode = hashJoin.getExtraCondition();
    return rexNode != null && !rexNode.isAlwaysTrue();
  }

  @Override
  public void doOnMatch(RelOptRuleCall call) {
    final HashJoinPrel hashJoin = call.rel(0);

    JoinComputationExtractor.ExtractedComputation extractedComputation =
        JoinComputationExtractor.extractedComputation(
            hashJoin.getRowType(),
            hashJoin.getCondition(),
            hashJoin.getExtraCondition(),
            hashJoin.getLeft(),
            hashJoin.getRight());

    if (null == extractedComputation) {
      return;
    }

    HashJoinPrel newJoin =
        HashJoinPrel.create(
            hashJoin.getCluster(),
            hashJoin.getTraitSet(),
            extractedComputation.left,
            extractedComputation.right,
            extractedComputation.joinCondition,
            extractedComputation.extraCondition,
            hashJoin.getJoinType(),
            hashJoin.getIgnoreForJoinAnalysis());
    ProjectPrel projectPrel =
        ProjectPrel.create(
            newJoin.getCluster(),
            newJoin.getTraitSet(),
            newJoin,
            extractedComputation.topProject,
            hashJoin.getRowType());
    call.transformTo(projectPrel);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("HashJoinComputationExtractionRule")
            .withOperandSupplier(os1 -> os1.operand(HashJoinPrel.class).anyInputs())
            .as(Config.class);

    @Override
    default HashJoinComputationExtractionRule toRule() {
      return new HashJoinComputationExtractionRule(this);
    }
  }
}
