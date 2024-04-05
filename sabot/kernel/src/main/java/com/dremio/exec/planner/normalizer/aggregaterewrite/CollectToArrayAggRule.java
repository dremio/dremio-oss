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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilder;

/**
 * This rule is for rewriting ARRAY subquery into ARRAY_AGG.
 *
 * <p>For example:
 *
 * <p>SELECT ARRAY(SELECT ENAME FROM EMP WHERE DEPTNO = 20)
 *
 * <p>Get's rewritten to:
 *
 * <p>SELECT ARRAY_AGG(ENAME) FROM EMP WHERE DEPTNO = 20
 *
 * <p>If we want to talk about RelNodes then the plans are:
 *
 * <p>Collect(field=[EXPR$0]) LogicalProject(ENAME=[$1]) LogicalFilter(condition=[=($6, 20)])
 * ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,
 * `DEPTNO`, `COMM`], splits=[1])
 *
 * <p>And:
 *
 * <p>LogicalAggregate(group=[{}], collect_to_array_agg=[ARRAY_AGG($0)]) LogicalProject(ENAME=[$1])
 * LogicalFilter(condition=[=($6, 20)]) ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`,
 * `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `DEPTNO`, `COMM`], splits=[1])
 */
public final class CollectToArrayAggRule extends RelOptRule {
  public static final CollectToArrayAggRule INSTANCE = new CollectToArrayAggRule();

  private CollectToArrayAggRule() {
    super(RelOptHelper.any(Collect.class, Convention.NONE), "CollectToArrayAggRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Collect collect = call.rel(0);
    RelNode rewritenRelNode =
        rewrite(call.builder(), ((HepRelVertex) collect.getInput()).getCurrentRel(), false);

    call.transformTo(rewritenRelNode);
  }

  public static RelNode rewrite(RelBuilder relBuilder, RelNode input, boolean isCorrelated) {
    // If this is a correlated query, then we need to offset for the join key introduced.
    int fieldToCollect = isCorrelated ? 1 : 0;
    InputInfo inputInfo = extractInputInfo(input, isCorrelated);

    AggregateCall arrayAggCall =
        AggregateCall.create(
            DremioSqlOperatorTable.ARRAY_AGG,
            inputInfo.hasDistinct,
            false,
            ImmutableList.of(fieldToCollect),
            -1,
            inputInfo.hasSort ? RelCollations.of(fieldToCollect) : RelCollations.EMPTY,
            1,
            inputInfo.inputForAggregation,
            /*type*/ null,
            "collect_to_array_agg");

    RelNode rewrittenQuery =
        relBuilder
            .push(inputInfo.inputForAggregation)
            .aggregate(
                // We need to GROUP BY the ARRAY / JOIN Key
                // otherwise we will be applying ARRAY_AGG on the UNNEST'ing of all ARRAYs in the
                // dataset:
                isCorrelated
                    ? relBuilder.groupKey(
                        input
                            .getCluster()
                            .getRexBuilder()
                            .makeInputRef(inputInfo.inputForAggregation, 0))
                    : relBuilder.groupKey(),
                ImmutableList.of(arrayAggCall))
            .build();

    return rewrittenQuery;
  }

  private static InputInfo extractInputInfo(RelNode input, boolean isCorrelated) {
    CollectInputExtractor collectInputExtractor = new CollectInputExtractor();
    RelNode inputForAggregation = input.accept(collectInputExtractor);

    if (!isCorrelated) {
      if (inputForAggregation.getRowType().getFieldCount() != 1) {
        throw UserException.planError()
            .message("ARRAY Subquery can only support 1 column in the projection.")
            .buildSilently();
      }
    }

    return new InputInfo(
        inputForAggregation, collectInputExtractor.hasDistinct, collectInputExtractor.hasSort);
  }

  /**
   * Extracts out the actual input we are going to apply ARRAY_AGG on. If the provided RelNode has a
   * DISTINCT or ORDER BY before the ARRAY_AGG, then we need to push this to the AggCall, things
   * it's not defined what "aggregating" on an ordered set means.
   */
  private static final class CollectInputExtractor extends StatelessRelShuttleImpl {
    private boolean hasDistinct;
    private boolean hasSort;

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      /**
       * This means we have a DISTINCT in the subquery and the plan looks like this:
       * Collect(field=[EXPR$0]) LogicalAggregate(group=[{0}]) LogicalProject(JOB=[$2])
       * ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`,
       * `SAL`, `DEPTNO`, `COMM`], splits=[1])
       */
      if (!aggregate.getAggCallList().isEmpty()) {
        // We only know how to handle DISTINCT in this scenario.
        throw new UnsupportedOperationException(
            "Did not expect aggregate to have aggregate calls in this context.");
      }

      hasDistinct = true;
      return aggregate.getInput().accept(this);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
      /**
       * This means we have a SORT in the subquery and the plan looks like this:
       * Collect(field=[EXPR$0]) LogicalSort(fields=[{0}]) LogicalProject(JOB=[$2])
       * ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`,
       * `SAL`, `DEPTNO`, `COMM`], splits=[1])
       */
      if (sort.getSortExps().size() != 1) {
        // We only how to handle a single sort key in this scenario
        throw new UnsupportedOperationException("Did not expect SORT to have multiple sort keys.");
      }

      hasSort = true;
      return sort.getInput().accept(this);
    }

    @Override
    public RelNode visit(RelNode other) {
      return other;
    }

    public boolean isHasDistinct() {
      return hasDistinct;
    }

    public boolean isHasSort() {
      return hasSort;
    }
  }

  private static final class InputInfo {
    private final RelNode inputForAggregation;
    private final boolean hasDistinct;
    private final boolean hasSort;

    public InputInfo(RelNode inputForAggregation, boolean hasDistinct, boolean hasSort) {
      this.inputForAggregation = inputForAggregation;
      this.hasDistinct = hasDistinct;
      this.hasSort = hasSort;
    }
  }
}
