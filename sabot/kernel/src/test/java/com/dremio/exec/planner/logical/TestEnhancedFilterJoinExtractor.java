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

import static com.dremio.test.dsl.RexDsl.and;
import static com.dremio.test.dsl.RexDsl.caseExpr;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.intInput;
import static com.dremio.test.dsl.RexDsl.intNullInput;
import static com.dremio.test.dsl.RexDsl.isNotDistinctFrom;
import static com.dremio.test.dsl.RexDsl.isNull;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.not;
import static com.dremio.test.dsl.RexDsl.or;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link EnhancedFilterJoinExtractor}. */
public class TestEnhancedFilterJoinExtractor {
  private static final RelBuilder relBuilder = makeRelBuilder();

  @Test
  public void testExtractJoinCondition() {
    testExtract(
        eq(intInput(0), intInput(2)),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "=($0, $2)",
        "true",
        "true");
  }

  @Test
  public void testExtractPushdownPredicates() {
    testExtract(
        and(eq(intInput(0), literal(10)), eq(intInput(2), literal(20))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "=($0, 10)",
        "=($2, 20)");
  }

  @Test
  public void testCnfCanExtract() {
    testExtract(
        and(
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            or(eq(intInput(2), literal(30)), eq(intInput(3), literal(40)))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "OR(=($0, 10), =($1, 20))",
        "OR(=($2, 30), =($3, 40))");
  }

  @Test
  public void testCnfCannotExtract() {
    testExtract(
        and(
            or(eq(intInput(0), literal(10)), eq(intInput(2), literal(30))),
            or(eq(intInput(1), literal(20)), eq(intInput(3), literal(40)))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "true",
        "true");
  }

  @Test
  public void testDnfCanExtract() {
    testExtract(
        or(
            and(eq(intInput(0), literal(10)), eq(intInput(2), literal(30))),
            and(eq(intInput(1), literal(20)), eq(intInput(3), literal(40)))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "OR(=($0, 10), =($1, 20))",
        "OR(=($2, 30), =($3, 40))");
  }

  @Test
  public void testDnfCannotExtract() {
    testExtract(
        or(
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            and(eq(intInput(2), literal(30)), eq(intInput(3), literal(40)))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "true",
        "true");
  }

  @Test
  public void testNotCanExtract() {
    testExtract(
        or(
            not(or(eq(intInput(0), literal(10)), eq(intInput(2), literal(30)))),
            not(or(eq(intInput(1), literal(20)), eq(intInput(3), literal(40))))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "OR(<>($0, 10), <>($1, 20))",
        "OR(<>($2, 30), <>($3, 40))");
  }

  @Test
  public void testNotCannotExtract() {
    testExtract(
        or(
            not(and(eq(intInput(0), literal(10)), eq(intInput(2), literal(30)))),
            not(and(eq(intInput(1), literal(20)), eq(intInput(3), literal(40))))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "true",
        "true");
  }

  @Test
  public void testSupersetAnd() {
    testExtract(
        and(
            or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            eq(intInput(0), literal(10))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "=($0, 10)",
        "true");
  }

  @Test
  public void testSupersetOr() {
    testExtract(
        or(
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            eq(intInput(0), literal(10))),
        literal(true),
        JoinRelType.INNER,
        JoinRelType.INNER,
        "true",
        "=($0, 10)",
        "true");
  }

  @Test
  public void testLeftOuterJoinCanSimplifyToInner() {
    testExtract(
        eq(intInput(0), intNullInput(2)),
        literal(true),
        JoinRelType.LEFT,
        JoinRelType.INNER,
        "=($0, $2)",
        "true",
        "true");
  }

  @Test
  public void testLeftOuterJoinNotDistinctFrom() {
    testExtract(
        isNotDistinctFrom(intInput(0), intNullInput(2)),
        literal(true),
        JoinRelType.LEFT,
        JoinRelType.LEFT,
        "true",
        "true",
        "true");
  }

  @Test
  public void testLeftOuterJoinCannotSimplifyToInner() {
    testExtract(
        eq(intInput(0), literal(10)),
        literal(true),
        JoinRelType.LEFT,
        JoinRelType.LEFT,
        "true",
        "=($0, 10)",
        "true");
  }

  @Test
  public void testLeftOuterJoinCanPushJoinConditionToRightButNotLeft() {
    testExtract(
        eq(intInput(1), literal(10)),
        and(eq(intInput(0), literal(10)), eq(intInput(2), literal(20))),
        JoinRelType.LEFT,
        JoinRelType.LEFT,
        "=($0, 10)",
        "=($1, 10)",
        "=($2, 20)");
  }

  @Test
  public void testLeftOuterJoinCanPushTopLevelFilterToLeftButNotRight() {
    testExtract(
        and(
            eq(intInput(0), literal(10)),
            or(eq(intNullInput(2), literal(20)), isNull(intNullInput(2)))),
        literal(true),
        JoinRelType.LEFT,
        JoinRelType.LEFT,
        "true",
        "=($0, 10)",
        "true");
  }

  @Test
  public void testLeftOuterJoinCannotPushTopLevelFilterToJoin() {
    testExtract(
        eq(caseExpr(isNull(intNullInput(2)), intInput(1), intNullInput(2)), literal(20)),
        literal(true),
        JoinRelType.LEFT,
        JoinRelType.LEFT,
        "true",
        "true",
        "true");
  }

  private void testExtract(
      RexNode inputFilterCondition,
      RexNode inputJoinCondition,
      JoinRelType joinRelType,
      JoinRelType expectedJoinType,
      String expectedJoinConditionString,
      String expectedLeftPushdownPredicateString,
      String expectedRightPushdownPredicateString) {
    Join joinRel =
        (Join)
            relBuilder
                .values(new String[] {"a", "b"}, 1, 2)
                .values(new String[] {"x", "y"}, 5, 6)
                .join(joinRelType, inputJoinCondition)
                .build();
    Filter filterRel = (Filter) relBuilder.push(joinRel).filter(inputFilterCondition).build();

    EnhancedFilterJoinExtraction extraction =
        new EnhancedFilterJoinExtractor(
                filterRel, joinRel, FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
            .extract();
    Assert.assertEquals(expectedJoinType, extraction.getSimplifiedJoinType());
    Assert.assertEquals(expectedJoinConditionString, extraction.getJoinCondition().toString());
    Assert.assertEquals(
        expectedLeftPushdownPredicateString, extraction.getLeftPushdownPredicate().toString());
    Assert.assertEquals(
        expectedRightPushdownPredicateString, extraction.getRightPushdownPredicate().toString());
  }

  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner =
        new HepPlanner(
            new HepProgramBuilder().build(), context, false, null, new DremioCost.Factory());
    RelOptCluster cluster =
        RelOptCluster.create(planner, new DremioRexBuilder(SqlTypeFactoryImpl.INSTANCE));
    return org.apache.calcite.tools.RelBuilder.proto(context).create(cluster, null);
  }
}
