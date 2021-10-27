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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;

/**
 * Test for {@link EnhancedFilterJoinExtractor}.
 */
public class TestEnhancedFilterJoinExtractor {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  private static final RexNode col_R_a = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_S_x = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_S_y = rexBuilder.makeInputRef(intColumnType,3);

  private static final RexNode intLit10 = rexBuilder.makeLiteral(10,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 = rexBuilder.makeLiteral(30,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit40 = rexBuilder.makeLiteral(40,
    typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testExtractJoinCondition() {
    testExtract(
      rEq(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $2)",
      "true",
      "true");
  }

  @Test
  public void testExtractPushdownPredicates() {
    testExtract(
      rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "=($0, 10)",
      "=($2, 20)");
  }

  @Test
  public void testCnfCanExtract() {
    testExtract(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rOr(rEq(col_S_x, intLit30), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 20))",
      "OR(=($2, 30), =($3, 40))");
  }

  @Test
  public void testCnfCannotExtract() {
    testExtract(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_S_x, intLit30)),
        rOr(rEq(col_R_b, intLit20), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "true",
      "true");
  }

  @Test
  public void testDnfCanExtract() {
    testExtract(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit30)),
        rAnd(rEq(col_R_b, intLit20), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 20))",
      "OR(=($2, 30), =($3, 40))");
  }

  @Test
  public void testDnfCannotExtract() {
    testExtract(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rAnd(rEq(col_S_x, intLit30), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "true",
      "true");
  }

  @Test
  public void testNotCanExtract() {
    testExtract(
      rOr(
        rNot(rOr(rEq(col_R_a, intLit10), rEq(col_S_x, intLit30))),
        rNot(rOr(rEq(col_R_b, intLit20), rEq(col_S_y, intLit40)))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(<>($0, 10), <>($1, 20))",
      "OR(<>($2, 30), <>($3, 40))");
  }

  @Test
  public void testNotCannotExtract() {
    testExtract(
      rOr(
        rNot(rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit30))),
        rNot(rAnd(rEq(col_R_b, intLit20), rEq(col_S_y, intLit40)))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "true",
      "true");
  }

  @Test
  public void testSupersetAnd() {
    testExtract(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rEq(col_R_a, intLit10)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "=($0, 10)",
      "true");
  }

  @Test
  public void testSupersetOr() {
    testExtract(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rEq(col_R_a, intLit10)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "=($0, 10)",
      "true");
  }

  @Test
  public void testLeftOuterJoinCanSimplifyToInner() {
    testExtract(
      rEq(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.INNER,
      "=($0, $2)",
      "true",
      "true");
  }

  @Test
  public void testLeftOuterJoinNotDistinctFrom() {
    testExtract(
      rIsNotDistinctFrom(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "true",
      "true");
  }

  @Test
  public void testLeftOuterJoinCannotSimplifyToInner() {
    testExtract(
      rEq(col_R_a, intLit10),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "=($0, 10)",
      "true");
  }

  @Test
  public void testLeftOuterJoinCanPushJoinConditionToRightButNotLeft() {
    testExtract(
      rEq(col_R_b, intLit10),
      rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "=($0, 10)",
      "=($1, 10)",
      "=($2, 20)");
  }

  @Test
  public void testLeftOuterJoinCanPushTopLevelFilterToLeftButNotRight() {
    testExtract(
      rAnd(rEq(col_R_a, intLit10), rOr(rEq(col_S_x, intLit20), rIsNull(col_S_x))),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "=($0, 10)",
      "true");
  }

  @Test
  public void testLeftOuterJoinCannotPushTopLevelFilterToJoin() {
    testExtract(
      rEq(rCase(rIsNull(col_R_a), col_R_b, col_S_x), intLit20),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "true",
      "true");
  }

  private void testExtract(RexNode inputFilterCondition, RexNode inputJoinCondition,
    JoinRelType joinRelType, JoinRelType expectedJoinType, String expectedJoinConditionString,
    String expectedLeftPushdownPredicateString, String expectedRightPushdownPredicateString) {
    Join joinRel = (Join) relBuilder
      .values(new String[] {"a", "b"}, 1, 2)
      .values(new String[] {"x", "y"}, 5, 6)
      .join(joinRelType, inputJoinCondition)
      .build();
    Filter filterRel = (Filter) relBuilder
      .push(joinRel)
      .filter(inputFilterCondition)
      .build();

    EnhancedFilterJoinExtraction extraction = new EnhancedFilterJoinExtractor(filterRel,
      joinRel, FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM).extract();
    Assert.assertEquals(expectedJoinType, extraction.getSimplifiedJoinType());
    Assert.assertEquals(expectedJoinConditionString,
      extraction.getJoinCondition().toString());
    Assert.assertEquals(expectedLeftPushdownPredicateString,
      extraction.getLeftPushdownPredicate().toString());
    Assert.assertEquals(expectedRightPushdownPredicateString,
      extraction.getRightPushdownPredicate().toString());
  }

  private static RexNode rEq(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(EQUALS, rexNode1, rexNode2);
  }

  private static RexNode rAnd(RexNode... rexNodes) {
    return rexBuilder.makeCall(AND, rexNodes);
  }

  private static RexNode rOr(RexNode... rexNodes) {
    return rexBuilder.makeCall(OR, rexNodes);
  }

  private static RexNode rNot(RexNode rexNode) {
    return rexBuilder.makeCall(NOT, rexNode);
  }

  private static RexNode rIsNotDistinctFrom(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(IS_NOT_DISTINCT_FROM, rexNode1, rexNode2);
  }

  private static RexNode rIsNull(RexNode rexNode) {
    return rexBuilder.makeCall(IS_NULL, rexNode);
  }

  private static RexNode rCase(RexNode... rexNodes) {
    return rexBuilder.makeCall(CASE, rexNodes);
  }

  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build(), context, false,
      null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return org.apache.calcite.tools.RelBuilder.proto(context).create(cluster, null);
  }
}
