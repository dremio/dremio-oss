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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
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
 * Test for {@link EnhancedFilterJoinRule}.
 */
public class TestEnhancedFilterJoinRule {
  private static final EnhancedFilterJoinRule ruleWithFilter = EnhancedFilterJoinRule.WITH_FILTER;
  private static final EnhancedFilterJoinRule ruleNoFilter = EnhancedFilterJoinRule.NO_FILTER;

  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  private static final RexNode col_R_a = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_R_c = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_R_d = rexBuilder.makeInputRef(intColumnType,3);
  private static final RexNode col_S_x = rexBuilder.makeInputRef(intColumnType,4);
  private static final RexNode col_S_y = rexBuilder.makeInputRef(intColumnType,5);
  private static final RexNode col_S_z = rexBuilder.makeInputRef(intColumnType,6);
  private static final RexNode col_S_w = rexBuilder.makeInputRef(intColumnType,7);

  private static final RexNode intLit10 = rexBuilder.makeLiteral(10,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 = rexBuilder.makeLiteral(30,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit40 = rexBuilder.makeLiteral(40,
    typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testWithTopFilterBothHaveConditionExtractBoth() {
    testWithTopFilter(
      rEq(col_R_a, intLit10),
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "AND(=($0, 10), OR(=($1, 10), =($1, 30)))",
      "OR(=($1, 20), =($1, 40))",
      "OR(AND(=($1, 10), =($5, 20)), AND(=($1, 30), =($5, 40)))"
    );
  }

  @Test
  public void testWithTopFilterBothHaveConditionJoinHasOnlyJoinCondition() {
    testWithTopFilter(
      rEq(col_R_a, intLit10),
      rEq(col_R_a, col_S_x),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "=($0, 10)",
      "true",
      "true"
    );
  }

  @Test
  public void testWithTopFilterFilterHasConditionExtractBoth() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "OR(=($1, 10), =($1, 30))",
      "OR(=($1, 20), =($1, 40))",
      "OR(AND(=($1, 10), =($5, 20)), AND(=($1, 30), =($5, 40)))"
    );
  }

  @Test
  public void testWithTopFilterFilterHasConditionExtractOnlyJoinCondition() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rOr(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20))),
        rAnd(rEq(col_R_a, col_S_x), rOr(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40)))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "true",
      "true",
      "OR(=($1, 10), =($5, 20), =($1, 30), =($5, 40))"
    );
  }

  @Test
  public void testWithTopFilterFilterHasConditionExtractOnlyPushdownPredicates() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($1, 10), =($1, 30))",
      "OR(=($1, 20), =($1, 40))",
      "OR(AND(=($1, 10), =($5, 20)), AND(=($1, 30), =($5, 40)))"
    );
  }

  @Test
  public void testWithTopFilterFilterHasConditionExtractNothing() {
    testWithTopFilter(
      rAnd(
        rOr(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rOr(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      null,
      "no pushdown",
      "no pushdown",
      "no pushdown",
      "no pushdown"
    );
  }

  @Test
  public void testWithTopFilterCanPushdownFilterBelowNot() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rNot(rOr(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)))),
        rAnd(rEq(col_R_a, col_S_x), rNot(rOr(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "OR(<>($1, 10), <>($1, 30))",
      "OR(<>($1, 20), <>($1, 40))",
      "OR(AND(<>($1, 10), <>($5, 20)), AND(<>($1, 30), <>($5, 40)))"
    );
  }

  @Test
  public void testWithTopFilterCannotPushdownFilterBelowNot() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rNot(rAnd(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)))),
        rAnd(rEq(col_R_a, col_S_x), rNot(rAnd(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "true",
      "true",
      "OR(<>($1, 10), <>($5, 20), <>($1, 30), <>($5, 40))"
    );
  }

  @Test
  public void testNoTopFilterExtractBoth() {
    testNoTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "OR(=($1, 10), =($1, 30))",
      "OR(=($1, 20), =($1, 40))",
      "OR(AND(=($1, 10), =($5, 20)), AND(=($1, 30), =($5, 40)))"
    );
  }

  @Test
  public void testNoTopFilterExtractOnlyJoinCondition() {
    testNoTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rOr(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20))),
        rAnd(rEq(col_R_a, col_S_x), rOr(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40)))),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "true",
      "true",
      "OR(=($1, 10), =($5, 20), =($1, 30), =($5, 40))"
    );
  }

  @Test
  public void testNoTopFilterExtractOnlyPushdownPredicates() {
    testNoTopFilter(
      rOr(
        rAnd(rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($1, 10), =($1, 30))",
      "OR(=($1, 20), =($1, 40))",
      "OR(AND(=($1, 10), =($5, 20)), AND(=($1, 30), =($5, 40)))"
    );
  }

  @Test
  public void testNoTopFilterExtractNothing() {
    testNoTopFilter(
      rAnd(
        rOr(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
        rOr(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit30), rEq(col_S_y, intLit40))),
      JoinRelType.INNER,
      null,
      "no pushdown",
      "no pushdown",
      "no pushdown",
      "no pushdown"
    );
  }

  @Test
  public void testSupersetPruningAnd() {
    testWithTopFilter(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_S_y, intLit30)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 20))",
      "true",
      "true"
    );
  }

  @Test
  public void testSupersetPruningOr() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_S_y, intLit30)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "AND(=($0, 10), =($1, 20))",
      "true",
      "true"
    );
  }

  /**
   * a = x AND b AND y
   */
  @Test
  public void testSimplifyRemainingFilterAnd() {
    testWithTopFilter(
      rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "=($1, 10)",
      "=($1, 20)",
      "true"
    );
  }

  /**
   * (a = x AND b) OR (a = x AND y)
   */
  @Test
  public void testSimplifyRemainingFilterOrHaveCommonExact() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10)),
        rAnd(rEq(col_R_a, col_S_x), rEq(col_S_y, intLit20))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "true",
      "true",
      "OR(=($1, 10), =($5, 20))"
    );
  }

  /**
   * (a AND b and x) OR (a AND y)
   */
  @Test
  public void testSimplifyRemainingFilterOrHaveCommonNotExact() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_S_x, intLit30)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_y, intLit30))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "=($0, 10)",
      "OR(=($0, 30), =($1, 30))",
      "OR(=($1, 20), =($5, 30))"
    );
  }

  /**
   * (a AND x) OR (a AND y) OR b
   */
  @Test
  public void testSimplifyRemainingFilterOrHaveCommonWithExtraChild() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_y, intLit30)),
        rEq(col_R_b, intLit40)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 40))",
      "true",
      "OR(=($4, 20), =($5, 30), =($1, 40))"
    );
  }

  /**
   * (a AND x) OR y
   */
  @Test
  public void testSimplifyRemainingFilterOrNoCommon() {
    testWithTopFilter(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
        rEq(col_R_b, intLit30)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 30))",
      "true",
      "OR(=($4, 20), =($1, 30))"
    );
  }

  /**
   * (x AND (((y OR z) AND a) OR ((y OR z) AND b))) OR w
   */
  @Test
  public void testSimplifyRemainingFilterComplexOrCanSimplify() {
    testWithTopFilter(
      rOr(
        rAnd(
          rEq(col_S_x, intLit10),
          rOr(
            rAnd(
              rOr(rEq(col_S_y, intLit20), rEq(col_S_z, intLit30)),
              rEq(col_R_a, intLit10)),
            rAnd(
              rOr(rEq(col_S_y, intLit20), rEq(col_S_z, intLit30)),
              rEq(col_R_b, intLit20)))),
        rEq(col_S_w, intLit40)),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "true",
      "OR(AND(=($0, 10), OR(=($1, 20), =($2, 30))), =($3, 40))",
      "OR(=($0, 10), =($1, 20), =($7, 40))"
    );
  }

  /**
   * (x AND (((y OR z) AND a) OR ((y OR z) AND b))) OR (w AND c)
   */
  @Test
  public void testSimplifyRemainingFilterComplexORCannotSimplify() {
    testWithTopFilter(
      rOr(
        rAnd(
          rEq(col_S_x, intLit10),
          rOr(
            rAnd(
              rOr(rEq(col_S_y, intLit20), rEq(col_S_z, intLit30)),
              rEq(col_R_a, intLit10)),
            rAnd(
              rOr(rEq(col_S_y, intLit20), rEq(col_S_z, intLit30)),
              rEq(col_R_b, intLit20)))),
        rAnd(rEq(col_S_w, intLit40), rEq(col_R_c, intLit30))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 20), =($2, 30))",
      "OR(AND(=($0, 10), OR(=($1, 20), =($2, 30))), =($3, 40))",
      "OR(" +
        "AND(" +
          "=($4, 10), " +
          "OR(" +
            "AND(" +
              "OR(=($5, 20), =($6, 30)), " +
              "=($0, 10)), " +
            "AND(" +
              "OR(=($5, 20), =($6, 30)), " +
              "=($1, 20)))), " +
        "AND(=($7, 40), =($2, 30)))"
    );
  }

  /**
   * (((x and y and a) or (x and y and b)) and w) or (((x and z and c) or (x and z and d)) and w)
   */
  @Test
  public void testSimplifyRemainingUpperOrHaveCommonNotExact() {
    testWithTopFilter(
      rOr(
        rAnd(
          rOr(
            rAnd(rEq(col_S_x, intLit10), rEq(col_S_y, intLit10), rEq(col_R_a, intLit10)),
            rAnd(rEq(col_S_x, intLit10), rEq(col_S_y, intLit10), rEq(col_R_b, intLit10))),
          rEq(col_S_w, intLit10)),
        rAnd(
          rOr(
            rAnd(rEq(col_S_x, intLit10), rEq(col_S_z, intLit10), rEq(col_R_c, intLit10)),
            rAnd(rEq(col_S_x, intLit10), rEq(col_S_z, intLit10), rEq(col_R_d, intLit10))),
          rEq(col_S_w, intLit10))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "true",
      "OR(=($0, 10), =($1, 10), =($2, 10), =($3, 10))",
      "OR(AND(=($0, 10), =($1, 10), =($3, 10)), " +
        "AND(=($0, 10), =($2, 10), =($3, 10)))",
      "OR(AND(OR(=($0, 10), =($1, 10)), =($5, 10)), " +
        "AND(OR(=($2, 10), =($3, 10)), =($6, 10)))");
  }

  /**
   * ((a = x AND b) OR (a = x AND y)) AND (c or d)
   */
  @Test
  public void testSimplifyRemainingFilterAndOr() {
    testWithTopFilter(
      rAnd(
        rOr(
          rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10)),
          rAnd(rEq(col_R_a, col_S_x), rEq(col_S_y, intLit20))),
        rOr(rEq(col_R_c, intLit30), rEq(col_R_d, intLit40))),
      rexBuilder.makeLiteral(true),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "OR(=($2, 30), =($3, 40))",
      "true",
      "OR(=($1, 10), =($5, 20))"
    );
  }

  /**
   * filter:  a = x AND b AND y
   * join:    c AND z
   */
  @Test
  public void testLeftOuterJoinSimplifyToInner() {
    testWithTopFilter(
      rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.LEFT,
      JoinRelType.INNER,
      "=($0, $4)",
      "AND(=($1, 10), =($2, 30))",
      "AND(=($1, 20), =($2, 40))",
      "true");
  }

  /**
   * filter:  a = x AND b AND y
   * join:    c AND z
   */
  @Test
  public void testRightOuterJoinSimplifyToInner() {
    testWithTopFilter(
      rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.RIGHT,
      JoinRelType.INNER,
      "=($0, $4)",
      "AND(=($1, 10), =($2, 30))",
      "AND(=($1, 20), =($2, 40))",
      "true");
  }

  /**
   * filter:  a = x AND b AND y
   * join:    c AND z
   */
  @Test
  public void testFullOuterJoinSimplifyToInner() {
    testWithTopFilter(
      rAnd(rEq(col_R_a, col_S_x), rEq(col_R_b, intLit10), rEq(col_S_y, intLit20)),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.FULL,
      JoinRelType.INNER,
      "=($0, $4)",
      "AND(=($1, 10), =($2, 30))",
      "AND(=($1, 20), =($2, 40))",
      "true");
  }

  /**
   * filter:  b
   * join:    c AND z
   */
  @Test
  public void testFullOuterJoinSimplifyToLeft() {
    testWithTopFilter(
      rEq(col_R_b, intLit10),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.FULL,
      JoinRelType.LEFT,
      "=($2, 30)",
      "=($1, 10)",
      "=($2, 40)",
      "true");
  }

  /**
   * filter:  b
   * join:    c AND z
   */
  @Test
  public void testLeftOuterJoinCannotSimplifyJoin() {
    testWithTopFilter(
      rEq(col_R_b, intLit10),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "=($2, 30)",
      "=($1, 10)",
      "=($2, 40)",
      "true");
  }

  /**
   * filter:  y
   * join:    c AND z
   */
  @Test
  public void testRightOuterJoinCannotSimplifyJoin() {
    testWithTopFilter(
      rEq(col_S_y, intLit20),
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.RIGHT,
      JoinRelType.RIGHT,
      "=($6, 40)",
      "=($2, 30)",
      "=($1, 20)",
      "true");
  }

  /**
   * no filter
   * join:    c AND z
   */
  @Test
  public void testFullOuterJoinCannotSimplifyJoin() {
    testNoTopFilter(
      rAnd(rEq(col_R_c, intLit30), rEq(col_S_z, intLit40)),
      JoinRelType.FULL,
      null,
      "no pushdown",
      "no pushdown",
      "no pushdown",
      "no pushdown");
  }

  @Test
  public void testLeftOuterJoinNotDistinctFrom() {
    testWithTopFilter(
      rIsNotDistinctFrom(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      null,
      "no pushdown",
      "no pushdown",
      "no pushdown",
      "no pushdown");
  }

  @Test
  public void testLeftOuterJoinCanPushJoinConditionToRightButNotLeft() {
    testNoTopFilter(
      rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "=($0, 10)",
      "true",
      "=($0, 20)",
      "true");
  }

  @Test
  public void testLeftOuterJoinCanPushTopLevelFilterToLeftButNotRight() {
    testWithTopFilter(
      rAnd(rEq(col_R_a, intLit10), rOr(rEq(col_S_x, intLit20), rIsNull(col_S_x))),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "=($0, 10)",
      "true",
      "OR(=($4, 20), IS NULL($4))");
  }

  @Test
  public void testLeftOuterJoinCannotPushTopLevelFilterToJoin() {
    testWithTopFilter(
      rAnd(
        rEq(col_R_a, intLit10),
        rEq(rCase(rIsNull(col_R_a), col_R_b, col_S_x), intLit20)),
      rexBuilder.makeLiteral(true),
      JoinRelType.LEFT,
      JoinRelType.LEFT,
      "true",
      "=($0, 10)",
      "true",
      "=(CASE(IS NULL($0), $1, $4), 20)");
  }

  @Test
  public void testNoTopFilterJoinConditionDoesNotSatisfyPredicateExtractNothing() {
    testNoTopFilter(
      rAnd(
        rEq(col_R_a, col_S_x),
        rOr(rEq(col_R_b, col_S_y), rEq(col_R_c, col_S_z))),
      JoinRelType.INNER,
      JoinRelType.INNER,
      "=($0, $4)",
      "true",
      "true",
      "OR(=($1, $5), =($2, $6))"
    );
  }


  private void testWithTopFilter(RexNode inputFilterCondition, RexNode inputJoinCondition,
    JoinRelType joinRelType, JoinRelType expectedJoinType, String expectedJoinConditionString,
    String expectedLeftPushdownPredicateString, String expectedRightPushdownPredicateString,
    String expectedRemainingFilterString) {

    Join joinRel = (Join) relBuilder
      .values(new String[] {"a", "b", "c", "d"}, 1, 2, 3, 4)
      .values(new String[] {"x", "y", "z", "w"}, 5, 6, 7, 8)
      .join(joinRelType, inputJoinCondition)
      .build();
    Filter filterRel = (Filter) relBuilder
      .push(joinRel)
      .filter(inputFilterCondition)
      .build();

    RelNode rewrite = ruleWithFilter.doMatch(filterRel, joinRel, relBuilder);
    checkPushdown(rewrite, expectedJoinType, expectedJoinConditionString, expectedLeftPushdownPredicateString,
      expectedRightPushdownPredicateString, expectedRemainingFilterString);
    checkInfiniteLoop(rewrite);
  }

  private void testNoTopFilter(RexNode inputJoinCondition, JoinRelType joinRelType,
    JoinRelType expectedJoinType, String expectedJoinConditionString,
    String expectedLeftPushdownPredicateString, String expectedRightPushdownPredicateString,
    String expectedRemainingFilterString) {

    Join joinRel = (Join) relBuilder
      .values(new String[] {"a", "b", "c", "d"}, 1, 2, 3, 4)
      .values(new String[] {"x", "y", "z", "w"}, 5, 6, 7, 8)
      .join(joinRelType, inputJoinCondition)
      .build();

    RelNode rewrite = ruleNoFilter.doMatch(null, joinRel, relBuilder);
    checkPushdown(rewrite, expectedJoinType, expectedJoinConditionString, expectedLeftPushdownPredicateString,
      expectedRightPushdownPredicateString, expectedRemainingFilterString);
    checkInfiniteLoop(rewrite);
  }

  private void checkPushdown(RelNode rewrite, JoinRelType expectedJoinType,
    String expectedJoinConditionString, String expectedLeftPushdownPredicateString,
    String expectedRightPushdownPredicateString, String expectedRemainingFilterString) {
    // Get rewritten result
    Pair<JoinRelType, String> joinTypeCondition = getJoinTypeCondition(rewrite);
    Pair<String, String> pushdownPredicatesString = getPushdownPredicatesString(rewrite);
    String remainingFilterString = getRemainingFilterString(rewrite);

    // Assert
    Assert.assertEquals(expectedJoinType, joinTypeCondition.getKey());
    Assert.assertEquals(expectedJoinConditionString, joinTypeCondition.getValue());
    Assert.assertEquals(expectedLeftPushdownPredicateString, pushdownPredicatesString.getKey());
    Assert.assertEquals(expectedRightPushdownPredicateString, pushdownPredicatesString.getValue());
    Assert.assertEquals(expectedRemainingFilterString, remainingFilterString);
  }

  private void checkInfiniteLoop(RelNode rewrite) {
    if (rewrite instanceof Filter) {
      RelNode childOfFilter = rewrite.getInput(0);
      if (childOfFilter instanceof Join) {
        RelNode twiceRewrite = ruleWithFilter.doMatch((Filter) rewrite, (Join) childOfFilter,
          relBuilder);
        Assert.assertNull(twiceRewrite);
      } else if (childOfFilter instanceof Project){
        RelNode childOfProject = childOfFilter.getInput(0);
        if (childOfProject instanceof Join) {
          RelNode twiceRewrite = ruleNoFilter.doMatch(null, (Join) childOfProject, relBuilder);
          Assert.assertNull(twiceRewrite);
        }
      }
    } else if (rewrite instanceof Project) {
      RelNode childOfProject = rewrite.getInput(0);
      if (childOfProject instanceof Join) {
        RelNode twiceRewrite = ruleNoFilter.doMatch(null, (Join) childOfProject, relBuilder);
        Assert.assertNull(twiceRewrite);
      }
    } else if (rewrite instanceof Join) {
      RelNode twiceRewrite = ruleNoFilter.doMatch(null, (Join) rewrite, relBuilder);
      Assert.assertNull(twiceRewrite);
    }
  }

  private static Pair<JoinRelType, String> getJoinTypeCondition(RelNode rewrite) {
    Join joinRel = getJoinRelFromRewrite(rewrite);
    return (joinRel != null) ?
      Pair.of(joinRel.getJoinType(), joinRel.getCondition().toString()) :
      Pair.of(null, "no pushdown");
  }

  private static Pair<String, String> getPushdownPredicatesString(RelNode rewrite) {
    Join joinRel = getJoinRelFromRewrite(rewrite);
    if (joinRel == null) {
      return Pair.of("no pushdown", "no pushdown");
    }
    RelNode joinLeftInput = joinRel.getLeft();
    RelNode joinRightInput = joinRel.getRight();
    String pushdownPredicatesStringLeft = (joinLeftInput instanceof Filter) ?
      ((Filter) joinLeftInput).getCondition().toString() : "true";
    String pushdownPredicatesStringRight = (joinRightInput instanceof Filter) ?
      ((Filter) joinRightInput).getCondition().toString() : "true";
    return Pair.of(pushdownPredicatesStringLeft, pushdownPredicatesStringRight);
  }

  private static String getRemainingFilterString(RelNode rewrite) {
    if (rewrite == null) {
      return "no pushdown";
    }
    if (rewrite instanceof Filter) {
      return ((Filter) rewrite).getCondition().toString();
    } else {
      return "true";
    }
  }

  private static Join getJoinRelFromRewrite(RelNode rewrite) {
    if (rewrite instanceof Join) {
      return (Join) rewrite;
    } else if (rewrite instanceof Filter) {
      RelNode childOfFilter = rewrite.getInput(0);
      if (childOfFilter instanceof Join) {
        return (Join) childOfFilter;
      } else if (childOfFilter instanceof Project) {
        RelNode childOfProject = childOfFilter.getInput(0);
        if (childOfProject instanceof Join) {
          return (Join) childOfProject;
        } else {
          return null;
        }
      } else {
        return null;
      }
    } else if (rewrite instanceof Project) {
      RelNode childOfProject = rewrite.getInput(0);
      if (childOfProject instanceof Join) {
        return (Join) childOfProject;
      } else {
        return null;
      }
    } else {
      return null;
    }
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
    return RelBuilder.proto(context).create(cluster, null);
  }
}
