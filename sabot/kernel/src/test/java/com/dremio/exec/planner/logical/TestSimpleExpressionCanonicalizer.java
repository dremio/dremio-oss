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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link SimpleExpressionCanonicalizer}. */
public class TestSimpleExpressionCanonicalizer {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType =
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);

  private static final RexNode col_a = rexBuilder.makeInputRef(intColumnType, 0);
  private static final RexNode col_b = rexBuilder.makeInputRef(intColumnType, 1);
  private static final RexNode col_c = rexBuilder.makeInputRef(intColumnType, 2);

  private static final RexNode intLit10 =
      rexBuilder.makeLiteral(10, typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 =
      rexBuilder.makeLiteral(20, typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 =
      rexBuilder.makeLiteral(30, typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testCanonicalizeLeftInputRightLiteral() {
    testCanonicalizeExpression(rLt(col_a, intLit10), "<($0, 10)");
  }

  @Test
  public void testCanonicalizeLeftLiteralRightInput() {
    testCanonicalizeExpression(rGt(intLit10, col_a), "<($0, 10)");
  }

  @Test
  public void testCanonicalizeBothLiteral() {
    testCanonicalizeExpression(rGt(col_b, col_a), "<($0, $1)");
  }

  @Test
  public void testCanonicalizeNested() {
    testCanonicalizeExpression(
        rAnd(
            rOr(rLt(col_a, intLit10), rGt(intLit10, col_b)),
            rOr(rLt(col_c, col_a), rGt(col_b, col_a))),
        "AND(" + "OR(<($0, 10), <($1, 10)), " + "OR(>($0, $2), <($0, $1)))");
  }

  @Test
  public void testToNnfNoNot() {
    testToNnf(
        rAnd(
            rOr(rLt(col_a, intLit10), rGt(col_b, intLit20)),
            rOr(rLt(col_a, intLit10), rGt(col_c, intLit30))),
        "AND(" + "OR(<($0, 10), >($1, 20)), " + "OR(<($0, 10), >($2, 30)))");
  }

  @Test
  public void testToNnfNotAdjacentLeaf() {
    testToNnf(
        rAnd(
            rOr(rLt(col_a, intLit10), rNot(rGt(col_b, intLit20))),
            rOr(rLt(col_a, intLit10), rNot(rGt(col_c, intLit30)))),
        "AND(" + "OR(<($0, 10), NOT(>($1, 20))), " + "OR(<($0, 10), NOT(>($2, 30))))");
  }

  @Test
  public void testToNnfNotOneLevelAboveLeaf() {
    testToNnf(
        rAnd(
            rNot(rOr(rLt(col_a, intLit10), rGt(col_b, intLit20))),
            rNot(rOr(rLt(col_a, intLit10), rGt(col_c, intLit30)))),
        "AND(NOT(<($0, 10)), NOT(>($1, 20)), NOT(>($2, 30)))");
  }

  @Test
  public void testToNnfNotTwoLevelsAboveLeaf() {
    testToNnf(
        rNot(
            rAnd(
                rOr(rLt(col_a, intLit10), rGt(col_b, intLit20)),
                rOr(rLt(col_a, intLit10), rGt(col_c, intLit30)))),
        "OR(" + "AND(NOT(<($0, 10)), NOT(>($1, 20))), " + "AND(NOT(<($0, 10)), NOT(>($2, 30))))");
  }

  private void testCanonicalizeExpression(RexNode rexNode, String expectedRexNodeString) {
    RexNode result = SimpleExpressionCanonicalizer.canonicalizeExpression(rexNode, rexBuilder);
    Assert.assertEquals(expectedRexNodeString, result.toString());
  }

  private void testToNnf(RexNode rexNode, String expectedRexNodeString) {
    RexNode result = SimpleExpressionCanonicalizer.toNnf(rexNode, rexBuilder);
    Assert.assertEquals(expectedRexNodeString, result.toString());
  }

  private static RexNode rLt(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(LESS_THAN, rexNode1, rexNode2);
  }

  private static RexNode rGt(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(GREATER_THAN, rexNode1, rexNode2);
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
}
