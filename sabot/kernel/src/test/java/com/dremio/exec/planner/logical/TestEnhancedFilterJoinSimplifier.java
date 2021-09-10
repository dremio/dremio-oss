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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import java.util.Arrays;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;

/**
 * Test for {@link EnhancedFilterJoinSimplifier}.
 */
public class TestEnhancedFilterJoinSimplifier {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);

  private static final RexNode col_R_a = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_R_c = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_S_x = rexBuilder.makeInputRef(intColumnType,3);
  private static final RexNode col_S_y = rexBuilder.makeInputRef(intColumnType,4);

  private static final RexNode intLit10 = rexBuilder.makeLiteral(10,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 = rexBuilder.makeLiteral(30,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit40 = rexBuilder.makeLiteral(40,
    typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testAndRootNoPush() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_R_b, intLit20)),
      Arrays.asList(
        Pair.of(rexBuilder.makeLiteral(true), rEq(col_R_a, intLit10)),
        Pair.of(rexBuilder.makeLiteral(true), rEq(col_R_b, intLit20))),
      SqlKind.AND,
      true,
      rexBuilder);
    Assert.assertEquals("AND(=($0, 10), =($1, 20))", simplifiedFilter.toString());
  }

  @Test
  public void testAndRootAllPush() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_R_b, intLit20)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rEq(col_R_a, intLit10)),
        Pair.of(rEq(col_R_b, intLit20), rEq(col_R_b, intLit20))),
      SqlKind.AND,
      true,
      rexBuilder);
    Assert.assertEquals("true", simplifiedFilter.toString());
  }

  @Test
  public void testAndRootPartialPush() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_S_x, intLit20)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rEq(col_R_a, intLit10)),
        Pair.of(rexBuilder.makeLiteral(true), rEq(col_S_x, intLit20))),
      SqlKind.AND,
      true,
      rexBuilder);
    Assert.assertEquals("=($3, 20)", simplifiedFilter.toString());
  }

  @Test
  public void testAndNotRootPartialPush() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_S_x, intLit20)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rEq(col_R_a, intLit10)),
        Pair.of(rexBuilder.makeLiteral(true), rEq(col_S_x, intLit20))),
      SqlKind.AND,
      false,
      rexBuilder);
    Assert.assertEquals("AND(=($0, 10), =($3, 20))", simplifiedFilter.toString());
  }

  @Test
  public void testOrRootAllEntirelyPushed() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_R_b, intLit20),
        rEq(col_R_c, intLit30)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rEq(col_R_a, intLit10)),
        Pair.of(rEq(col_R_b, intLit20), rEq(col_R_b, intLit20)),
        Pair.of(rEq(col_R_c, intLit30), rEq(col_R_c, intLit30))),
      SqlKind.OR,
      true,
      rexBuilder);
    Assert.assertEquals("true", simplifiedFilter.toString());
  }

  @Test
  public void testOrNotRootAllEntirelyPushed() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rEq(col_R_a, intLit10),
        rEq(col_R_b, intLit20),
        rEq(col_R_c, intLit30)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rEq(col_R_a, intLit10)),
        Pair.of(rEq(col_R_b, intLit20), rEq(col_R_b, intLit20)),
        Pair.of(rEq(col_R_c, intLit30), rEq(col_R_c, intLit30))),
      SqlKind.OR,
      false,
      rexBuilder);
    Assert.assertEquals("OR(=($0, 10), =($1, 20), =($2, 30))", simplifiedFilter.toString());
  }

  @Test
  public void testOrNonEntirelyPushedHasCommonExtraction() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_y, intLit30)),
        rEq(col_R_b, intLit40)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20))),
        Pair.of(rEq(col_R_a, intLit10), rAnd(rEq(col_R_a, intLit10), rEq(col_S_y, intLit30))),
        Pair.of(rEq(col_R_b, intLit40), rEq(col_R_b, intLit40))),
      SqlKind.OR,
      true,
      rexBuilder);
    Assert.assertEquals("OR(=($3, 20), =($4, 30), =($1, 40))", simplifiedFilter.toString());
  }

  @Test
  public void testOrNonEntirelyPushedHasNoCommonExtraction() {
    RexNode simplifiedFilter = EnhancedFilterJoinSimplifier.simplifyConDisjunction(
      Arrays.asList(
        rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20)),
        rAnd(rEq(col_R_a, intLit20), rEq(col_S_y, intLit30)),
        rEq(col_R_b, intLit40)),
      Arrays.asList(
        Pair.of(rEq(col_R_a, intLit10), rAnd(rEq(col_R_a, intLit10), rEq(col_S_x, intLit20))),
        Pair.of(rEq(col_R_a, intLit20), rAnd(rEq(col_R_a, intLit20), rEq(col_S_y, intLit30))),
        Pair.of(rEq(col_R_b, intLit40), rEq(col_R_b, intLit40))),
      SqlKind.OR,
      true,
      rexBuilder);
    Assert.assertEquals("OR(AND(=($0, 10), =($3, 20)), AND(=($0, 20), =($4, 30)), =($1, 40))",
      simplifiedFilter.toString());
  }

  private static RexNode rEq(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(EQUALS, rexNode1, rexNode2);
  }

  private static RexNode rAnd(RexNode... rexNodes) {
    return rexBuilder.makeCall(AND, rexNodes);
  }
}
