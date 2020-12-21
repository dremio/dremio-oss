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
package com.dremio.exec.store.hive.orc;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.planner.RexBuilderTestBase;

/**
 * Unit tests for {@link ORCFindRelevantFilters}
 */
public class TestORCFindRelevantFilters extends RexBuilderTestBase {
  private final ORCFindRelevantFilters finder = new ORCFindRelevantFilters(builder, null);

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);

  @Test
  public void equal() {
    singleOpTest(EQUALS);
  }

  @Test
  public void notEqual() {
    singleOpTest(NOT_EQUALS);
  }

  @Test
  public void lessThan() {
    singleOpTest(LESS_THAN);
  }

  @Test
  public void lessThanOrEqual() {
    singleOpTest(LESS_THAN_OR_EQUAL);
  }

  @Test
  public void greaterThan() {
    singleOpTest(GREATER_THAN);
  }

  @Test
  public void greaterThanOrEqual() {
    singleOpTest(GREATER_THAN_OR_EQUAL);
  }

  @Test
  public void isNull() {
    RexNode isNullExpr = builder.makeCall(IS_NULL, asList(input(0)));
    assertEqualsDigest(isNullExpr, isNullExpr.accept(finder));
  }

  @Test
  public void isNotNull() {
    RexNode isNotNullExpr = builder.makeCall(IS_NOT_NULL, asList(input(0)));
    assertEqualsDigest(isNotNullExpr, isNotNullExpr.accept(finder));
  }

  @Test
  public void notExpr() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode notExpr = builder.makeCall(SqlStdOperatorTable.NOT, asList(gtExpr));
    assertEqualsDigest(notExpr, notExpr.accept(finder));
  }

  @Test
  public void notAndExpr() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode andExpr = builder.makeCall(AND, asList(gtExpr, eqVarcharExpr));
    RexNode notExpr = builder.makeCall(SqlStdOperatorTable.NOT, asList(andExpr));
    assertEqualsDigest(notExpr, notExpr.accept(finder));
  }

  @Test
  public void or() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode orExpr = builder.makeCall(OR, asList(gtExpr, eqVarcharExpr));
    assertEqualsDigest(orExpr, orExpr.accept(finder));
  }

  @Test
  public void orWithPartialSupport() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode twoColCmpExpr = builder.makeCall(EQUALS, asList(input(3), input(2)));
    RexNode orExpr = builder.makeCall(OR, asList(gtExpr, eqVarcharExpr, twoColCmpExpr));
    assertNull(orExpr.accept(finder));
  }

  @Test
  public void and() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode andExpr = builder.makeCall(AND, asList(gtExpr, eqVarcharExpr));
    assertEqualsDigest(andExpr, andExpr.accept(finder));
  }

  @Test
  public void andWithPartialSupport() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode twoColCmpExpr = builder.makeCall(EQUALS, asList(input(3), input(2)));
    RexNode andExpr = builder.makeCall(AND, asList(gtExpr, eqVarcharExpr, twoColCmpExpr));
    assertEqualsDigest(builder.makeCall(AND, asList(gtExpr, eqVarcharExpr)), andExpr.accept(finder));
  }

  @Test
  public void in() {
    RexNode inExpr = builder.makeCall(IN,
        asList(
            builder.makeCall(EQUALS, asList(input(0), intLit(0, 23))),
            builder.makeCall(EQUALS, asList(input(0), intLit(0, 234))),
            builder.makeCall(EQUALS, asList(input(0), intLit(0, 23423)))
        )
    );
    assertEqualsDigest(inExpr, inExpr.accept(finder));
  }

  @Test
  public void boolInputRefAnd() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode boolInputRef = input(7);
    RexNode inputAndExpr = builder.makeCall(AND, asList(gtExpr, boolInputRef));

    RexNode outputBoolInputRef = builder.makeCall(EQUALS, asList(input(7), boolLit(7, true)));
    RexNode outputAndExpr = builder.makeCall(AND, asList(gtExpr, outputBoolInputRef));

    assertEqualsDigest(outputAndExpr, inputAndExpr.accept(finder));
  }

  @Test
  public void boolInputRefOr() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode boolInputRef = input(7);
    RexNode inputOrExpr = builder.makeCall(OR, asList(gtExpr, boolInputRef));

    RexNode outputBoolInputRef = builder.makeCall(EQUALS, asList(input(7), boolLit(7, true)));
    RexNode outputOrExpr = builder.makeCall(OR, asList(gtExpr, outputBoolInputRef));

    assertEqualsDigest(outputOrExpr, inputOrExpr.accept(finder));
  }

  private void singleOpTest(SqlOperator op) {
    RexNode eqInt = builder.makeCall(op, asList(input(0), intLit(0, 23)));
    assertEqualsDigest(eqInt, eqInt.accept(finder));

    RexNode eqBigInt = builder.makeCall(op, asList(input(1), bigIntLit(1, 23234234L)));
    assertEqualsDigest(eqBigInt, eqBigInt.accept(finder));

    RexNode eqFloat = builder.makeCall(op, asList(input(2), floatLit(2, 23234.233f)));
    assertEqualsDigest(eqFloat, eqFloat.accept(finder));

    RexNode eqDouble = builder.makeCall(op, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEqualsDigest(eqDouble, eqDouble.accept(finder));

    RexNode eqDate = builder.makeCall(op, asList(input(4), dateLit(4, 1231293712)));
    assertEqualsDigest(eqDate, eqDate.accept(finder));

    RexNode eqTs = builder.makeCall(op, asList(input(5), tsLit(5, 232349893L)));
    assertEqualsDigest(eqTs, eqTs.accept(finder));

    RexNode eqVarchar = builder.makeCall(op, asList(input(6), varcharLit(6, "str")));
    assertEqualsDigest(eqVarchar, eqVarchar.accept(finder));

    RexNode eqBool = builder.makeCall(op, asList(input(7), boolLit(7, false)));
    assertEqualsDigest(eqBool, eqBool.accept(finder));
  }

  protected void assertEqualsDigest(RexNode exp, RexNode actual) {
    assertEquals(exp.toString(), actual.toString());
  }
}
