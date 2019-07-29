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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.DateString;
import org.apache.commons.net.ntp.TimeStamp;
import org.junit.Test;

import com.dremio.exec.planner.RexBuilderTestBase;

/**
 * Unit tests for {@link ORCSearchArgumentGenerator}
 */
public class TestORCSearchArgumentGenerator extends RexBuilderTestBase {
  @Test
  public void equal() {
    RexNode eqInt = builder.makeCall(EQUALS, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (EQUALS intC 23), expr = leaf-0", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(EQUALS, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (EQUALS bigIntC 23234234), expr = leaf-0", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(EQUALS, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (EQUALS floatC 23234.23), expr = leaf-0", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(EQUALS, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (EQUALS doubleC 2.35234234234324E8), expr = leaf-0", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(EQUALS, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (EQUALS dateC " + strDate + "), expr = leaf-0", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(EQUALS, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (EQUALS tsC " + strTime.toString() + "), expr = leaf-0", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (EQUALS varcharC str), expr = leaf-0", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(EQUALS, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (EQUALS boolC true), expr = leaf-0", sarg(eqBool));
  }

  @Test
  public void notEqual() {
    RexNode eqInt = builder.makeCall(NOT_EQUALS, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (EQUALS intC 23), expr = (not leaf-0)", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(NOT_EQUALS, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (EQUALS bigIntC 23234234), expr = (not leaf-0)", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(NOT_EQUALS, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (EQUALS floatC 23234.23), expr = (not leaf-0)", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(NOT_EQUALS, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (EQUALS doubleC 2.35234234234324E8), expr = (not leaf-0)", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(NOT_EQUALS, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (EQUALS dateC " + strDate + "), expr = (not leaf-0)", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(NOT_EQUALS, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (EQUALS tsC " + strTime.toString() + "), expr = (not leaf-0)", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(NOT_EQUALS, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (EQUALS varcharC str), expr = (not leaf-0)", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(NOT_EQUALS, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (EQUALS boolC true), expr = (not leaf-0)", sarg(eqBool));
  }

  @Test
  public void lessThan() {
    RexNode eqInt = builder.makeCall(LESS_THAN, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (LESS_THAN intC 23), expr = leaf-0", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(LESS_THAN, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (LESS_THAN bigIntC 23234234), expr = leaf-0", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(LESS_THAN, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (LESS_THAN floatC 23234.23), expr = leaf-0", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(LESS_THAN, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (LESS_THAN doubleC 2.35234234234324E8), expr = leaf-0", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(LESS_THAN, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (LESS_THAN dateC " + strDate + "), expr = leaf-0", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(LESS_THAN, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (LESS_THAN tsC " + strTime.toString() + "), expr = leaf-0", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(LESS_THAN, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (LESS_THAN varcharC str), expr = leaf-0", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(LESS_THAN, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (LESS_THAN boolC true), expr = leaf-0", sarg(eqBool));
  }

  @Test
  public void lessThanOrEqual() {
    RexNode eqInt = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS intC 23), expr = leaf-0", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS bigIntC 23234234), expr = leaf-0", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS floatC 23234.23), expr = leaf-0", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS doubleC 2.35234234234324E8), expr = leaf-0", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (LESS_THAN_EQUALS dateC " + strDate + "), expr = leaf-0", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS tsC " + strTime.toString() + "), expr = leaf-0", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS varcharC str), expr = leaf-0", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(LESS_THAN_OR_EQUAL, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS boolC true), expr = leaf-0", sarg(eqBool));
  }

  @Test
  public void greaterThan() {
    RexNode eqInt = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS intC 23), expr = (not leaf-0)", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(GREATER_THAN, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS bigIntC 23234234), expr = (not leaf-0)", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(GREATER_THAN, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS floatC 23234.23), expr = (not leaf-0)", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(GREATER_THAN, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS doubleC 2.35234234234324E8), expr = (not leaf-0)", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(GREATER_THAN, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (LESS_THAN_EQUALS dateC " + strDate + "), expr = (not leaf-0)", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(GREATER_THAN, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS tsC " + strTime.toString() + "), expr = (not leaf-0)", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(GREATER_THAN, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS varcharC str), expr = (not leaf-0)", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(GREATER_THAN, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS boolC true), expr = (not leaf-0)", sarg(eqBool));
  }

  @Test
  public void greaterThanOrEqual() {
    RexNode eqInt = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(0), intLit(0, 23)));
    assertEquals("leaf-0 = (LESS_THAN intC 23), expr = (not leaf-0)", sarg(eqInt));

    RexNode eqBigInt = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(1), bigIntLit(1, 23234234L)));
    assertEquals("leaf-0 = (LESS_THAN bigIntC 23234234), expr = (not leaf-0)", sarg(eqBigInt));

    RexNode eqFloat = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(2), floatLit(2, 23234.23f)));
    assertEquals("leaf-0 = (LESS_THAN floatC 23234.23), expr = (not leaf-0)", sarg(eqFloat));

    RexNode eqDouble = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(3), doubleLit(3, 235234234.234324d)));
    assertEquals("leaf-0 = (LESS_THAN doubleC 2.35234234234324E8), expr = (not leaf-0)", sarg(eqDouble));

    final int daysSinceEpoch = 24233;
    RexNode eqDate = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(4), dateLit(4, daysSinceEpoch)));
    Date date = new Date(DateString.fromDaysSinceEpoch(daysSinceEpoch).getMillisSinceEpoch());
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String strDate = dateFormat.format(date);
    assertEquals("leaf-0 = (LESS_THAN dateC " + strDate + "), expr = (not leaf-0)", sarg(eqDate));

    final long millisSinceEpoch = 232349893L;
    RexNode eqTs = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(5), tsLit(5, millisSinceEpoch)));
    final StringBuilder strTime = new StringBuilder();
    strTime.append(new Timestamp(millisSinceEpoch));
    assertEquals("leaf-0 = (LESS_THAN tsC " + strTime.toString() + "), expr = (not leaf-0)", sarg(eqTs));

    RexNode eqVarchar = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(6), varcharLit(6, "str")));
    assertEquals("leaf-0 = (LESS_THAN varcharC str), expr = (not leaf-0)", sarg(eqVarchar));

    RexNode eqBool = builder.makeCall(GREATER_THAN_OR_EQUAL, asList(input(7), boolLit(7, true)));
    assertEquals("leaf-0 = (LESS_THAN boolC true), expr = (not leaf-0)", sarg(eqBool));
  }

  @Test
  public void isNull() {
    RexNode isNullExpr = builder.makeCall(IS_NULL, asList(input(0)));
    assertEquals("leaf-0 = (IS_NULL intC), expr = leaf-0", sarg(isNullExpr));
  }

  @Test
  public void isNotNull() {
    RexNode isNotNullExpr = builder.makeCall(IS_NOT_NULL, asList(input(0)));
    assertEquals("leaf-0 = (IS_NULL intC), expr = (not leaf-0)", sarg(isNotNullExpr));
  }

  @Test
  public void notExpr() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode notExpr = builder.makeCall(NOT, asList(gtExpr));
    assertEquals("leaf-0 = (LESS_THAN_EQUALS intC 23), expr = leaf-0", sarg(notExpr));
  }

  @Test
  public void notAndExpr() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode andExpr = builder.makeCall(AND, asList(gtExpr, eqVarcharExpr));
    RexNode notExpr = builder.makeCall(NOT, asList(andExpr));
    assertEquals(
        "leaf-0 = (LESS_THAN_EQUALS intC 23), leaf-1 = (EQUALS varcharC str), expr = (or leaf-0 (not leaf-1))",
        sarg(notExpr)
    );
  }

  @Test
  public void or() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode orExpr = builder.makeCall(OR, asList(gtExpr, eqVarcharExpr));
    assertEquals(
        "leaf-0 = (LESS_THAN_EQUALS intC 23), leaf-1 = (EQUALS varcharC str), expr = (or (not leaf-0) leaf-1)",
        sarg(orExpr)
    );
  }

  @Test
  public void and() {
    RexNode gtExpr = builder.makeCall(GREATER_THAN, asList(input(0), intLit(0, 23)));
    RexNode eqVarcharExpr = builder.makeCall(EQUALS, asList(input(6), varcharLit(6, "str")));
    RexNode andExpr = builder.makeCall(AND, asList(gtExpr, eqVarcharExpr));
    assertEquals(
        "leaf-0 = (LESS_THAN_EQUALS intC 23), leaf-1 = (EQUALS varcharC str), expr = (and (not leaf-0) leaf-1)",
        sarg(andExpr)
    );
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
    assertEquals("leaf-0 = (IN intC 23 234 23423), expr = leaf-0", sarg(inExpr));
  }

  private String sarg(RexNode expr) {
    ORCSearchArgumentGenerator gen = new ORCSearchArgumentGenerator(input.getRowType().getFieldNames(), new ArrayList<>());
    expr.accept(gen);
    return gen.get().toString();
  }
}
