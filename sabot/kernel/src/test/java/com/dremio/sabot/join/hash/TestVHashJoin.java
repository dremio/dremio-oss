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
package com.dremio.sabot.join.hash;

import static com.dremio.sabot.Fixtures.NULL_INT;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.join.BaseTestJoin;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.VectorizedHashJoinOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVHashJoin extends BaseTestJoin {
  private static final String[] LEFT_SCHEMA = {
    "all_int_l", "all_string_l", "some_int_l", "some_string_l", "none_int_l", "none_string_l"
  };
  private static final String[] RIGHT_SCHEMA = {
    "all_int_r", "all_string_r", "some_int_r", "some_string_r", "none_int_r", "none_string_r"
  };
  private static final Object[][] L_ROWS = {
    {1, "str1", 21, "str21", 31, "str3100"},
    {2, "str2", 22, "str22", 32, "str3"},
    {3, "str3", 23, "str23", 33, "str3300"},
    {4, "str4", 24, "str24", 34, "str4"}
  };
  private static final Object[][] R_ROWS = {
    {1, "str1", 31, "str31", 41, "var41"},
    {2, "str2", 22, "str22", 42, "var42"},
    {3, "str3", 33, "str23", 43, "var43"},
    {4, "str4", 24, "str34", 44, "var44"}
  };

  private static final Object[][] D_R_ROWS = {
    {2, "str1", 31, "str31", 41, "var41"},
    {2, "str2", 22, "str22", 22, "var42"},
    {2, "str3", 33, "str23", 43, "var43"},
    {2, "str4", 24, "str34", 44, "var44"}
  };

  private static final Object[] NULL_LEFT = {
    NULL_INT, NULL_VARCHAR, NULL_INT, NULL_VARCHAR, NULL_INT, NULL_VARCHAR
  };
  private static final Object[] NULL_RIGHT = {
    NULL_INT, NULL_VARCHAR, NULL_INT, NULL_VARCHAR, NULL_INT, NULL_VARCHAR
  };

  private static final String[] ALL_SCHEMA;

  static {
    ALL_SCHEMA = new String[LEFT_SCHEMA.length + RIGHT_SCHEMA.length];
    System.arraycopy(RIGHT_SCHEMA, 0, ALL_SCHEMA, 0, RIGHT_SCHEMA.length);
    System.arraycopy(LEFT_SCHEMA, 0, ALL_SCHEMA, RIGHT_SCHEMA.length, LEFT_SCHEMA.length);
  }

  private static final Fixtures.Table LEFT =
      t(th(LEFT_SCHEMA), tr(L_ROWS[0]), tr(L_ROWS[1]), tr(L_ROWS[2]), tr(L_ROWS[3]));

  private static final Fixtures.Table RIGHT =
      t(th(RIGHT_SCHEMA), tr(R_ROWS[0]), tr(R_ROWS[1]), tr(R_ROWS[2]), tr(R_ROWS[3]));

  private static final Fixtures.Table D_RIGHT =
      t(th(RIGHT_SCHEMA), tr(D_R_ROWS[0]), tr(D_R_ROWS[1]), tr(D_R_ROWS[2]), tr(D_R_ROWS[3]));

  private static final Fixtures.HeaderRow TH = th(ALL_SCHEMA);
  private static final Fixtures.Table EMPTY_TABLE = t(TH, true, tr(combine(NULL_LEFT, NULL_RIGHT)));
  private final OptionManager options = testContext.getOptions();

  @Before
  public void before() {
    options.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, HashJoinOperator.ENABLE_SPILL.getOptionName(), false));
  }

  @After
  public void after() {
    options.setOption(HashJoinOperator.ENABLE_SPILL.getDefault());
  }

  @Override
  protected JoinInfo getJoinInfo(List<JoinCondition> conditions, JoinRelType type) {
    return new JoinInfo(
        VectorizedHashJoinOperator.class,
        new HashJoinPOP(PROPS, null, null, conditions, null, type, true, null));
  }

  @Test
  public void manyColumns() throws Exception {
    baseManyColumns();
  }

  @Test
  public void manyColumnsDecimal() throws Exception {
    baseManyColumnsDecimal();
  }

  @Test
  public void testExtraConditionInnerJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Arrays.asList(
                new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r")),
                new JoinCondition("EQUALS", f("all_string_l"), f("all_string_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.INNER);

    // only 1 and 3 matches for extra condition, while all matches for join
    final Fixtures.Table expected =
        t(TH, tr(combine(R_ROWS[0], L_ROWS[0])), tr(combine(R_ROWS[2], L_ROWS[2])));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testExtraConditionFullJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Arrays.asList(
                new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r")),
                new JoinCondition("EQUALS", f("all_string_l"), f("all_string_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.FULL);

    // only 1 and 3 matches for extra condition, while all matches for join
    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(R_ROWS[0], L_ROWS[0])),
            tr(combine(NULL_RIGHT, L_ROWS[1])),
            tr(combine(R_ROWS[2], L_ROWS[2])),
            tr(combine(NULL_RIGHT, L_ROWS[3])),
            tr(combine(R_ROWS[1], NULL_LEFT)),
            tr(combine(R_ROWS[3], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testExtraConditionLeftJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.LEFT);

    // only 1 and 3 matches for extra condition, while all matches for join
    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(R_ROWS[0], L_ROWS[0])),
            tr(combine(NULL_RIGHT, L_ROWS[1])),
            tr(combine(R_ROWS[2], L_ROWS[2])),
            tr(combine(NULL_RIGHT, L_ROWS[3])));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testExtraConditionRightJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.RIGHT);

    // only 1 and 3 matches for extra condition, while all matches for join
    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(R_ROWS[0], L_ROWS[0])),
            tr(combine(R_ROWS[2], L_ROWS[2])),
            tr(combine(R_ROWS[1], NULL_LEFT)),
            tr(combine(R_ROWS[3], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testInequalityKeyRightSide() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r"))),
            buildAndCondition(
                lengthVarcharGreaterThan("none_string_l", "none_string_r"),
                intGreaterThan("all_int_r", false, 1)),
            JoinRelType.LEFT);

    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(NULL_RIGHT, L_ROWS[0])),
            tr(combine(NULL_RIGHT, L_ROWS[1])),
            tr(combine(R_ROWS[2], L_ROWS[2])),
            tr(combine(NULL_RIGHT, L_ROWS[3])));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testInequalityKeyLeftSide() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r"))),
            buildAndCondition(
                lengthVarcharGreaterThan("none_string_l", "none_string_r"),
                intGreaterThan("all_int_l", true, 1)),
            JoinRelType.RIGHT);

    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(R_ROWS[2], L_ROWS[2])),
            tr(combine(R_ROWS[0], NULL_LEFT)),
            tr(combine(R_ROWS[1], NULL_LEFT)),
            tr(combine(R_ROWS[3], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testNoMatchesFullJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(
                new JoinCondition("EQUALS", f("some_int_l"), f("some_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.FULL);

    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(NULL_RIGHT, L_ROWS[0])),
            tr(combine(NULL_RIGHT, L_ROWS[1])),
            tr(combine(NULL_RIGHT, L_ROWS[2])),
            tr(combine(NULL_RIGHT, L_ROWS[3])),
            tr(combine(R_ROWS[0], NULL_LEFT)),
            tr(combine(R_ROWS[1], NULL_LEFT)),
            tr(combine(R_ROWS[2], NULL_LEFT)),
            tr(combine(R_ROWS[3], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testNoMatchesLeftJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(
                new JoinCondition("EQUALS", f("some_int_l"), f("some_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.LEFT);

    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(NULL_RIGHT, L_ROWS[0])),
            tr(combine(NULL_RIGHT, L_ROWS[1])),
            tr(combine(NULL_RIGHT, L_ROWS[2])),
            tr(combine(NULL_RIGHT, L_ROWS[3])));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testNoMatchesRightJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(
                new JoinCondition("EQUALS", f("some_int_l"), f("some_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.RIGHT);

    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(R_ROWS[0], NULL_LEFT)),
            tr(combine(R_ROWS[1], NULL_LEFT)),
            tr(combine(R_ROWS[2], NULL_LEFT)),
            tr(combine(R_ROWS[3], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void testNoMatchesInnerJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(
                new JoinCondition("EQUALS", f("none_int_l"), f("none_int_r"))),
            buildAndCondition(lengthVarcharGreaterThan("none_string_l", "none_string_r")),
            JoinRelType.INNER);

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        EMPTY_TABLE);
  }

  @Test
  public void testDuplicatesRightJoin() throws Exception {
    JoinInfo info =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("all_int_l"), f("all_int_r"))),
            buildAndCondition(intGreaterThan("none_int_l", "none_int_r")),
            JoinRelType.RIGHT);

    // only 1 and 3 matches for extra condition, while all matches for join
    final Fixtures.Table expected =
        t(
            TH,
            tr(combine(D_R_ROWS[1], L_ROWS[1])),
            tr(combine(D_R_ROWS[0], NULL_LEFT)),
            tr(combine(D_R_ROWS[3], NULL_LEFT)),
            tr(combine(D_R_ROWS[2], NULL_LEFT)));

    validateDual(
        info.operator,
        info.clazz,
        LEFT.toGenerator(getTestAllocator()),
        D_RIGHT.toGenerator(getTestAllocator()),
        DEFAULT_BATCH,
        expected);
  }

  @Test
  public void hugeBatchWithExtraConditionInner() throws Exception {
    JoinInfo joinInfo =
        getJoinInfo(
            Collections.singletonList(new JoinCondition("EQUALS", f("a"), f("b"))),
            buildAndCondition(lengthVarcharGreaterThan("c", "d")),
            JoinRelType.INNER);

    final Random rand = new Random();
    final int batchSize = 65536;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    int k = 0;
    for (int i = 0; i < batchSize; i++) {
      final int boundedLength = i % 100 + 1;
      final String s1 = buildString(1 + rand.nextInt(boundedLength));
      final String s2 = buildString(1 + rand.nextInt(boundedLength));

      leftRows[i] = tr((long) i, s1);
      rightRows[i] = tr((long) i, s2);
      if (s1.length() > s2.length()) {
        expectedRows[k++] = tr((long) i, s2, (long) i, s1);
      }
    }
    expectedRows = Arrays.copyOf(expectedRows, k);

    final Fixtures.Table left = t(th("a", "c"), leftRows);
    final Fixtures.Table right = t(th("b", "d"), rightRows);
    final Fixtures.Table expected = t(th("b", "d", "a", "c"), expectedRows);

    validateDual(
        joinInfo.operator,
        joinInfo.clazz,
        left.toGenerator(getTestAllocator()),
        right.toGenerator(getTestAllocator()),
        batchSize,
        expected);
  }

  protected JoinInfo getJoinInfo(
      List<JoinCondition> conditions, LogicalExpression extraCondition, JoinRelType type) {
    return new JoinInfo(
        VectorizedHashJoinOperator.class,
        new HashJoinPOP(PROPS, null, null, conditions, extraCondition, type, true, null));
  }

  private LogicalExpression buildAndCondition(LogicalExpression... exprs) {
    if (exprs.length <= 1) {
      return exprs[0];
    } else {
      return new BooleanOperator("booleanAnd", Arrays.asList(exprs));
    }
  }

  private LogicalExpression lengthVarcharGreaterThan(String leftColumn, String rightColumn) {
    List<LogicalExpression> args = new ArrayList<>();
    FieldReference ref = FieldReference.getWithQuotedRef(leftColumn);
    args.add(new InputReference(0, ref));
    FunctionCall f1 = new FunctionCall("length", args);
    List<LogicalExpression> args1 = new ArrayList<>();
    FieldReference ref1 = FieldReference.getWithQuotedRef(rightColumn);
    args1.add(new InputReference(1, ref1));
    FunctionCall f2 = new FunctionCall("length", args1);
    List<LogicalExpression> args2 = new ArrayList<>();
    args2.add(f1);
    args2.add(f2);
    return new FunctionCall("greater_than", args2);
  }

  private LogicalExpression intGreaterThan(String intColumn, boolean left, int value) {
    List<LogicalExpression> args = new ArrayList<>();
    FieldReference ref = FieldReference.getWithQuotedRef(intColumn);
    args.add(new InputReference(left ? 0 : 1, ref));
    args.add(new ValueExpressions.IntExpression(value));
    return new FunctionCall("greater_than", args);
  }

  private LogicalExpression intGreaterThan(String leftIntColumn, String rightIntColumn) {
    List<LogicalExpression> args = new ArrayList<>();
    FieldReference lref = FieldReference.getWithQuotedRef(leftIntColumn);
    FieldReference rref = FieldReference.getWithQuotedRef(rightIntColumn);
    args.add(new InputReference(0, lref));
    args.add(new InputReference(1, rref));
    return new FunctionCall("greater_than", args);
  }

  private static Object[] combine(Object[] one, Object[] two) {
    final Object[] ret = new Object[one.length + two.length];
    System.arraycopy(one, 0, ret, 0, one.length);
    System.arraycopy(two, 0, ret, one.length, two.length);
    return ret;
  }

  private static String buildString(int expectedLength) {
    final char[] c = new char[expectedLength];
    for (int i = 0; i < expectedLength; i++) {
      c[i] = (char) ('a' + i % 26);
    }
    return new String(c);
  }
}
