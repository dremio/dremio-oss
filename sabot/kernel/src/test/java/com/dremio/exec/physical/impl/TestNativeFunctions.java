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
package com.dremio.exec.physical.impl;

import static com.dremio.sabot.Fixtures.NULL_BIGINT;
import static com.dremio.sabot.Fixtures.NULL_BINARY;
import static com.dremio.sabot.Fixtures.NULL_BOOLEAN;
import static com.dremio.sabot.Fixtures.NULL_DECIMAL;
import static com.dremio.sabot.Fixtures.NULL_DOUBLE;
import static com.dremio.sabot.Fixtures.NULL_FLOAT;
import static com.dremio.sabot.Fixtures.NULL_INT;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.date;
import static com.dremio.sabot.Fixtures.interval_day;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.time;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.ts;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.expression.InExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.llvm.expr.GandivaPushdownSieveHelper;
import com.dremio.sabot.op.project.ProjectOperator;
import com.google.common.collect.Lists;

/*
 * This class tests native (LLVM) implementation of functions.
 */
public class TestNativeFunctions extends BaseTestFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestNativeFunctions.class);
  private static String execPreferenceGandivaOnly = SupportedEngines.CodeGenOption.GandivaOnly.toString();
  private static String execPreferenceMixed = SupportedEngines.CodeGenOption.Gandiva.toString();
  private static String execPreferenceJava = SupportedEngines.CodeGenOption.Java.toString();


  @BeforeClass
  public static void setUpTestNative() {
    if (System.getProperty("execPreferenceGandivaOnly") != null) {
      execPreferenceGandivaOnly = System.getProperty("execPreferenceGandivaOnly");
    }

    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      execPreferenceGandivaOnly));
  }

  @Test
  public void testIf() throws Exception {
    testFunctions(new Object[][]{
      {"case when c0 < 10 then true else false end", 10, false},
      {"case when c0 < 10 then 1 else 0 end", 8, 1},
      {"case when c0 < 10 then 1 else 0 end", NULL_INT, 0},
      {"case when c0 < 10 then 1 else __$INTERNAL_NULL$__ end", 10, NULL_INT},
      {"case when c0 < 10 then 1 else __$INTERNAL_NULL$__ end", 8, 1},
      {"case when c0 < 10 then 1 else __$INTERNAL_NULL$__ end", NULL_INT, NULL_INT},
      {"case when c0 >= 10 AND c0 < 20 then 2 else 0 end", 8, 0},
      {"case when c0 >= 10 AND c0 < 20 then 2 else 0 end", 10, 2},
      {"case when c0 >= 10 AND c0 < 20 then 2 else 0 end", 30, 0},
      {"case when c0 >= 10 AND c0 < 20 then 2 else 0 end", NULL_INT, 0},
    });
  }

  @Test
  public void testStringOutput() throws Exception {
    testFunctions(new Object[][]{
      {"case when c0 >= 10 then 'hello' else 'bye' end", 12, "hello"},
      {"case when c0 >= 10 then 'hello' else 'bye' end", 5, "bye"},
      {"upper(c0)", "hello", "HELLO"},
      {"upper(c0)", NULL_VARCHAR, NULL_VARCHAR},
      {"reverse(c0)", "hello", "olleh"},
      {"reverse(c0)", NULL_VARCHAR, NULL_VARCHAR},
    });
  }

  @Test
  public void testCastDate() throws Exception {
    testFunctions(new Object[][]{
      {"extractYear(castDATE(c0))","0079:10:10", 79l}
    });
    testFunctions(new Object[][]{
      {"extractYear(castDATE(c0))","79:10:10", 1979l}
    });
    testFunctions(new Object[][]{
      {"extractYear(castDATE(c0))","2079:10:10", 2079l}
    });
  }

  @Test
  public void testCastTimestamp() throws Exception {
    testFunctions(new Object[][]{
      {"extractYear(castTIMESTAMP(c0))","0079-10-10", 79l}
    });
    testFunctions(new Object[][]{
      {"extractYear(castTIMESTAMP(c0))","1979-10-10", 1979l}
    });
  }

  @Test
  public void testTimestampDiffMonth() throws Exception {
    testFunctions(new Object[][] {
      {"timestampdiffMonth(c0, c1)", date("2019-01-31"), date("2019-02-28"), 1},
      {"timestampdiffMonth(c0, c1)", date("2020-01-31"), date("2020-02-28"), 0},
      {"timestampdiffMonth(c0, c1)", date("2020-01-31"), date("2020-02-29"), 1},
      {"timestampdiffMonth(c0, c1)", date("2019-03-31"), date("2019-04-30"), 1},
      {"timestampdiffMonth(c0, c1)", date("2020-03-30"), date("2020-02-29"), -1},
      {"timestampdiffMonth(c0, c1)", date("2020-05-31"), date("2020-09-30"), 4},
      {"timestampdiffMonth(c0, c1)", date("2019-10-10"), date("2020-11-21"), 13}
    });
  }

  @Test
  public void testDx14049() throws Exception {
    try {
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceJava
      ));
      testFunctionsCompiledOnly(new Object[][]{
        {"months_between(c0, castDATE(c1))",new LocalDate(), new LocalDateTime(), 0.0}
      });
    } finally {
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceGandivaOnly
      ));
    }
  }

  @Test
  public void testGandivaOnlyFunctions() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"starts_with(c0, 'test')","testMe", true}
    });
  }

  @Test
  public void testToDate() throws Exception {
    testFunctions(new Object[][]{
      {"extractYear(to_date(c0, 'YYYYMMDD', 0))","19671211", 1967l},
      {"extractDay(to_date(c0, 'YYYYMMDD HHMISS', 0))","19671211 010000", 11l}
    });
  }

  // test that to_date with time zone pattern is executed in java.
  @Test
  public void testToDate_Timezone() throws Exception {
    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      execPreferenceMixed));
    testFunctions(new Object[][]{
      {"extractYear(to_date(c0, 'YYYYMMDD HHMISS tzo', 0))","19671211 121212 +08:00", 1967l}
    });
    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      execPreferenceGandivaOnly));
  }

  @Test
  public void testNested() throws Exception {
    testFunctions(new Object[][]{
      {"isnotnull(isnotnull(c0))", 10, true},
      {"isnotnull(isnull(c0))", 10, true},
      {"isnull(isnull(c0))", 10, false},
      {"isnull(isnotnull(c0))", 10, false},
    });
  }


  @Test
  public void testBooleanExpression() throws Exception {
    testFunctions(new Object[][]{
      {"c0 > 20 OR c0 < 10", 9, true},
      {"c0 > 20 OR c0 < 10", 15, false},
      {"c0 > 20 OR c0 < 10", 25, true},
      {"c0 > 10 AND c0 < 20", 9, false},
      {"c0 > 10 AND c0 < 20", 15, true},
      {"c0 > 10 AND c0 < 20", 25, false},
      {"c0 > 10 AND c0 < 20 AND (mod(c0, 2) == 0)", 18, true},
      {"c0 > 10 AND c0 < 20 AND (mod(c0,2) == 0)", 19, false},
      {"(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)", 18, true},
      {"(c0 > 10 AND c0 < 20) OR (mod(c0,2) == 0)", 19, true},
      {"(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)", 24, true},
      {"(c0 > 10 AND c0 < 20) OR (mod(c0,2) == 0)", 25, false},
      {"isnull(c0) OR c0 + c0 == 20", 10, true},
      {"isnotnull(c0) OR c0 + c0 == 21", 10, true},
    });
  }

  @Test
  public void testHash32() throws Exception {
    testFunctions(new Object[][]{
      {"hash32(c0)", NULL_INT, 0},
      {"hash32(c0)", 10, 918068555},
    });
  }

  @Test
  public void testIsNull() throws Exception {
    testFunctions(new Object[][]{
      {"isnull(c0)", 10, false},
      {"isnull(c0)", 10L, false},
      {"isnull(c0)", 10.0F, false},
      {"isnull(c0)", 10.0D, false},
      {"isnull(c0)", "hello", false},
      {"isnull(c0)", NULL_INT, true},
      {"isnull(c0)", NULL_BIGINT, true},
      {"isnull(c0)", NULL_FLOAT, true},
      {"isnull(c0)", NULL_DOUBLE, true},
      {"isnull(c0)", NULL_VARCHAR, true},
    });
  }

  @Test
  public void testIsNotNull() throws Exception {
    testFunctions(new Object[][]{
      {"isnotnull(c0)", 10, true},
      {"isnotnull(c0)", 10L, true},
      {"isnotnull(c0)", 10.0F, true},
      {"isnotnull(c0)", 10.0D, true},
      {"isnotnull(c0)", "hello", true},
      {"isnotnull(c0)", NULL_INT, false},
      {"isnotnull(c0)", NULL_BIGINT, false},
      {"isnotnull(c0)", NULL_FLOAT, false},
      {"isnotnull(c0)", NULL_DOUBLE, false},
      {"isnotnull(c0)", NULL_VARCHAR, false},
    });
  }

  @Test
  public void testFromTimeStamp() throws Exception {
    testFunctions(new Object[][]{
      {"extractMinute(c0)", ts("1970-01-02T10:20:33"), 20l},
      {"extractHour(c0)", ts("1970-01-02T10:20:33"), 10l},
      {"extractDay(c0)", ts("1970-01-02T10:20:33"), 2l},
      {"extractMonth(c0)", ts("1970-01-02T10:20:33"), 1l},
      {"extractYear(c0)", ts("1970-01-02T10:20:33"), 1970l},
      {"extractSecond(c0)", ts("1970-01-02T10:20:33"), 33l},
    });
  }

  @Test
  public void testFromDate() throws Exception {
    testFunctions(new Object[][]{
      {"extractDay(c0)", date("1970-01-02"), 2l},
      {"extractMonth(c0)", date("1970-01-02"), 1l},
      {"extractYear(c0)", date("1970-01-02"), 1970l},
    });

  }

  @Test
  public void testFromTime() throws Exception {
    testFunctions(new Object[][]{
      {"extractMinute(c0)", time("10:20:33"), 20l},
      {"extractHour(c0)", time("10:20:33"), 10l},
      {"extractSecond(c0)", time("10:20:33"), 33l},
    });
  }

  @Test
  public void testRelational() throws Exception {
    testFunctions(new Object[][]{
      {"c0 == c1", 5, 5, true},
      {"c0 == c1", 5, 6, false},
      {"c0 == c1", 5, NULL_INT, NULL_BOOLEAN},
      {"c0 != c1", 5, 5, false},
      {"c0 != c1", 5, 6, true},
      {"c0 != c1", 5, NULL_INT, NULL_BOOLEAN},
      {"c0 < c1", 5, 5, false},
      {"c0 < c1", 5, 6, true},
      {"c0 < c1", 5, NULL_INT, NULL_BOOLEAN},
      {"c0 <= c1", 5, 5, true},
      {"c0 <= c1", 5, 6, true},
      {"c0 <= c1", 5, 4, false},
      {"c0 <= c1", 5, NULL_INT, NULL_BOOLEAN},
      {"c0 > c1", 5, 5, false},
      {"c0 > c1", 5, 6, false},
      {"c0 > c1", 5, NULL_INT, NULL_BOOLEAN},
      {"c0 >= c1", 5, 5, true},
      {"c0 >= c1", 5, 6, false},
      {"c0 >= c1", 5, 4, true},
      {"c0 >= c1", 5, NULL_INT, NULL_BOOLEAN},
    });
  }

  @Test
  public void testNumericInt() throws Exception {
    testFunctions(new Object[][]{
      {"c0 + c1", 5, 10, 15},
      {"c0 - c1", 5, 3, 2},
      {"c0 - c1", 3, 5, -2},
      {"c0 * c1", 5, 6, 30},
      {"c0 * c1", 5, -6, -30},
      {"c0 / c1", 5, 2, 2},
      {"c0 / c1", 6, -2, -3},
      {"c0 + 2", 5, 7},
      {"c0 + __$INTERNAL_NULL$__", 5, NULL_INT},
      {"c0 + 20.0", 5, 25.0F},
      {"mod(c0, c1)", 13, 5, 3},
      {"mod(c0, c1)", 1001, 13L, 0L},
    });
  }

  @Test
  public void testNumericBigInt() throws Exception {
    testFunctions(new Object[][]{
      {"c0 + c1", 5L, 10, 15L},
      {"c0 - c1", 5L, 3, 2L},
      {"c0 - c1", 3L, 5, -2L},
      {"c0 * c1", 5L, 6, 30L},
      {"c0 * c1", 5L, -6, -30L},
      {"c0 / c1", 5L, 2, 2L},
      {"c0 / c1", 6L, -2, -3L},
      {"c0 + 2", 5L, 7L},
      {"c0 + __$INTERNAL_NULL$__", 5L, NULL_BIGINT},
      {"c0 + 20.0", 5L, 25.0F},
    });
  }

  @Test
  public void testNumericFloat() throws Exception {
    testFunctions(new Object[][]{
      {"c0 + c0", 5.1, 10.2},
      {"c0 - c0", 5.1, 0.0},
      {"c0 * c0", 5.1, 26.009999999999998},
      {"c0 / c0", 5.1, 1.0},
      {"c0 + __$INTERNAL_NULL$__", 5.1F, NULL_FLOAT},
      {"c0 + 2", 5.1, 7.1},
    });
  }

  @Test
  public void testNumericDouble() throws Exception {
    testFunctions(new Object[][]{
      {"c0 + c0", 5.1D, 10.2D},
      {"c0 - c0", 5.1D, 0.0D},
      {"c0 * c0", 5.1D, 26.009999999999998D},
      {"c0 / c0", 5.1D, 1.0D},
      {"c0 + __$INTERNAL_NULL$__", 5.1D, NULL_DOUBLE},
      {"c0 + 2", 5.1D, 7.1D},
    });
  }

  @Test
  public void testStrings() throws Exception {
    String hello = "hello";

    testFunctions(new Object[][]{
      {"octet_length(c0)", hello, 5},
      {"bit_length(c0)", hello, 40},
      {"c0 == 'hello'", hello, true},
      {"c0 == 'hello'", "bye", false},
      {"c0 == 'hello'", NULL_VARCHAR, NULL_BOOLEAN},
    });
  }

  @Test
  public void testLike() throws Exception {

    testFunctions(new Object[][]{
      {"like(c0, '%super%')", "superb", true},
      {"like(c0, '%super%')", "awesome superb", true},
      {"like(c0, '%super%')", "supper", false},
      {"like(c0, '%super%')", NULL_VARCHAR, NULL_BOOLEAN},

      {"like(c0, 'arm_')", "arm", false},
      {"like(c0, 'arm_')", "army", true},
      {"like(c0, 'arm_')", "armies", false},
    });
  }

  @Test
  public void testBinary() throws Exception {
    byte[] hello = "hello".getBytes();

    testFunctions(new Object[][]{
      {"c0 == c1", hello, hello, true},
      {"c0 == c1", hello, "bye".getBytes(), false},
      {"c0 == c1", hello, NULL_BINARY, NULL_BOOLEAN},
      {"c0 == __$INTERNAL_NULL$__", hello, NULL_BOOLEAN},
    });
  }

  private void validateMultiRow(String strExpr, Table input, Table output) throws Exception {
    Project p = new Project(OpProps.prototype(), null, Arrays.asList(n(strExpr, "out")));
    validateSingle(p, ProjectOperator.class, input, output);
  }

  @Test
  public void testMultiRowNullIfNull() throws Exception {
    Table input = t(
      th("c0", "c1"),
      tr(1, 2),
      tr(3, 4),
      tr(5, NULL_INT)
    );

    Table output = t(
      th("out"),
      tr(3),
      tr(7),
      tr(NULL_INT)
    );
    validateMultiRow("c0 + c1", input, output);
  }

  @Test
  public void testMultiRowNullNever() throws Exception {
    Table input = t(
      th("c0"),
      tr(0),
      tr(8),
      tr(NULL_INT)
    );

    Table output = t(
      th("out"),
      tr(false),
      tr(false),
      tr(true)
    );
    validateMultiRow("isnull(c0)", input, output);
  }

  @Test
  public void testMultiRowNullInternal() throws Exception {
    Table input = t(
      th("c0"),
      tr(0),
      tr(8),
      tr(NULL_INT)
    );

    Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(false)
    );
    validateMultiRow("isnotnull(c0)", input, output);
  }

  @Test
  public void testMultiRowNestedNullInternal() throws Exception {
    Table input = t(
      th("c0"),
      tr(0),
      tr(8),
      tr(NULL_INT)
    );

    Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true)
    );
    validateMultiRow("isnotnull(isnotnull(c0))", input, output);
  }

  @Test
  public void testMultiRowStrings() throws Exception {
    Table input = t(
      th("c0", "c1"),
      tr("aaa", "aaa"),
      tr("abcd", "abcde"),
      tr("bc", NULL_VARCHAR),
      tr("xyz", "xyz"),
      tr("abx", "abcde")
    );

    Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(NULL_BOOLEAN),
      tr(true),
      tr(true)
    );
    validateMultiRow("c0 >= c1", input, output);
  }

  @Test
  public void testMultiRowCaseWithBooleanExp() throws Exception {
    Table input = t(
      th("c0"),
      tr(3),
      tr(4),
      tr(5),
      tr(6),
      tr(7),
      tr(NULL_INT)
    );

    Table output = t(
      th("out"),
      tr(2),
      tr(1),
      tr(1),
      tr(2),
      tr(2),
      tr(2)
    );
    validateMultiRow("case when c0 > 3 AND c0 < 6 then 1 else 2 end", input, output);
  }

  @Test
  public void testInOptimization() {
    try {
      InExpression.COUNT.set(0);
      testFunctions(new Object[][]{
        {"booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 10, c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 = " +
          "8i, c0 = 9i)", 10, false}}
      );
      //in optimization not enabled for integer types
      Assert.assertEquals(0, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
    }
  }

  @Test
  public void testInOptimizationWithCasts() {
    try {
      InExpression.COUNT.set(0);
      testFunctions(new Object[][]{
        {"booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 10, c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 =" +
          " " +
          "8i, c0 = 9i)", 10l, false}}
      );
      //in optimization not enabled for integer types
      Assert.assertEquals(0, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
    }
  }

  @Test
  public void testInOptimizationWithCastForField() {
    try {
      InExpression.COUNT.set(0);
      testFunctions(new Object[][]{
        {"booleanOr(c0 = 1l, c0 = 2l, c0 = 3l, c0 != 10l, c0 = 4l, c0 = 5l, c0 = 6l, c0 = 7l, c0 " +
          "= 8l, c0 = 9l)", 10, false}}
      );
      Assert.assertEquals(0, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
    }
  }

  @Test
  public void testInOptimizationWithStringCasts() {
    try {
        testContext.getOptions().setOption(OptionValue.createString(
          OptionValue.OptionType.SYSTEM,
          ExecConstants.QUERY_EXEC_OPTION_KEY,
          execPreferenceMixed
        ));
      InExpression.COUNT.set(0);
      // Note: ceil() is not available in Gandiva
      // Hence, this leads to excessive splits and will eventually be implemented in Java using
      // the OrIn optimization.
      testFunctions(new Object[][]{
        {"booleanOr(ceil(c0) = 1l, ceil(c0) = 2l, ceil(c0) = 3l, ceil(c0) != 10l, ceil(c0) = 4l, ceil(c0) = 5l, ceil(c0) = 6l, ceil(c0) = 7l, ceil(c0) " +
          "= 8l, ceil(c0) = 9l)", 10, false}}
      );
      Assert.assertEquals(1, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceGandivaOnly
      ));
    }

    try {
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceMixed
      ));
      InExpression.COUNT.set(0);
      testFunctions(new Object[][]{
        {"booleanOr(ceil(c0) = '1', ceil(c0) = '2', ceil(c0) = '3', ceil(c0) != '4', ceil(c0) = '5', ceil(c0) = " +
          "'6', ceil(c0) = '7', ceil(c0) = '8', ceil(c0) = '9', ceil(c0) = " +
          "'10')", 9l, true}}
      );
      Assert.assertEquals(1, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceGandivaOnly
      ));
    }
  }


  @Test
  public void testUnSupportedInOptimization() {
    try {
      InExpression.COUNT.set(0);
      testFunctions(new Object[][]{
        {"booleanOr((c0 + c1) = 1i, (c0 + c1) = 2i, (c0 + c1) = 3i, (c0 + c1) != 14, (c0 + c1) = " +
          "cast(__$INTERNAL_NULL$__ as int), (c0 + c1) = 4i, (c0 + c1) = 5i, (c0 + c1) = 6i, (c0 " +
          "+ c1) = 7i, (c0 + c1) = 8i, (c0 + c1) = 9i)", 4, 5, true}}
      );
      Assert.assertEquals(0, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
    }
  }

  @Test
  public void testDecimalHashFunctions() throws Exception {
    try {
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        true));
      testFunctions(new Object[][]{
        {"hash32(c0)", NULL_DECIMAL, 0},
        {"hash64(c0)", NULL_DECIMAL, 0L},
        {"hash(c0)", BigDecimal.valueOf(10, 2), 767196228},
        {"hash32(c0)", BigDecimal.valueOf(10, 1), 767196228},
        {"hash64(c0)", BigDecimal.valueOf(10, 1), 7567921574379139112L},
        {"hash32AsDouble(c0)", BigDecimal.valueOf(10, 2), 767196228},
        {"hash64AsDouble(c0)", BigDecimal.valueOf(10, 1), 7567921574379139112L},
      });
    } finally {
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        false));
    }
  }

  @Test
  public void testDecimalHashWithSeedFunctions() throws Exception {
    try{
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        true));
      testFunctions(new Object[][]{
        {"hash32(c0, c1)", NULL_DECIMAL, 10, 10},
        {"hash64(c0, c1)", NULL_DECIMAL, 10, 10L},
        {"hash32(c0, c1)", BigDecimal.valueOf(11, 1), 10, -2080146543},
        {"hash64(c0, c1)", BigDecimal.valueOf(10, 2), 10, -1652176568671252228L},
        {"hash32AsDouble(c0, c1)", BigDecimal.valueOf(10, 2), 10, -268274402},
        {"hash64AsDouble(c0, c1)", BigDecimal.valueOf(10, 1), 10, -1652176568671252228L},
      });
    } finally {
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        false));
    }
  }

  @Test
  public void testTimestampAddFunctions() throws Exception {
      testFunctions(new Object[][]{
        {"timestampaddSecond(c0, c1)", 30, ts("2000-05-01T10:20:34"),
          ts("2000-05-01T10:21:04")},
        {"timestampaddMinute(c0, c1)", -30L, ts("2000-05-01T10:20:34"), ts("2000-05-01T09:50:34")},
        {"timestampaddHour(c0, c1)", 20, ts("2000-05-01T10:20:34"), ts("2000-05-02T06:20:34")},
        {"timestampaddDay(c0, c1)", 10L, ts("2019-06-26T17:20:34"), ts("2019-07-06T17:20:34")},
        {"timestampaddWeek(c0, c1)", 4L, ts("2019-06-26T17:20:34"), ts("2019-07-24T17:20:34")},
        {"timestampaddMonth(c0, c1)", 7, ts("2019-06-26T17:20:34"), ts("2020-01-26T17:20:34")},
        {"timestampaddQuarter(c0, c1)", 4, ts("2019-06-26T17:20:34"), ts("2020-06-26T17:20:34")},
        {"timestampaddYear(c0, c1)", 1, ts("2019-06-26T17:20:34"), ts("2020-06-26T17:20:34")},
      });
  }

  @Test
  public void testDivFunction() throws Exception {
    testFunctions(new Object[][]{
      {"div(c0, c1)", 64, 3, 21},
      {"div(c0, c1)", 64L, 3L, 21L},
      {"div(c0, c1)", 2.5f, 1.2f, 2.0f},
      {"div(c0, c1)", 10.0d, 3.1d, 3.0d},
    });
  }

  @Test(expected = RuntimeException.class)
  public void testDivFunctionExpectDivByZeroError() throws Exception {
    try {
      testFunctions(new Object[][] {
        {"div(c0, c1)", 64, 0, 0},
      });
    } catch (RuntimeException re) {
      Assert.assertTrue(re.getCause().getCause().getMessage().contains("divide by zero error"));
      throw re;
    }
  }

  @Test
  public void testBitwiseFunctions() throws Exception {
    // bitwise AND
    testFunctions(new Object[][]{
      {"bitwise_and(c0, c1)", 0x0147D, 0x17159, 0x01059},
      {"bitwise_and(c0, c1)", 0xFFFFFFCC, 0x00000297, 0x00000284},
      {"bitwise_and(c0, c1)", 0x000, 0x285, 0x000},
      {"bitwise_and(c0, c1)", 0x563672F83L, 0x0D9FCF85BL, 0x041642803L},
      {"bitwise_and(c0, c1)", 0xFFFFFFFFFFDA8F6AL, 0xFFFFFFFFFFFF791CL, 0xFFFFFFFFFFDA0908L},
      {"bitwise_and(c0, c1)", 0x6A5B1L, 0x00000L, 0x00000L},
    });

    // bitwise OR
    testFunctions(new Object[][]{
      {"bitwise_or(c0, c1)", 0x0147D, 0x17159, 0x1757D},
      {"bitwise_or(c0, c1)", 0xFFFFFFCC, 0x00000297, 0xFFFFFFDF},
      {"bitwise_or(c0, c1)", 0x000, 0x285, 0x285},
      {"bitwise_or(c0, c1)", 0x563672F83L, 0x0D9FCF85BL, 0x5FBFFFFDBL},
      {"bitwise_or(c0, c1)", 0xFFFFFFFFFFDA8F6AL, 0xFFFFFFFFFFFF791CL, 0xFFFFFFFFFFFFFF7EL},
      {"bitwise_or(c0, c1)", 0x6A5B1L, 0x00000L, 0x6A5B1L},
    });

    // bitwise NOT
    testFunctions(new Object[][]{
      {"bitwise_not(c0)", 0x00017159, 0xFFFE8EA6},
      {"bitwise_not(c0)", 0xFFFFF226, 0x00000DD9},
      {"bitwise_not(c0)", 0x000000008BCAE9B4L, 0xFFFFFFFF7435164BL},
      {"bitwise_not(c0)", 0xFFFFFF966C8D7997L, 0x0000006993728668L},
      {"bitwise_not(c0)", 0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL},
    });
  }

  @Test
  public void testRound() throws Exception {
    // round for integers / longs
    testFunctions(new Object[][] {
      {"round(c0)", 21134, 21134},
      {"round(c0)", -132422, -132422},
      {"round(c0)", 3453562312L, 3453562312L},
      {"round(c0)", -23453462343L, -23453462343L},
      {"round(c0, c1)", 7589, -1, 7590},
      {"round(c0, c1)", 8532, -2, 8500},
      {"round(c0, c1)", -8579, -1, -8580},
      {"round(c0, c1)", -8612, -2, -8600},
      {"round(c0, c1)", -3874, -6, 0},
      {"round(c0, c1)", 758, 2, 758},
      {"round(c0, c1)", 3453562312L, -2, 3453562300L},
      {"round(c0, c1)", 3453562343L, -5, 3453600000L},
      {"round(c0, c1)", -345353425343L, 12, -345353425343L},
      {"round(c0, c1)", -23453462343L, -4, -23453460000L},
      {"round(c0, c1)", -23453462343L, -5, -23453500000L},
      {"round(c0, c1)", 345353425343L, -12, 0L}
    });

    // round for floats / doubles
    testFunctions(new Object[][] {
      {"round(c0)", 1234.245f, 1234f},
      {"round(c0)", -11.7892f, -12f},
      {"round(c0)", 1.4999999f, 1f},
      {"round(c0)", 1234.245d, 1234d},
      {"round(c0)", -11.7892d, -12d},
      {"round(c0)", 1.4999999d, 1d},
      {"round(c0, c1)", 1234.789f, 2, 1234.79f},
      {"round(c0, c1)", 1234.12345f, -3, 1000f},
      {"round(c0, c1)", -1234.4567f, 3, -1234.457f},
      {"round(c0, c1)", -1234.4567f, -3, -1000f},
      {"round(c0, c1)", 1234.4567f, 0, 1234f},
      {"round(c0, c1)", 1.5499999523162842f, 1, 1.5f},
      {"round(c0, c1)", (float)(1.55), 1, 1.5f},
      {"round(c0, c1)", (float)(9.134123), 2, 9.13f},
      {"round(c0, c1)", (float)(-1.923), 1, -1.9f},
      {"round(c0, c1)", 1234.789d, 2, 1234.79d},
      {"round(c0, c1)", 1234.12345d, -3, 1000d},
      {"round(c0, c1)", -1234.4567d, 3, -1234.457d},
      {"round(c0, c1)", -1234.4567d, -3, -1000d},
      {"round(c0, c1)", 1234.4567d, 0, 1234d},
    });
  }

  @Test
  public void testConcat() throws Exception {
    testFunctions(new Object[][]{
      { "concat(c0, c1)", "abc", "ABC", "abcABC"},
      { "concat(c0, c1)", "abc", NULL_VARCHAR, "abc"},
      { "concat(c0, c1)", "", "ABC", "ABC"},
      { "concat(c0, c1)", NULL_VARCHAR, NULL_VARCHAR, ""},
      { "concat(c0, c1, c2)", "abcd", "a", "bcd", "abcdabcd"},
      { "concat(c0, c1, c2)", NULL_VARCHAR, "pq", "ard", "pqard"},
      { "concat(c0, c1, c2)", "abcd", NULL_VARCHAR, "-a", "abcd-a"},
      { "concat(c0, c1, c2, c3)", "pqrs", "", "[n]abc", "y", "pqrs[n]abcy"},
      { "concat(c0, c1, c2, c3)", "", "pq", "ard", NULL_VARCHAR, "pqard"},
      { "concat(c0, c1, c2, c3, c4)", "", "npq", "ARD", "", "abc", "npqARDabc"},
      { "concat(c0, c1, c2, c3, c4)", "pqrs", NULL_VARCHAR, "nabc", "=y123",
        NULL_VARCHAR, "pqrsnabc=y123"},
      { "concat(c0, c1, c2, c3, c4, c5)", ":a", "npq", "ard", "", "abcn",
        "sdfgs", ":anpqardabcnsdfgs"},
      { "concat(c0, c1, c2, c3, c4, c5)", "PQRS", NULL_VARCHAR, "abc", "y",
        NULL_VARCHAR, "bcd", "PQRSabcybcd"},
      { "concat(c0, c1, c2, c3, c4, c5, c6)", ":o", "npq", "ard", "==", "abcn",
        "sdfgs", "", ":onpqard==abcnsdfgs"},
      { "concat(c0, c1, c2, c3, c4, c5, c6)", "pqrs", NULL_VARCHAR, "abc-n",
        "YABC", NULL_VARCHAR, "asdf", "jkl", "pqrsabc-nYABCasdfjkl"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7)", "", "##npq", "ard",
        "", "abcn", "sdf*gs", "", "", "##npqardabcnsdf*gs"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7)", NULL_VARCHAR, "pqrs",
        "abc", "y", NULL_VARCHAR, "asdf", "jkl", "$$", "pqrsabcyasdfjkl$$"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7, c8)", "abcd", "", "pq", "ard",
        "", "abcn", "sdfgs", "", "qwert|n", "abcdpqardabcnsdfgsqwert|n"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7, c8)", NULL_VARCHAR, "abcd", "npq",
        "sdfgs", "", "uwu", "wfw", "ABC", NULL_VARCHAR, "abcdnpqsdfgsuwuwfwABC"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)", "abcd", "", "pq", "ard", "",
        "ABCN", "sdfgs", "", "qwert|n", "a", "abcdpqardABCNsdfgsqwert|na"},
      { "concat(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)", NULL_VARCHAR, "abcd", "npq",
        "sdfgs", "", "uwu", "wfw", "ABC", NULL_VARCHAR, "1234", "abcdnpqsdfgsuwuwfwABC1234"},
    });
  }

  @Test
  public void testSplitPart() throws Exception {
    testFunctions(new Object[][]{
      { "split_part(c0, c1, c2)", "abc~@~def~@~ghi", "~@~", 1, "abc"},
      { "split_part(c0, c1, c2)", "abc~@~def~@~ghi", "~@~", 2, "def"},
      { "split_part(c0, c1, c2)", "A,B,C", ",", 3, "C"},
      { "split_part(c0, c1, c2)", "A,B,C", ",", 4, ""},
      { "split_part(c0, c1, c2)", "123|456|789", "|", 1, "123"},
      { "split_part(c0, c1, c2)", "abc\\u1111dremio\\u1111ghi", "\\u1111", 2, "dremio"},
      { "split_part(c0, c1, c2)", "a,b,c", " ", 1, "a,b,c"},
      { "split_part(c0, c1, c2)", "a,b,c", " ", 2, ""},
    });
  }

  @Test(expected = RuntimeException.class)
  public void testSplitPartZeroIndexError() throws Exception {
    try {
      testFunctions(new Object[][] {
        { "split_part(c0, c1, c2)", "A,B,C", ",", 0, ""},
      });
    } catch (RuntimeException re) {
      Assert.assertTrue(re.getCause().getCause().getMessage().contains("Index in split_part must be positive, value provided was 0"));
      throw re;
    }
  }

  @Test
  public void testBinaryString() throws Exception {
    testFunctions(new Object[][]{
      {"convert_from(binary_string(c0), 'UTF8')", "\\x41\\x42\\x43", "ABC"},
      {"convert_from(binary_string(c0), 'UTF8')", "\\x41\\x20\\x42\\x20\\x43", "A B C"},
      {"convert_from(binary_string(c0), 'UTF8')", "\\x41", "A"},
      {"convert_from(binary_string(c0), 'UTF8')", "TestString", "TestString"},
      {"convert_from(binary_string(c0), 'UTF8')", "T", "T"},
      {"convert_from(binary_string(c0), 'UTF8')", "\\x4f\\x4D", "OM"},
    });
  }

  @Test
  public void testConcatOperator() throws Exception {
    testFunctions(new Object[][]{
      { "concatOperator(c0, c1)", "abcd", "ABCD", "abcdABCD"},
      { "concatOperator(c0, c1)", "", "ABCD", "ABCD"},
      { "concatOperator(c0, c1)", NULL_VARCHAR, "abc", NULL_VARCHAR},
      { "concatOperator(c0, c1)", "", "", ""},
    });
  }

  @Test
  public void testFlippedCodeGenerator() throws Exception {
    ArrowType strType = new ArrowType.Utf8();
    ArrowType bigIntType = new ArrowType.Int(64, true);
    FunctionSignature substrFn = new FunctionSignature("substr", strType, Lists.newArrayList(strType, bigIntType, bigIntType));
    GandivaPushdownSieveHelper helper = new GandivaPushdownSieveHelper();
    try {
      // hide substr function if implemented in Gandiva
      helper.addFunctionToHide(substrFn);
      // enable decimal
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY,
        true));
      // enable decimal v2
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        true));
      // enabled mixed mode execution
      testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceMixed
      ));
      // increase the threshold for flipping the code generator
      testContext.getOptions().setOption(OptionValue.createDouble(
        OptionValue.OptionType.SYSTEM,
      ExecConstants.WORK_THRESHOLD_FOR_SPLIT_KEY,
        10.0));
      testFunctionsCompiledOnly(new Object[][]{
        {"c1 = 'CA' or c1 = 'WA' or c1 = 'GA' or c2 > 500 or substr(c0,1,4) = '8566' or substr(c0, 1, 4) = '8619' or substr(c0, 1, 4) = '8827' or substr(c0, 1, 4) = '8340' or substr(c0, 1, 4) = '1111' or substr(c0, 1, 4) = '1234' or substr(c0, 1, 4) = 2222",
          "3031", "TN", BigDecimal.valueOf(100, 2), false}
      });
    } finally {
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY,
        PlannerSettings.ENABLE_DECIMAL_DATA_TYPE.getDefault().getBoolVal()));
      testContext.getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        PlannerSettings.ENABLE_DECIMAL_V2_KEY,
        PlannerSettings.ENABLE_DECIMAL_V2.getDefault().getBoolVal()));
      testContext.getOptions().setOption(OptionValue.createDouble(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.WORK_THRESHOLD_FOR_SPLIT_KEY,
        ExecConstants.WORK_THRESHOLD_FOR_SPLIT.getDefault().getFloatVal()));
      testContext.getOptions().setOption(OptionValue.createString(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.QUERY_EXEC_OPTION_KEY,
        execPreferenceGandivaOnly
      ));
      helper.removeFunctionToHide(substrFn);
    }
  }

  @Test
  public void testTSToVarchar() throws Exception {
    testFunctions(new Object[][] {
      {"castVARCHAR(c0, c1)", ts("2019-06-26T17:20:34"), 23L, "2019-06-26 17:20:34.000"},
      {"castVARCHAR(c0, c1)", ts("2019-06-26T17:20:34"), 30L, "2019-06-26 17:20:34.000"},
      {"castVARCHAR(c0, c1)", ts("2019-06-26T17:20:34"), 22L, "2019-06-26 17:20:34.00"},
      {"castVARCHAR(c0, c1)", ts("2019-06-26T17:20:34"), 0L, ""},
    });
  }

  @Test
  public void testIntervalDayToBigInt() throws Exception {
    testFunctions(new Object[][]{
        {"cast(c0 as BIGINT)", interval_day(10, 1), 864000001L},
        {"cast(c0 as BIGINT)", interval_day(1, 1), 86400001L},
        {"cast(c0 as BIGINT)", interval_day(-1, 0), -86400000L},
        {"cast(c0 as BIGINT)", interval_day(-1, -1), -86400001L}
    });
  }

  @Test
  public void testCastBigIntToTimestamp() throws Exception {
    testFunctions(new Object[][]{
      {"castTIMESTAMP(c0)", 15020L, ts("1970-01-01T00:00:15.020")},
      {"castTIMESTAMP(c0)", 5*86400000L, ts("1970-01-06T00:00:00")},
      {"castTIMESTAMP(c0)", -20800L, ts("1969-12-31T23:59:39.200")},
      {"castTIMESTAMP(c0)", -12*86400000L, ts("1969-12-20T00:00:00")}
    });
  }

  @Test(expected = RuntimeException.class)
  public void testTSToVarcharNegativeLenError() throws Exception {
    try {
      testFunctions(new Object[][] {
        {"castVARCHAR(c0, c1)", ts("2019-06-26T17:20:34"), -2L, ""},
      });
    } catch (RuntimeException re) {
      Assert.assertTrue(re.getCause().getCause().getMessage().contains("Length of output string cannot be negative"));
      throw re;
    }
  }

  @Test
  public void testRegistryIsCaseInsensitive() throws Exception {
    testFunctions(new Object[][]{
      {"extractday(c0)", ts("1970-01-02T10:20:33"), 2l},
      {"extractmonth(c0)", ts("1970-01-02T10:20:33"), 1l},
      {"extractday(c0)", date("1970-01-02"), 2l},
      {"extractmonth(c0)", date("1970-01-02"), 1l}
    });
  }

  @Test
  public void testDateDiffFunctions() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"extractday(date_sub(c0, 10))", date("1970-01-12"), 2l},
      {"extractday(date_diff(c0, 10))", ts("1970-01-12T10:20:33"), 2l},
      {"extractday(subtract(c0, 10))", ts("1970-01-12T10:20:33"), 2l},
    });
  }

  @Test
  public void testDateTruncFunctions() {
    testFunctions(new Object[][]{
      {"extractday(date_trunc_Month(c0))", date("2020-11-12"), 1L},
      {"extractmonth(date_trunc_Month(c0))", date("2020-11-12"), 11L},
      {"extractyear(date_trunc_Month(c0))", date("2020-11-12"), 2020L},

      {"extractday(date_trunc_Year(c0))", date("2020-11-12"), 1L},
      {"extractmonth(date_trunc_Year(c0))", date("2020-11-12"), 1L},
      {"extractyear(date_trunc_Year(c0))", date("2020-11-12"), 2020L},
    });
  }

  @Test
  public void testCastTimestampToDate() throws Exception {
    testFunctions(new Object[][]{
      {"cast(castDATE(c0) as TIMESTAMP)", ts("1970-01-12T10:20:33"), ts("1970-01-12T00:00:00")},
      {"cast(to_date(c0) as TIMESTAMP)", ts("1970-01-12T10:20:33"), ts("1970-01-12T00:00:00")},
      {"cast(cast(c0 as DATE) as TIMESTAMP)", ts("1970-01-12T10:20:33"), ts("1970-01-12T00:00:00")},
      {"extractYear(to_date(c0))", ts("1970-01-12T10:20:33"), 1970L},
    });
  }

  @Test
  public void testCastIntFromString() throws Exception {
    testFunctions(new Object[][]{
      {"castINT(c0)", "56", 56},
      {"cast(c0 as INT)", "-2", -2},
      {"castBIGINT(c0)", "56", 56l},
      {"cast(c0 as BIGINT)", "-2", -2l},
    });
  }

  @Test
  public void testCastFloatFromString() throws Exception {
    testFunctions(new Object[][]{
      {"castFLOAT4(c0)", "3.4", 3.4f},
      {"castFLOAT4(c0)", "-2.1", -2.1f},
      {"castFLOAT8(c0)", "3.4", 3.4},
      {"castFLOAT8(c0)", "-2.1", -2.1},
    });
  }
}
