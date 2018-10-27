/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.sabot.Fixtures.*;

import java.util.Arrays;

import com.dremio.common.expression.EvaluationType;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionValue;
import org.joda.time.LocalDate;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.physical.config.Project;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.op.project.ProjectOperator;

/*
 * This class tests native (LLVM) implementation of functions.
 */
@Ignore
public class TestNativeFunctions extends BaseTestFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestNativeFunctions.class);
  private static String execPreferenceGandivaOnly = EvaluationType.CodeGenOption.GandivaOnly.toString();
  private static String execPreferenceMixed = EvaluationType.CodeGenOption.Gandiva.toString();
  private static String execPreferenceJava = EvaluationType.CodeGenOption.Java.toString();


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
  public void testCastDate() throws Exception {
    testFunctions(new Object[][]{
      {"extractYear(castDATE(c0))","79:10:10", 1979l}
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
      {"hash32(c0)", 10, -561650695},
    });
  }

  @Test
  public void testIsNull() throws Exception {
    testFunctions(new Object[][]{
      {"isnull(c0)", 10, false},
      {"isnull(c0)", 10L, false},
      {"isnull(c0)", 10.0F, false},
      {"isnull(c0)", 10.0D, false},
      {"isnull(c0)", NULL_INT, true},
      {"isnull(c0)", NULL_BIGINT, true},
      {"isnull(c0)", NULL_FLOAT, true},
      {"isnull(c0)", NULL_DOUBLE, true},
    });
  }

  @Test
  public void testIsNotNull() throws Exception {
    testFunctions(new Object[][]{
      {"isnotnull(c0)", 10, true},
      {"isnotnull(c0)", 10L, true},
      {"isnotnull(c0)", 10.0F, true},
      {"isnotnull(c0)", 10.0D, true},
      {"isnotnull(c0)", NULL_INT, false},
      {"isnotnull(c0)", NULL_BIGINT, false},
      {"isnotnull(c0)", NULL_FLOAT, false},
      {"isnotnull(c0)", NULL_DOUBLE, false},
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
    Project p = new Project(Arrays.asList(n(strExpr, "out")), null);
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
}
