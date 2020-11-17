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
package com.dremio.exec.expr;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.dremio.common.expression.InExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.util.AssertionUtil;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;
import com.google.common.collect.ObjectArrays;

/**
 * Test that multi or optimization is working
 */
public class TestMultiOrOptimization extends BaseTestFunction {

  @Test
  public void dateNoMatch(){
    check(false, 1, "booleanOr(c0 = cast(1l as DATE), c0 = cast(2l as DATE), c0 = cast(3l as DATE), c0 = cast(4l as DATE), c0 = cast(5l as DATE), c0 = cast(6l as DATE), c0 = cast(7l as DATE), c0 = cast(8l as DATE), c0 = cast(9l as DATE))"
        , new LocalDate(10, DateTimeZone.UTC));
  }

  @Test
  public void dateMatch(){
    // match needs to be on a day boundary. 1521849600000 is Mar 28, 2018 at Midnight.
    check(true, 1, "booleanOr(c0 = cast(1l as DATE), c0 = cast(1521849600000l as DATE), c0 = cast(3l as DATE), c0 = cast(4l as DATE), c0 = cast(5l as DATE), c0 = cast(6l as DATE), c0 = cast(7l as DATE), c0 = cast(8l as DATE), c0 = cast(9l as DATE))"
        , new LocalDate(1521849600000l, DateTimeZone.UTC));
  }

  @Test
  public void tsNoMatch(){
    check(false, 1, "booleanOr(c0 = cast(1l as TIMESTAMP), c0 = cast(2l as TIMESTAMP), c0 = cast(3l as TIMESTAMP), c0 = cast(4l as TIMESTAMP), c0 = cast(5l as TIMESTAMP), c0 = cast(6l as TIMESTAMP), c0 = cast(7l as TIMESTAMP), c0 = cast(8l as TIMESTAMP), c0 = cast(9l as TIMESTAMP))"
        , new LocalDateTime(10, DateTimeZone.UTC));
  }

  @Test
  public void tsMatch(){
    check(true, 1, "booleanOr(c0 = cast(1l as TIMESTAMP), c0 = cast(2l as TIMESTAMP), c0 = cast(3l as TIMESTAMP), c0 = cast(4l as TIMESTAMP), c0 = cast(5l as TIMESTAMP), c0 = cast(6l as TIMESTAMP), c0 = cast(7l as TIMESTAMP), c0 = cast(8l as TIMESTAMP), c0 = cast(9l as TIMESTAMP))"
        , new LocalDateTime(7, DateTimeZone.UTC));
  }

  @Test
  public void timeNoMatch(){
    check(false, 1, "booleanOr(c0 = cast(1l as TIME), c0 = cast(2l as TIME), c0 = cast(3l as TIME), c0 = cast(4l as TIME), c0 = cast(5l as TIME), c0 = cast(6l as TIME), c0 = cast(7l as TIME), c0 = cast(8l as TIME), c0 = cast(9l as TIME))"
        , new LocalTime(10, DateTimeZone.UTC));
  }

  @Test
  public void timeMatch(){
    check(true, 1, "booleanOr(c0 = cast(1l as TIME), c0 = cast(2l as TIME), c0 = cast(3l as TIME), c0 = cast(4l as TIME), c0 = cast(5l as TIME), c0 = cast(6l as TIME), c0 = cast(7l as TIME), c0 = cast(8l as TIME), c0 = cast(9l as TIME))"
        , new LocalTime(7, DateTimeZone.UTC));
  }

  @Test
  public void intComplexNoMatch(){
    check(false, 1, "booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 10, c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 = 8i, c0 = 9i)", 10);
  }

  @Test
  public void intComplexMatch(){
    check(true, 1, "booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 14, c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 = 8i, c0 = 9i)", 9);
  }

  @Test
  public void bigIntSimpleNoMatch(){
    check(false, 1, "booleanOr(c0 = 1l, c0 = 2l, c0 = 3l, c0 = 4l, c0 = 5l, c0 = 6l, c0 = 7l, c0 = 8l, c0 = 9l)", 10l);
  }

  @Test
  public void bigIntComplexNoMatch(){
    check(false, 1, "booleanOr(c0 = 1l, c0 = 2l, c0 = 3l, c0 != 10l, c0 = 4l, c0 = 5l, c0 = 6l, c0 = 7l, c0 = 8l, c0 = 9l)", 10l);
  }

  @Test
  public void bigIntComplexMatch(){
    check(true, 1, "booleanOr(c0 = 1l, c0 = 2l, c0 = 3l, c0 != 14, c0 = 4l, c0 = 5l, c0 = 6l, c0 = 7l, c0 = 8l, c0 = 9l)", 9l);
  }

  @Test
  public void varCharSimpleNoMatch(){
    check(false, 1, "booleanOr(c0 = 'myString1l', c0 = 'myString2l', c0 = 'myString3l', c0 = 'myString4l', c0 = 'myString5l', c0 = 'myString6l', c0 = 'myString7l', c0 = 'myString8l', c0 = 'myString9l')", "myString10l");
  }

  @Test
  public void a() {
    check(false, 0, "c0 != 'myString10l'", "myString10l");
  }

  @Test
  public void varCharComplexNoMatch(){
    check(false, 1, "booleanOr(c0 = 'myString1l', c0 = 'myString2l', c0 = 'myString3l', c0 != 'myString10l', c0 = 'myString4l', c0 = 'myString5l', c0 = 'myString6l', c0 = 'myString7l', c0 = 'myString8l', c0 = 'myString9l')", "myString10l");
  }

  @Test
  public void varCharComplexMatch(){
    check(true, 1, "booleanOr(c0 = 'myString1l', c0 = 'myString2l', c0 = 'myString3l', c0 != 'myStringx', c0 = 'myString4l', c0 = 'myString5l', c0 = 'myString6l', c0 = 'myString7l', c0 = 'myString8l', c0 = 'myString9l')", "myString9l");
  }

  @Test
  public void multiMatch(){
    check(true, 2, "booleanAnd("
        + "booleanOr(c1 = 1i, c1 = 2i, c1 = 3i, c1 != 14, c1 = 4i, c1 = 5i, c1 = 6i, c1 = 7i, c1 = 8i, c1 = 9i), "
        + "booleanOr(c0 = 'myString1l', c0 = 'myString2l', c0 = 'myString3l', c0 != 'myStringx', c0 = 'myString4l', c0 = 'myString5l', c0 = 'myString6l', c0 = 'myString7l', c0 = 'myString8l', c0 = 'myString9l') "
        + ")",
        "myString9l",
        9);
  }

  @Test
  public void nullInList() {
    check(true, 1, "booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 14, c0 = cast(__$INTERNAL_NULL$__ as int), c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 = 8i, c0 = 9i)", 9);
  }

  @Test
  public void nullValueAndInList() {
    check(Fixtures.NULL_BOOLEAN, 1, "booleanOr(c0 = 1i, c0 = 2i, c0 = 3i, c0 != 14, c0 = cast(__$INTERNAL_NULL$__ as int), c0 = 4i, c0 = 5i, c0 = 6i, c0 = 7i, c0 = 8i, c0 = 9i)", Fixtures.NULL_INT);
  }

  @Test
  public void complexEvalExpression() {
    check(true, 1, "booleanOr((c0 + c1) = 1i, (c0 + c1) = 2i, (c0 + c1) = 3i, (c0 + c1) != 14, (c0 + c1) = cast(__$INTERNAL_NULL$__ as int), (c0 + c1) = 4i, (c0 + c1) = 5i, (c0 + c1) = 6i, (c0 + c1) = 7i, (c0 + c1) = 8i, (c0 + c1) = 9i)", 4, 5);
  }


  /**
   * Verify that the expression provided returns the expected result whether we use in optimization or not.
   * @param result The expected result.
   * @param count Number of expected in constructions.
   * @param expr The expression (internal expression language, not sql)
   * @param objects Field values.
   */
  private void check(Object result, int count, String expr, Object...objects) {
    Assume.assumeTrue(AssertionUtil.ASSERT_ENABLED);

    // now test the base case
    try {
      testContext.getOptions().setOption(OptionValue.createBoolean(OptionType.SYSTEM, ExecConstants.FAST_OR_ENABLE.getOptionName(), false));
      testFunction(expr, ObjectArrays.concat(objects, result));
    } catch(Throwable t) {
      t.addSuppressed(new Exception("Failure during unoptimized operation."));
      throw t;
    } finally {
      testContext.getOptions().deleteOption(ExecConstants.FAST_OR_ENABLE.getOptionName(), OptionType.SYSTEM);
    }

    // test the optimized case
    try {
      // setting the codegen option to use Java to ensure that the expected code path is exercised
      testContext.getOptions().setOption(OptionValue.createString(OptionType.SYSTEM, ExecConstants.QUERY_EXEC_OPTION.getOptionName(), SupportedEngines.CodeGenOption.Java.name()));
      InExpression.COUNT.set(0);
      try {
        testFunction(expr, ObjectArrays.concat(objects, result));
      } catch(Throwable t) {
        t.addSuppressed(new Exception("Failure during optimized operation."));
        throw t;
      }

      Assert.assertEquals("Expected in expression invocation count.", count, InExpression.COUNT.get());
    } finally {
      InExpression.COUNT.set(0);
      testContext.getOptions().deleteOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName(), OptionType.SYSTEM);
    }
  }
}
