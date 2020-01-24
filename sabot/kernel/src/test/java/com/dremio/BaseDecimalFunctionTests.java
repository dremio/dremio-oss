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
package com.dremio;

import static com.dremio.sabot.Fixtures.NULL_DECIMAL;

import java.math.BigDecimal;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;

/**
 * decimal tests for functions implemented in Gandiva.
 */
public abstract class BaseDecimalFunctionTests extends BaseTestFunction {

  protected String execPreference;
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      execPreference));
    testContext.getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      PlannerSettings.ENABLE_DECIMAL_V2_KEY,
      true));
  }

  @After
  public void tearDown() {
    testContext.getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      PlannerSettings.ENABLE_DECIMAL_V2_KEY,
      PlannerSettings.ENABLE_DECIMAL_V2.getDefault().getBoolVal()));

    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      SupportedEngines.CodeGenOption.DEFAULT.toString()));
  }

  @Test
  public void testDecimalAdd() throws Exception {
    testFunctions(new Object[][]{
      {"c0 + castDECIMAL(2.45d, 3l, 2l)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45).add(BigDecimal.valueOf(7.62))},

      {"c0 + c1",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(2.45).add(BigDecimal.valueOf(7.62))}
    });
  }

  @Test
  public void testDecimalSubtract() throws Exception {
    testFunctions(new Object[][]{
      {"c0 - castDECIMAL(2.45d, 3l, 2l)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(7.62).subtract(BigDecimal.valueOf(2.45))},

      {"c0 - c1",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(7.62).subtract(BigDecimal.valueOf(2.45))}
    });
  }

  @Test
  public void testDecimalMultiply() throws Exception {
    testFunctions(new Object[][]{
      {"c0 * castDECIMAL(2.45d, 3l, 2l)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(7.62).multiply(BigDecimal.valueOf(2.45))},

      {"c0 * c1",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(7.62).multiply(BigDecimal.valueOf(2.45))}
    });
  }

  @Test
  public void testDecimalDivide() throws Exception {
    testFunctions(new Object[][]{
      {"c0 / castDECIMAL(2.45d, 3l, 2l)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(7.62).divide(BigDecimal.valueOf(2.45), 6, BigDecimal.ROUND_HALF_UP)},

      {"c0 / c1",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(7.62).divide(BigDecimal.valueOf(2.45), 6, BigDecimal.ROUND_HALF_UP)}
    });
  }

  @Test
  public void testDecimalMod() throws Exception {
    testFunctions(new Object[][]{
      {"mod(c0, castDECIMAL(2.45d, 38l, 2l))",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(7.62).divideAndRemainder(BigDecimal.valueOf(2.45))[1]},

      {"mod(c0, c1)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(7.62).divideAndRemainder(BigDecimal.valueOf(2.45))[1]},
    });
  }

  @Test
  public void testNestedGandivaOnlyFunction() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"castFLOAT8(c0) + castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l)) + castFLOAT8(c0)",
        BigDecimal.valueOf(7.62),
        18.350204D}
    });
  }

  @Test
  public void testNestedGandivaOnlyIfCondition() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      // if condition that is an arg to a function has a gandiva only function in
      // then clause.
      {"castFLOAT8(case when c0 > castDECIMAL('2.45', 3l, 2l) " +
        " then c0 / castDECIMAL('2.45', 3l,2l) " +
        " else castDECIMAL('3.110204', 38l, 6l) end)",
        BigDecimal.valueOf(7.62),
        3.110204D},

      // if condition that is an arg to a function has a gandiva only function in
      // then and else clause.
      {"castFLOAT8(case when c0 < castDECIMAL('2.45', 3l, 2l) " +
        " then c0 / castDECIMAL('2.45', 3l,2l) " +
        " else c0 / castDECIMAL('2.45', 3l,2l) end)",
        BigDecimal.valueOf(7.62),
        3.110204D},

      // if condition that is an arg to a function has a gandiva only function in
      // condition, then and else clause.
      {"castFLOAT8(case when c0 < (c0 / castDECIMAL('2.45', 3l,2l)) " +
        " then c0 / castDECIMAL('2.45', 3l,2l) " +
        " else c0 / castDECIMAL('2.45', 3l,2l) end)",
        BigDecimal.valueOf(7.62),
        3.110204D},
    });
  }

  @Test
  public void testDoubleNestedGandivaOnlyIfCondition() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      // double nested if conditions with gandiva only functions
      {"castFLOAT8(case when c0 > castDECIMAL('2.45', 3l, 2l) " +
                     " then (case when c0 > castDECIMAL('2.45', 3l, 2l) " +
                                 " then c0 / castDECIMAL('2.45', 3l,2l) " +
                                 " else castDECIMAL('3.110204', 38l, 6l) end)" +
                     " else castDECIMAL('3.110204', 38l, 6l) end)",
        BigDecimal.valueOf(7.62),
        3.110204D},
      {"castFLOAT8(case when c0 > castDECIMAL('2.45', 3l, 2l) " +
                    " then (case when c0 > castDECIMAL('2.45', 3l, 2l) " +
                                 " then c0 / castDECIMAL('2.45', 3l,2l) " +
                                 " else castDECIMAL('3.110204', 38l, 6l) end)" +
                    " else c0 / castDECIMAL('2.45', 3l,2l) end)",
        BigDecimal.valueOf(7.62),
        3.110204D}
    });
  }

  @Test
  public void testMixedBooleanAnd() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      // condition and then have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) AND  (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " else castFLOAT8(castDECIMAL('3.110204', 38l, 6l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},

      // condition and else have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) AND  (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(castDECIMAL('3.110204', 38l, 6l))" +
        " else castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},

      // condition, then and else have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) AND  (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " else castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},
    });
  }

  @Test
  public void testMixedBooleanOr() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      // condition and then have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) OR (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " else castFLOAT8(castDECIMAL('3.110204', 38l, 6l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},

      // condition and else have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) OR (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(castDECIMAL('3.110204', 38l, 6l))" +
        " else castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},

      // condition, then and else have the gandiva only functions.
      {"case when (c0 > castDECIMAL('2.45', 3l, 2l) OR (castFLOAT8(c0 / castDECIMAL('2.45', 3l,2l)) > c1))" +
        " then castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " else castFLOAT8(c0 / castDECIMAL('2.45', 3l, 2l))" +
        " end",
        BigDecimal.valueOf(7.62),
        2.10D,
        3.110204D},
    });
  }

  @Test
  public void testDecimalCompare() throws Exception {
    testFunctions(
        new Object[][] {
          {"c0 == c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(7620, 3), true},
          {"c0 == c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(7620, 3), true},
          {"c0 == c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(834, 3), false},
          {"c0 <  c1", BigDecimal.valueOf(762, 2), BigDecimal.valueOf(7620, 3), false},
          {"c0 < c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(8340, 3), true},
          {"c0 <= c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(7620, 3), true},
          {"c0 <= c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(8340, 3), true},
          {"c0 > c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(7620, 3), false},
          {"c0 > c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(8340, 3), false},
          {"c0 >= c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(7620, 3), true},
          {"c0 >= c1", BigDecimal.valueOf(7.62), BigDecimal.valueOf(8340, 3), false},
        });
  }

  @Test
  public void testAbs() throws Exception {
    testFunctions(
      new Object[][] {
        {"abs(c0)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.23)},
        {"abs(c0)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1.58)},
        {"abs(c0)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(1.23)},
        {"abs(c0)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(1.58)}
      });
  }

  @Test
  public void testCeil() throws Exception {
    testFunctions(
      new Object[][] {
        {"ceil(c0)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(2)},
        {"ceil(c0)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(2)},
        {"ceil(c0)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1)},
        {"ceil(c0)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1)}
      });
  }

  @Test
  public void testFloor() throws Exception {
    testFunctions(
      new Object[][] {
        {"floor(c0)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1)},
        {"floor(c0)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1)},
        {"floor(c0)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-2)},
        {"floor(c0)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-2)}
      });
  }

  @Test
  public void testRound() throws Exception {
    testFunctions(
      new Object[][] {
        {"round(c0)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1)},
        {"round(c0)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(2)},
        {"round(c0)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1)},
        {"round(c0)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-2)},

        {"round(c0, 1)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.2)},
        {"round(c0, 1)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1.6)},
        {"round(c0, 1)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1.2)},
        {"round(c0, 1)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1.6)},

        {"round(c0, -1)", BigDecimal.valueOf(112.3), BigDecimal.valueOf(110)},
        {"round(c0, -1)", BigDecimal.valueOf(-112.3), BigDecimal.valueOf(-110)},
      });
  }

  @Test
  public void testTruncate() throws Exception {
    testFunctions(
      new Object[][] {
        {"truncate(c0)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1)},
        {"truncate(c0)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1)},
        {"truncate(c0)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1)},
        {"truncate(c0)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1)},

        {"truncate(c0, 1)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.2)},
        {"truncate(c0, 1)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1.5)},
        {"truncate(c0, 1)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1.2)},
        {"truncate(c0, 1)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1.5)},

        {"truncate(c0, -1)", BigDecimal.valueOf(112.3), BigDecimal.valueOf(110)},
        {"truncate(c0, -1)", BigDecimal.valueOf(-112.3), BigDecimal.valueOf(-110)},
      });
  }

  @Test
  public void testCastDecimalToDecimal() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        // no-op casts
        {"castDECIMAL(c0, 38l, 2l)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.23)},
        {"castDECIMAL(c0, 38l, 2l)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1.23)},

        // scale-down
        {"castDECIMAL(c0, 38l, 1l)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.2)},
        {"castDECIMAL(c0, 38l, 1l)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1.2)},
        {"castDECIMAL(c0, 38l, 1l)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1.6)},
        {"castDECIMAL(c0, 38l, 1l)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1.6)},

        // scale-up
        {"castDECIMAL(c0, 38l, 3l)", BigDecimal.valueOf(1.23), BigDecimal.valueOf(1230, 3)},
        {"castDECIMAL(c0, 38l, 3l)", BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1230, 3)},
        {"castDECIMAL(c0, 38l, 3l)", BigDecimal.valueOf(1.58), BigDecimal.valueOf(1580, 3)},
        {"castDECIMAL(c0, 38l, 3l)", BigDecimal.valueOf(-1.58), BigDecimal.valueOf(-1580, 3)},
      });
  }

  @Test
  public void testCastIntToDecimal() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"castDECIMAL(c0, 38l, 2l)", 123, BigDecimal.valueOf(12300, 2)},
        {"castDECIMAL(c0, 38l, 2l)", 158, BigDecimal.valueOf(15800, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -123, BigDecimal.valueOf(-12300, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -158, BigDecimal.valueOf(-15800, 2)}
      });
  }

  @Test
  public void testCastLongToDecimal() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"castDECIMAL(c0, 38l, 2l)", 123l, BigDecimal.valueOf(12300, 2)},
        {"castDECIMAL(c0, 38l, 2l)", 158l, BigDecimal.valueOf(15800, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -123l, BigDecimal.valueOf(-12300, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -158l, BigDecimal.valueOf(-15800, 2)}
     });
  }

  @Test
  public void testCastFloatToDecimal() throws Exception {
    testFunctionsCompiledOnly(
        new Object[][] {
          // cast with same scale
          {"castDECIMAL(c0, 38l, 2l)", 1.23, BigDecimal.valueOf(123, 2)},
          {"castDECIMAL(c0, 38l, 2l)", 1.58, BigDecimal.valueOf(158, 2)},
          {"castDECIMAL(c0, 38l, 2l)", -1.23, BigDecimal.valueOf(-123, 2)},
          {"castDECIMAL(c0, 38l, 2l)", -1.58, BigDecimal.valueOf(-158, 2)},
        });
  }

  @Test
  public void testCastDoubleToDecimal() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        // cast with same scale
        {"castDECIMAL(c0, 38l, 2l)", 1.23D, BigDecimal.valueOf(123, 2)},
        {"castDECIMAL(c0, 38l, 2l)", 1.58D, BigDecimal.valueOf(158, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -1.23D, BigDecimal.valueOf(-123, 2)},
        {"castDECIMAL(c0, 38l, 2l)", -1.58D, BigDecimal.valueOf(-158, 2)},

        // cast with scale down
        {"castDECIMAL(c0, 38l, 1l)", 1.23D, BigDecimal.valueOf(12, 1)},
        {"castDECIMAL(c0, 38l, 1l)", 1.58D, BigDecimal.valueOf(16, 1)},
        {"castDECIMAL(c0, 38l, 1l)", -1.23D, BigDecimal.valueOf(-12,1)},
        {"castDECIMAL(c0, 38l, 1l)", -1.58D, BigDecimal.valueOf(-16,1)},

        // cast with scale up
        {"castDECIMAL(c0, 38l, 3l)", 1.23D, BigDecimal.valueOf(1230, 3)},
        {"castDECIMAL(c0, 38l, 3l)", 1.58D, BigDecimal.valueOf(1580, 3)},
        {"castDECIMAL(c0, 38l, 3l)", -1.23D, BigDecimal.valueOf(-1230,3)},
        {"castDECIMAL(c0, 38l, 3l)", -1.58D, BigDecimal.valueOf(-1580,3)},

        // rounding
        {"castDECIMAL(c0, 38l, 0l)", 1.1534D, BigDecimal.valueOf(1,0)},
        {"castDECIMAL(c0, 38l, 0l)", -1.1534D, BigDecimal.valueOf(-1,0)},
        {"castDECIMAL(c0, 38l, 0l)", 1.534D, BigDecimal.valueOf(2,0)},
        {"castDECIMAL(c0, 38l, 0l)", -1.534D, BigDecimal.valueOf(-2,0)},
      });
  }

  @Test
  public void testCastDecimalToLong() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"castBIGINT(c0)", BigDecimal.valueOf(123, 2), 1l},
        {"castBIGINT(c0)", BigDecimal.valueOf(-123, 2), -1l},
        {"castBIGINT(c0)", BigDecimal.valueOf(158, 2), 2l},
        {"castBIGINT(c0)", BigDecimal.valueOf(-158, 2), -2l},
      });
  }

  @Test
  public void testCastDecimalToDouble() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"castFLOAT8(c0)", BigDecimal.valueOf(123, 2), 1.23D},
        {"castFLOAT8(c0)", BigDecimal.valueOf(158, 2), 1.58D},
        {"castFLOAT8(c0)", BigDecimal.valueOf(-123, 2), -1.23D},
        {"castFLOAT8(c0)", BigDecimal.valueOf(-158, 2), -1.58D},
      });
  }

  @Test
  public void testDecimalDoubleFunctionsBasic() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"power(c0,2)", BigDecimal.valueOf(123, 2), BigDecimal.valueOf(123,2 ).pow(2).doubleValue()},
        {"power(c0,0.5)", BigDecimal.valueOf(64, 2), BigDecimal.valueOf(0.8).doubleValue()},
        {"power(c0,0.5)", BigDecimal.valueOf(64, 2), BigDecimal.valueOf(0.8).doubleValue()},
        {"cbrt(c0)", BigDecimal.valueOf(64, 0), BigDecimal.valueOf(4.0).doubleValue()},
        {"log(c0)", BigDecimal.valueOf(64, 1), BigDecimal.valueOf(1.85629799036563).doubleValue()},
      });
  }

  @Test
  public void testDecimalMixedArithmetic() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"c0 + c1", BigDecimal.valueOf(123, 2), 2d, BigDecimal.valueOf(1.23).add(BigDecimal
          .valueOf(2)).doubleValue()},
        {"c0 + c1", BigDecimal.valueOf(123, 2), 2f, BigDecimal.valueOf(1.23).add(BigDecimal
          .valueOf(2f)).doubleValue()},
        {"c0 * c1", BigDecimal.valueOf(123, 2), 2f, BigDecimal.valueOf(1.23).multiply(BigDecimal
        .valueOf(2f)).doubleValue()},
        {"c0 - c1", BigDecimal.valueOf(123, 2), 2f, BigDecimal.valueOf(1.23).subtract(BigDecimal
          .valueOf(2f)).doubleValue()},
        {"c0 / c1", BigDecimal.valueOf(123, 2), 2f, BigDecimal.valueOf(1.23).divide(BigDecimal
          .valueOf(2f)).doubleValue()},
        {"c0 * c1", BigDecimal.valueOf(123, 2), 2L, BigDecimal.valueOf(1.23).multiply(BigDecimal
          .valueOf(2l))},
        {"c0 * c1", BigDecimal.valueOf(123, 2), 2, BigDecimal.valueOf(1.23).multiply(BigDecimal
          .valueOf(2))},
        {"mod(c0,c1)", BigDecimal.valueOf(123, 2), 2f, BigDecimal.valueOf(1.23).divideAndRemainder
        (BigDecimal.valueOf(2f))[1].doubleValue()},
      });
  }


  @Test
  public void testDecimalDoubleAdd() throws Exception {
    testFunctionsCompiledOnly(
      new Object[][] {
        {"castDECIMAL(c0,3l,2l) + c1", BigDecimal.valueOf(123, 2), 2d,
          BigDecimal.valueOf(1.23).add(BigDecimal.valueOf(2)).doubleValue()},
      });
  }

  @Test
  public void testMixedDecimalReturnTypesInIfExpr() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      // mixed decimal return types in an if-else condition.
      {"case when c0 > castDECIMAL('2.45', 3l, 2l) " +
        " then castDECIMAL('2.45', 36l, 2l) " +
        " else castDECIMAL('2.45', 36l, 0l)  end",
        BigDecimal.valueOf(7.62),
        new BigDecimal("2.45")}
    });
  }

  @Test
  public void testMixedDecimalReturnTypesInIfExprOverflow() throws Exception {
    // using manual matching instead of expected exception since the actual
    // cause is deep down the exception chain.
    boolean exceptionThrown = false;
    try {
      testFunctionsCompiledOnly(new Object[][]{
        // mixed decimal return types in an if-else condition.
        {"case when c0 > castDECIMAL('2.45', 3l, 2l) " +
          " then castDECIMAL('2.45', 36l, 2l) " +
          " else castDECIMAL('2.45', 37l, 0l)  end",
          BigDecimal.valueOf(7.62),
          new BigDecimal("2.45")}
      });
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().getCause() instanceof  UnsupportedOperationException);
      Assert.assertTrue(e.getCause().getCause().getMessage().contains("Incompatible precision and scale"));
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testMixedReturnTypesInIfExpr() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"case when false " +
        " then castDECIMAL('2.45', 38l, 2l) " +
        " else c0  end",
        12l,
        new BigDecimal("12.00")},
      {"case when false " +
        " then castDECIMAL('2.45', 38l, 2l) " +
        " else c0 end",
        0.0d,
        0.0d},
      {"case when true " +
        " then castDECIMAL('2.45', 38l, 2l) " +
        " else c0 end",
        0.0d,
        2.45d},
      {"case when c0 > 2 " +
        " then castDECIMAL('2.45', 38l, 2l) " +
        " else c1 end",
        100,
        1.12f,
        2.45d},
    });
  }

  @Test
  public void testDecimalLiteralOperations() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 * 1 / c1",
        new BigDecimal("12.00"),
        new BigDecimal("1"),
        new BigDecimal("12.000000")},
      {"castDECIMAL('999999999999999999' ,18l,0l) + castDECIMAL" +
        "('-0.0000000000000000000000000000000000001',38l,38l)",
        new BigDecimal("999999999999999999.0000000000000000000")}
    });
  }

  @Test
  public void testIsNull() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"isnull(c0)", BigDecimal.valueOf(12.356D), false},
      {"isnull(c0)", NULL_DECIMAL, true},
    });
  }

  @Test
  public void testIsNotNull() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"isnotnull(c0)", BigDecimal.valueOf(12.356D), true},
      {"isnotnull(c0)", NULL_DECIMAL, false},
    });
  }

  @Test
  public void testIsNumeric() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"isnumeric(c0)", BigDecimal.valueOf(12.356D), true},
      {"isnumeric(c0)", NULL_DECIMAL, false},
    });
  }

  @Test
  public void testIsDistinct() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"is_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), BigDecimal.valueOf(12.356D), false},
      {"is_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), BigDecimal.valueOf(1.35D), true},
      {"is_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), NULL_DECIMAL, true},
      {"is_distinct_from(c0, c1)", NULL_DECIMAL, NULL_DECIMAL, false},
    });
  }

  @Test
  public void testIsNotDistinct() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"is_not_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), BigDecimal.valueOf(12.356D), true},
      {"is_not_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), BigDecimal.valueOf(1.35D), false},
      {"is_not_distinct_from(c0, c1)", BigDecimal.valueOf(12.356D), NULL_DECIMAL, false},
      {"is_not_distinct_from(c0, c1)", NULL_DECIMAL, NULL_DECIMAL, true},
    });
  }

  @Test
  public void testDecimalHashFunctions() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"hash32(c0)", NULL_DECIMAL, 0},
      {"hash64(c0)", NULL_DECIMAL, 0L},
      {"hash(c0)", BigDecimal.valueOf(10, 2), 767196228},
      {"hash32(c0)", BigDecimal.valueOf(10, 1), 767196228},
      {"hash32(c0)", BigDecimal.valueOf(11, 1), -104032488},
      {"hash64(c0)", BigDecimal.valueOf(11, 1), -8244673537519175652L},
      {"hash64(c0)", BigDecimal.valueOf(10, 1), 7567921574379139112L},
      {"hash64(c0)", BigDecimal.valueOf(10, 2), 7567921574379139112L},
      {"hash32AsDouble(c0)", BigDecimal.valueOf(10, 2), 767196228},
      {"hash64AsDouble(c0)", BigDecimal.valueOf(10, 1), 7567921574379139112L},
    });
  }

  @Test
  public void testDecimalHashWithSeedFunctions() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"hash32(c0, c1)", NULL_DECIMAL, 10, 10},
      {"hash64(c0, c1)", NULL_DECIMAL, 10, 10L},
      {"hash32(c0, c1)", BigDecimal.valueOf(10, 2), 10, -268274402},
      {"hash32(c0, c1)", BigDecimal.valueOf(10, 1), 10, -268274402},
      {"hash32(c0, c1)", BigDecimal.valueOf(11, 1), 10, -2080146543},
      {"hash64(c0, c1)", BigDecimal.valueOf(11, 1), 10, -7738029686922063901L},
      {"hash64(c0, c1)", BigDecimal.valueOf(10, 1), 10, -1652176568671252228L},
      {"hash64(c0, c1)", BigDecimal.valueOf(10, 2), 10, -1652176568671252228L},
      {"hash32AsDouble(c0, c1)", BigDecimal.valueOf(10, 2), 10, -268274402},
      {"hash64AsDouble(c0, c1)", BigDecimal.valueOf(10, 1), 10, -1652176568671252228L},
    });
  }

  @Test
  public void testDecimalCastNull() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"case when true then castDECIMAL(__$INTERNAL_NULL$__ ,38l,0l) else c0 end",new BigDecimal
        ("1"),
        Fixtures.NULL_DECIMAL}
    });
  }

  @Test
  public void testDecimalVarcharCastFunctions() throws Exception {
    testFunctions(new Object[][] {
      {"castDECIMAL(c0, 38l, 3l)", "1.2354", BigDecimal.valueOf(1235, 3)},
      {"castDECIMAL(c0, 38l, 2l)", "1.236", BigDecimal.valueOf(124, 2)},
      {"castDECIMAL(c0, 38l, 2l)", "1.234", BigDecimal.valueOf(123, 2)},
      {"castDECIMAL(c0, 38l, 4l)", "1.234", BigDecimal.valueOf(12340, 4)},
      {"castVARCHAR(c0, c1)", BigDecimal.valueOf(1256, 3), 5L, "1.256"},
      {"castVARCHAR(c0, c1)", BigDecimal.valueOf(1256, 3), 3L, "1.2"},
      {"castVARCHAR(c0, c1)", BigDecimal.valueOf(1256, 3), 6L, "1.256"},
      {"castVARCHAR(c0, c1)", BigDecimal.valueOf(-1256, 3), 1L, "-"},
      {"castVARCHAR(c0, c1)", BigDecimal.valueOf(-1256, 3), 2L, "-1"},
    });
  }

  @Test(expected = RuntimeException.class)
  public void testDecimalVarcharCastWithInvalidInput() throws Exception {
    try {
      testFunctions(new Object[][] {
        {"castDECIMAL(c0, 38l, 3l)", "a1.2354", BigDecimal.valueOf(1235, 3)},
      });
    } catch (RuntimeException ex) {
      // assert gandiva message
      Assert.assertTrue(ex.getCause().getCause().getMessage().contains("not a valid decimal number"));
      throw ex;
    }
  }

  @Test
  public void testGdvFunctionAliases() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 % c1", 64L, 2, 0},
      {"c0 % c1", BigDecimal.valueOf(2.5), BigDecimal.valueOf(1.2), BigDecimal.valueOf(0.1)},
      {"mod(c0, c1)", BigDecimal.valueOf(2.5), BigDecimal.valueOf(1.2), BigDecimal.valueOf(0.1)},
      {"modulo(c0, c1)", BigDecimal.valueOf(2.5), BigDecimal.valueOf(1.2), BigDecimal.valueOf(0.1)},
      {"pow(c0, c1)", 10.0D, 2.0D, 100.0D},
    });
  }

  @Test
  public void testMisc() throws Exception {
    testFunctions(new Object[][]{
      {"castDECIMAL('123456789', 9l, 0l) / castDECIMAL" +
        "('1234567890123456789.1234567890123456789'," +
        "38l,5l)", new BigDecimal("9.9999999990000E-11")},
      {"castDECIMAL('-999999999', 9l, 0l) / castDECIMAL('-9999999999999999999999999999999999999'," +
        "38l,0l)", new BigDecimal("1.0E-28")},
      {"castDECIMAL('12345.6789', 9l, 4l) / castDECIMAL('1234567890123456789.1234567890123456789'," +
        "38l,19l)", new BigDecimal("1E-14")},
    });
  }

  @Test
  public void testCastDecimalNullOnOverflow() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
        {"castDECIMALNullOnOverflow(c0, 38l, 1l)", DecimalUtils.MAX_DECIMAL, Fixtures
            .createDecimal(null, 38, 1)},
        {"castDECIMALNullOnOverflow(c0, 38l, 1l)", new BigDecimal("1.234"), new BigDecimal("1.2")},
        {"castDECIMALNullOnOverflow(c0, 2l, 2l)", new BigDecimal("111111111111111111111.111111111"), Fixtures
            .createDecimal(null, 2, 2)},
    });
  }

  @Test
  @Ignore("DX-20528")
  public void testCastDecimalZeroOnOverflow() {
    testFunctionsCompiledOnly(new Object[][]{
        {"castDECIMAL(c0, 2l, 2l)", new BigDecimal("111111111111111111111.111111111"), Fixtures
            .createDecimal(new BigDecimal("0.00"), 2, 2)}
    });
  }
}
