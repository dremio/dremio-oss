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
package com.dremio;

import java.math.BigDecimal;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestFunction;

/**
 * Tests decimal functions implemented in Gandiva.
 */
public class TestDecimalFunctions extends BaseTestFunction {

  @BeforeClass
  public static void setUp() {
    testContext.getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      PlannerSettings.ENABLE_DECIMAL_V2_KEY,
      true));
  }

  @AfterClass
  public static void tearDown() {
    testContext.getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      PlannerSettings.ENABLE_DECIMAL_V2_KEY,
      PlannerSettings.ENABLE_DECIMAL_V2.getDefault().getBoolVal()));
  }

  @Test
  public void testDecimalAdd() throws Exception {
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 + castDECIMAL('2.45', 3l, 2l)",
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
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 - castDECIMAL('2.45', 3l, 2l)",
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
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 * castDECIMAL('2.45', 3l, 2l)",
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
    testFunctionsCompiledOnly(new Object[][]{
      {"c0 / castDECIMAL('2.45', 3l, 2l)",
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
    testFunctionsCompiledOnly(new Object[][]{
      {"mod(c0, castDECIMAL('2.45', 38l, 2l))",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(7.62).divideAndRemainder(BigDecimal.valueOf(2.45))[1]},

      {"mod(c0, c1)",
        BigDecimal.valueOf(7.62),
        BigDecimal.valueOf(2.45),
        BigDecimal.valueOf(7.62).divideAndRemainder(BigDecimal.valueOf(2.45))[1]},
    });
  }
}
