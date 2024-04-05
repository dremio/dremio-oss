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
package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.dremio.exec.ExecConstants;
import com.dremio.sabot.BaseTestFunction;
import org.junit.Assert;
import org.junit.Test;

public class TestMathFunctions extends BaseTestFunction {

  @Test
  public void basicMathFunctions() {
    testFunctions(
        new Object[][] {
          {"c0 * c1", 1, 2, 2},
          {"c0 * c1", 1.1f, 2.2f, 2.42f},
          {"c0 + c1", 1, 2, 3},
          {"c0 + c1", 1.1f, 2.2f, 3.3f}
        });
  }

  @Test
  public void testJavaFloatingPointDivide() throws Exception {
    try (AutoCloseable closeable = with(ExecConstants.DISABLED_GANDIVA_FUNCTIONS, "divide")) {
      testFunctions(
          new Object[][] {
            {"c0/c1", 100.0f, 10.0f, 10.0f},
            {"c0/c1", 100.0d, 10.0d, 10.0d}
          });
      try {
        testFunctions(new Object[][] {{"c0/c1", 1.1f, 0.0f, Float.NaN}});
        Assert.fail();
      } catch (Exception exception) {
        Assert.assertEquals("divide by zero", exception.getCause().getCause().getMessage());
      }
      try {
        testFunctions(new Object[][] {{"c0/c1", 1.1d, 0.0d, Double.NaN}});
        Assert.fail();
      } catch (Exception exception) {
        Assert.assertEquals("divide by zero", exception.getCause().getCause().getMessage());
      }
    }
  }

  @Test
  public void testFactorialFunctions() throws Exception {
    testFunctions(
        new Object[][] {
          {"factorial(c0)", 0, 1L},
          {"factorial(c0)", 1L, 1L},
          {"factorial(c0)", 2, 2L},
          {"factorial(c0)", 3, 6L},
          {"factorial(c0)", 4, 24L},
          {"factorial(c0)", 20, 2432902008176640000L},
        });

    try {
      testFunction("factorial(c0)", 21, 0L);
      fail("Query expected to fail");
    } catch (Exception e) {
      assertEquals("Numbers greater than 20 cause overflow!", e.getCause().getCause().getMessage());
    }

    try {
      testFunction("factorial(c0)", -1, 0L);
      fail("Query expected to fail");
    } catch (Exception e) {
      assertEquals("Factorial of negative number not exist!", e.getCause().getCause().getMessage());
    }
  }

  @Test
  public void testPmodFunctions() throws Exception {
    testFunctions(
        new Object[][] {
          {"pmod(c0, c1)", 3, 4, 3},
          {"pmod(c0, c1)", 4, 3, 1},
          {"pmod(c0, c1)", -3, 4, 1},
          {"pmod(c0, c1)", -4, 3, 2},
          {"pmod(c0, c1)", 3, -4, -1},
          {"pmod(c0, c1)", 4, -3, -2},
          {"pmod(c0, c1)", 0, 3, 0},
          {"pmod(c0, c1)", 3L, 4L, 3L},
          {"pmod(c0, c1)", 4L, 3L, 1L},
          {"pmod(c0, c1)", -3L, 4L, 1L},
          {"pmod(c0, c1)", -4L, 3L, 2L},
          {"pmod(c0, c1)", 3L, -4L, -1L},
          {"pmod(c0, c1)", 4L, -3L, -2L},
          {"pmod(c0, c1)", 0L, 3L, 0L},
          {"pmod(c0, c1)", 5.1f, 3.0f, 2.1f},
          {"pmod(c0, c1)", -5.1f, 3.0f, 0.9f},
          {"pmod(c0, c1)", 5.1f, -3.0f, -0.9000001f},
          {"pmod(c0, c1)", -5.1f, -3.0f, -5.1f},
          {"pmod(c0, c1)", 1.2f, 2.4f, 1.2f},
          {"pmod(c0, c1)", -1.2f, 2.4f, 1.2f},
          {"pmod(c0, c1)", 1.2f, -2.4f, -1.2f},
          {"pmod(c0, c1)", -1.2f, -2.4f, -3.6000001f},
          {"pmod(c0, c1)", 0.0f, 2.4f, 0.0f},
          {"pmod(c0, c1)", 5.1, 3.0, 2.0999999999999996},
          {"pmod(c0, c1)", -5.1, 3.0, 0.9000000000000004},
          {"pmod(c0, c1)", 5.1, -3.0, -0.9000000000000004},
          {"pmod(c0, c1)", -5.1, -3.0, -5.1},
          {"pmod(c0, c1)", 1.2, 2.4, 1.1999999999999997},
          {"pmod(c0, c1)", -1.2, 2.4, 1.2},
          {"pmod(c0, c1)", 1.2, -2.4, -1.2},
          {"pmod(c0, c1)", -1.2, -2.4, -3.5999999999999996},
          {"pmod(c0, c1)", 0.0, 2.4, 0.0},
        });
  }

  @Test
  public void testShiftFunctions() throws Exception {
    testFunctions(
        new Object[][] {
          {"shiftleft(1,2)", 4},
          {"shiftright(8,2)", 2},
          {"shiftleft(0,5)", 0},
          {"shiftright(0,5)", 0},
          {"shiftrightunsigned(0,5)", 0},
          {"shiftleft(1,31)", Integer.MIN_VALUE},
          {"shiftright(-16,2)", -4},
          {"shiftrightunsigned(-16,2)", 1073741820},
          {"rshiftunsigned(-16,2)", 1073741820},
          {"shiftrightunsigned(-1,31)", 1},
          {"shiftleft(100500,2)", 402000},
          {"shiftright(402000,2)", 100500},
          {"shiftrightunsigned(402000,2)", 100500},
        });
  }
}
