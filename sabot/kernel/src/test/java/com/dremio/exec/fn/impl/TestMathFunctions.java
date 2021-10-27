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

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.sabot.BaseTestFunction;

public class TestMathFunctions extends BaseTestFunction {

  @Test
  public void basicMathFunctions() {
    testFunctions(new Object[][]{
      {"c0 * c1", 1, 2, 2},
      {"c0 * c1", 1.1f, 2.2f, 2.42f},
      {"c0 + c1", 1, 2, 3},
      {"c0 + c1", 1.1f, 2.2f, 3.3f}
    });
  }

  @Test
  public void testJavaFloatingPointDivide() throws Exception {
    try (
      AutoCloseable closeable = with(ExecConstants.DISABLED_GANDIVA_FUNCTIONS, "divide")
    ) {
      testFunctions(new Object[][]{
        {"c0/c1", 100.0f, 10.0f, 10.0f},
        {"c0/c1", 100.0d, 10.0d, 10.0d}
      });
      try {
        testFunctions(new Object[][]{
          {"c0/c1", 1.1f, 0.0f, Float.NaN}
        });
        Assert.fail();
      } catch (Exception exception) {
        Assert.assertEquals("divide by zero",
          exception.getCause().getCause().getMessage());      }
      try {
        testFunctions(new Object[][]{
          {"c0/c1", 1.1d, 0.0d, Double.NaN}
        });
        Assert.fail();
      } catch (Exception exception) {
        Assert.assertEquals("divide by zero",
          exception.getCause().getCause().getMessage());
      }
    }
  }
}
