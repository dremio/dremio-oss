/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.math.BigDecimal;

import org.junit.Test;

import com.dremio.sabot.BaseTestFunction;

public class TestNewMathFunctions extends BaseTestFunction {

  @Test
  public void trig(){
    testFunctions(new Object[][]{
      {"sin(45)", Math.sin(45)},
      {"cos(45)", Math.cos(45)},
      {"tan(45)", Math.tan(45)},
      {"asin(45)", Math.asin(45)},
      {"acos(45)", Math.acos(45)},
      {"atan(45)", Math.atan(45)},
      {"sinh(45)", Math.sinh(45)},
      {"tanh(45)", Math.tanh(45)},

    });
  }

  @Test
  public void extendedMath() {
     final BigDecimal d = new BigDecimal("100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001");
     testFunctions(new Object[][]{
     { "cbrt(1000)", Math.cbrt(1000)},
     { "log(10)", Math.log(10)},
     { "log10(10)", Math.log10(10)},
     { "log(2.0, 64.0)", (Math.log(64.0)/Math.log(2.0))},
     { "exp(10)", Math.exp(10)},
     { "degrees(0.5)",  Math.toDegrees(0.5)},
     { "radians(45.0)", Math.toRadians(45.0)},
     { "pi()" , Math.PI},
     { "cbrt(100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", Math.cbrt(d.doubleValue())},
     { "log(100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", Math.log(d.doubleValue())},
     { "log(2, 100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", (Math.log(d.doubleValue())/Math.log(2))},
     { "exp(100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", Math.exp(d.doubleValue())},
     { "degrees(100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", Math.toDegrees(d.doubleValue())},
     { "radians(100111111111111111111111111111111111.00000000000000000000000000000000000000000000000000001d)", Math.toRadians(d.doubleValue())}
   });
   }

  @Test
  public void truncDivMod() throws Exception {
    testFunctions(new Object[][]{
      { "truncate(101.11)", 101.0f},
      { "truncate(101.11, 1)", 101.1f},
      { "truncate(101.11, -1)", 100.0f},
      { "div(101, 111)", 0},
      { "mod(101, 111)", 101},
      { "truncate(1010.00)", 1010.0f},
      { "mod(101, 0)", 101},
      { "div(1010.1010, 2.1)", 481.0f}
    });
  }

  @Test
  public void isNumeric(){
    testFunctions(new Object[][]{
      { "isnumeric(500)", true},
      { "isnumeric(-500)", true},
      { "isnumeric(1000.000000000089)", true},
      { "isnumeric('String')", false}
    });
  }
}
