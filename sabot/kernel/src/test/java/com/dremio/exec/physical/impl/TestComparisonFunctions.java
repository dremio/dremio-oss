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

import org.junit.Test;

import com.dremio.sabot.BaseTestFunction;

public class TestComparisonFunctions extends BaseTestFunction {

  Object[][] intTests = {
      {"c0 == c1", 10, 10, true},
      {"c0 == c1", 10, 11, false},
      {"c0 != c1", 10, 10, false},
      {"c0 != c1", 10, 11, true},
      {"c0 > c1", 10, 11, false},
      {"c0 < c1", 10, 11, true},
      {"c0 <= c1", 10, 11, true},
      {"c0 >= c1", 10, 11, false}
  };

  Object[][] bigIntTests = {
      {"c0 == c1", 10l, 10l, true},
      {"c0 == c1", 10l, 11l, false},
      {"c0 != c1", 10l, 10l, false},
      {"c0 != c1", 10l, 11l, true},
      {"c0 > c1", 10l, 11l, false},
      {"c0 < c1", 10l, 11l, true},
      {"c0 <= c1", 10l, 11l, true},
      {"c0 >= c1", 10l, 11l, false}
  };

  Object[][] floatTests = {
      {"c0 == c1", 10f, 10f, true},
      {"c0 == c1", 10f, 11f, false},
      {"c0 != c1", 10f, 10f, false},
      {"c0 != c1", 10f, 11f, true},
      {"c0 > c1", 10f, 11f, false},
      {"c0 < c1", 10f, 11f, true},
      {"c0 <= c1", 10f, 11f, true},
      {"c0 >= c1", 10f, 11f, false}
  };

  Object[][] doubleTests = {
      {"c0 == c1", 10d, 10d, true},
      {"c0 == c1", 10d, 11d, false},
      {"c0 != c1", 10d, 10d, false},
      {"c0 != c1", 10d, 11d, true},
      {"c0 > c1", 10d, 11d, false},
      {"c0 < c1", 10d, 11d, true},
      {"c0 <= c1", 10d, 11d, true},
      {"c0 >= c1", 10d, 11d, false}
  };

  @Test
  public void intComparisons(){
    testFunctions(intTests);
  }
  @Test
  public void bigIntComparisons(){
    testFunctions(bigIntTests);
  }

  @Test
  public void doubleComparisons(){
    testFunctions(doubleTests);
  }

  @Test
  public void floatComparisons(){
    testFunctions(floatTests);
  }
}
