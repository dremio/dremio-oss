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

import java.math.BigDecimal;
import org.junit.Test;

/** decimal tests that have functions implemented across java and gandiva. */
public abstract class TestMixedDecimalFunctionTests extends BaseDecimalFunctionTests {

  @Test
  public void testDecimalDoubleFunctions() throws Exception {
    testFunctionsCompiledOnly(
        new Object[][] {
          {"sqrt(c0)", BigDecimal.valueOf(64, 2), BigDecimal.valueOf(0.8).doubleValue()},
          {
            "cos(c0)",
            BigDecimal.valueOf(64, 1),
            BigDecimal.valueOf(0.993184918758193).doubleValue()
          },
          {
            "sin(c0)",
            BigDecimal.valueOf(64, 1),
            BigDecimal.valueOf(0.116549204850494).doubleValue()
          },
          {
            "tan(c0)",
            BigDecimal.valueOf(64, 1),
            BigDecimal.valueOf(0.117348947461082).doubleValue()
          },
          {"power(sqrt(c0),2)", BigDecimal.valueOf(64, 2), BigDecimal.valueOf(0.64).doubleValue()},
        });
  }
}
