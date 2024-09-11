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

import com.dremio.common.expression.SupportedEngines.CodeGenOption;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests Gandiva decimal functions that run across java and gandiva, but with the default code
 * generator set to gandiva.
 */
public class TestDecimalFunctionsInGandivaCodeGen extends TestMixedDecimalFunctionTests {

  public TestDecimalFunctionsInGandivaCodeGen() {
    execPreference = CodeGenOption.Gandiva.toString();
  }

  @Test
  public void testCot() {
    Assume.assumeFalse(
        "Test is ignored on aarch64 architecture!",
        System.getProperty("os.arch").equals("aarch64"));
    testFunctionsCompiledOnly(
        new Object[][] {
          {"truncate(cot(c0),5)", -2147483649D, 2.91303D},
          {"truncate(cot(c0),5)", -2147483648D, 0.24484D},
          {"truncate(cot(c0),5)", -32769D, 0.71063D},
          {"truncate(cot(c0),5)", -32768D, -0.40193D},
          {"truncate(cot(c0),5)", -34.84D, -3.44539D},
          {"truncate(cot(c0),5)", -2D, 0.45765D},
          {"truncate(cot(c0),5)", -1.2D, -0.38877D},
          {"truncate(cot(c0),5)", -1D, -0.64209D},
          {"truncate(cot(c0),5)", 1D, 0.64209D},
          {"truncate(cot(c0),5)", 1.3D, 0.27761D},
          {"truncate(cot(c0),5)", 2D, -0.45765D},
          {"truncate(cot(c0),5)", 0.25D, 3.91631D},
          {"truncate(cot(c0),5)", 1004.3D, -0.62854D},
          {"truncate(cot(c0),5)", 32767D, 5.23855D},
          {"truncate(cot(c0),5)", 32768D, 0.40193D},
          {"truncate(cot(c0),5)", 2147483647D, 0.95022D},
          {"truncate(cot(c0),5)", 2147483648D, -0.24484D},
        });
  }
}
