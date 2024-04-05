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
package com.dremio.exec.store.dfs.implicit;

import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

public class TestDecimalTools {

  private static void test(String number, String expectedBinary) {
    BigDecimal decimal = new BigDecimal(number);
    String actualBinary =
        DecimalTools.toBinary(DecimalTools.signExtend16(decimal.unscaledValue().toByteArray()));
    Assert.assertEquals(expectedBinary, actualBinary);
  }

  @Test
  public void zero() {
    test(
        "0",
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
  }

  @Test
  public void one() {
    test(
        "1",
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  }

  @Test
  public void negativeOne() {
    test(
        "-1",
        "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
  }

  @Test
  public void negativePointOne() {
    test(
        "-0.1",
        "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
  }

  @Test
  public void negativeTrailingZeroes() {
    test(
        "-0.1000",
        "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111110000011000");
  }
}
