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
package com.dremio.exec.physical.impl;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class TestCastFunctions extends BaseTestFunction {

  @Test
  public void bigInt() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as bigint)", 14.0f, 14L},
          {"cast(c0 as bigint)", 14.0d, 14L},
          {"cast(c0 as bigint)", 14, 14L},
          {"cast(c0 as bigint)", "14", 14L},
        });
  }

  @Test
  public void integer() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as int)", 14.0f, 14},
          {"cast(c0 as int)", 14.0d, 14},
          {"cast(c0 as int)", 14L, 14},
          {"cast(c0 as int)", "14", 14},
        });
  }

  @Test
  public void floating() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as float4)", 14L, 14f},
          {"cast(c0 as float4)", 14.3d, 14.3f},
          {"cast(c0 as float4)", 14, 14f},
          {"cast(c0 as float4)", "14.3", 14.3f},
        });
  }

  @Test
  public void doublePrecision() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as float8)", 14L, 14d},
          {"cast(c0 as float8)", 14f, 14d},
          {"cast(c0 as float8)", 14, 14d},
          {"cast(c0 as float8)", "14.3", 14.3d},
        });
  }

  @Test
  public void varchar() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as varchar(30))", 14L, "14"},
          {"cast(c0 as varchar(30))", 14.3f, "14.3"},
          {"cast(c0 as varchar(30))", 14, "14"},
          {"cast(c0 as varchar(30))", 14.3d, "14.3"},
        });
  }

  @Test
  public void fromBigInt() {
    testFunctions(
        new Object[][] {
          {
            "cast(c0 as INTERVALDAY)", 14L, Period.millis(14)
          }, // INTERVALDAY is represented in milliseconds
          {
            "cast(c0 as INTERVALYEAR)", 7L, Period.months(7)
          }, // INTERVALYEAR is represented in months
        });
  }

  @Test
  public void fromInteger() {
    testFunctions(
        new Object[][] {
          {"cast(c0 as INTERVALDAY)", Integer.valueOf(14), Period.millis(14)},
          {"cast(c0 as INTERVALYEAR)", Integer.valueOf(7), Period.months(7)},
        });
  }

  @Test
  public void stringDecimalOverflow() {
    testFunctionsCompiledOnly(
        new Object[][] {
          {"castDECIMALNullOnOverflow(c0, 2l, 0l)", "99.99", Fixtures.createDecimal(null, 2, 0)},
          {
            "castDECIMALNullOnOverflow(c0, 2l, 0l)",
            "9.99",
            Fixtures.createDecimal(new BigDecimal("10"), 2, 0)
          },
        });
  }

  @Test
  public void decimalDecimalOverflow() {
    testFunctionsCompiledOnly(
        new Object[][] {
          {
            "castDECIMALNullOnOverflow(c0, 38l, 1l)",
            DecimalUtils.MAX_DECIMAL,
            Fixtures.createDecimal(null, 38, 1)
          }
        });
  }

  @Test
  public void stringDecimalOverflowException() {
    Class exceptionClass = null;
    try {
      testFunctionsCompiledOnly(
          new Object[][] {
            {"castDECIMALNullOnOverflow(c0, 2l, 0l)", "s3AWS", Fixtures.createDecimal(null, 2, 0)},
          });
    } catch (Exception e) {
      Throwable rootCause = e.getCause().getCause();
      exceptionClass = rootCause.getClass();
    }
    Assert.assertEquals(
        "Expected a number format exception", NumberFormatException.class, exceptionClass);
  }

  @Test
  public void fromListUserException() {
    final List<String> input1 = Arrays.asList(new String[] {"abc123", "abc456"});
    Class exceptionClass = null;
    String expectedErrorMessage = "";
    try {
      testFunctions(new Object[][] {{"cast(c0 as varchar(30))", input1, "abc"}});
    } catch (Exception e) {
      Throwable rootCause = e.getCause().getCause();
      exceptionClass = rootCause.getClass();
      expectedErrorMessage = e.getCause().getCause().getMessage();
    }
    Assert.assertEquals("Excepted a user exception", UserException.class, exceptionClass);
    Assert.assertTrue(
        expectedErrorMessage.startsWith("Dremio does not support casting or coercing list"));
  }
}
