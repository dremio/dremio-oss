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
package com.dremio.exec.util;

import java.math.BigDecimal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

public class DecimalUtilsTest extends DremioTest {
  public static final String MIN_HALF = "-9999999999999999999";
  public static final String MAX_HALF = "9999999999999999999";
  private BufferAllocator testAllocator;
  private static final long LONG_MASK = Long.MIN_VALUE;//0xFFFFFFFFL;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("decimal-utils-test", 0, Long.MAX_VALUE);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(testAllocator);
  }

  @Test
  public void testBigDecimalComparisonPositiveWithNegative() {
    BigDecimal value1 = new BigDecimal("-32.001");
    BigDecimal value2 = new BigDecimal("0.01");

    int compare = getCompareResult(value1, value2,3 );
    Assert.assertTrue(compare  < 0);

    value1 = new BigDecimal("0.00000000001");
    value2 = new BigDecimal("-0.01");
    compare = getCompareResult(value1, value2, 11);
    Assert.assertTrue(compare  > 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  > 0);
  }

  @Test
  public void testBigDecimalComparisonBothPositive() {
    BigDecimal value1 = new BigDecimal("32.001");
    BigDecimal value2 = new BigDecimal("0.01");

    int compare = getCompareResult(value1, value2, 3);
    Assert.assertTrue(compare  > 0);

    value1 = new BigDecimal("0.00000000009");
    value2 = new BigDecimal("0.01");
    compare = getCompareResult(value1, value2, 11 );
    Assert.assertTrue(compare  < 0);

    value1 = new BigDecimal("0.00000000001");
    value2 = new BigDecimal("0.00000000001");
    compare = getCompareResult(value1, value2, 11);
    Assert.assertTrue(compare  == 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MAX_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  == 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MAX_DECIMAL.subtract(BigDecimal.valueOf(Long.MAX_VALUE));
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  > 0);

    value1 = new BigDecimal(MAX_HALF).add(BigDecimal.ONE);
    value2 = value1.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);
  }

  @Test
  public void testBigDecimalComparisonBothNegative() {
    BigDecimal value1 = new BigDecimal("-0.5");
    BigDecimal value2 = new BigDecimal("-1.0");
    int compare = getCompareResult(value1, value2, 1);
    Assert.assertTrue(compare  > 0);

    value1 = new BigDecimal("-11.12345678901234567890");
    value2 = new BigDecimal("-2.00000000000000000000");
    compare = getCompareResult(value1, value2, 20);
    Assert.assertTrue(compare  < 0);

    value1 = new BigDecimal(MIN_HALF);
    value2 = new BigDecimal("-999999999999999999");
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);

    value1 = new BigDecimal(Long.MIN_VALUE);
    value2 = value1.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);

    value1 = new BigDecimal(MIN_HALF).add(new BigDecimal(MIN_HALF));
    value2 = new BigDecimal(MIN_HALF);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = new BigDecimal(Long.MIN_VALUE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  == 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare  < 0);
  }

  public int getCompareResult(BigDecimal value1, BigDecimal value2, int scale) {
    byte [] value1InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value1.setScale(scale));
    byte [] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2.setScale(scale));

    try (ArrowBuf bufferValue1 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);
         ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      bufferValue1.setBytes(0, value1InBytes, 0, value1InBytes.length);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      return compareSignedDecimalInLittleEndianBytes(bufferValue1, bufferValue2);
    }
  }

  private int compareSignedDecimalInLittleEndianBytes(ArrowBuf left, ArrowBuf right) {
    return DecimalUtils.compareSignedDecimalInLittleEndianBytes(left, 0 , right , 0);
  }

  @Test
  public void testDecimalAdditions() {
    try (ArrowBuf bufferValueResult = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      BigDecimal[] val1 = new BigDecimal[] {DecimalUtils.MIN_DECIMAL, BigDecimal.valueOf(Long
        .MIN_VALUE), BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)), BigDecimal.ZERO , BigDecimal
        .ONE, BigDecimal.valueOf(Long.MAX_VALUE), DecimalUtils.MAX_DECIMAL};

      BigDecimal[] val2 = new BigDecimal[] {DecimalUtils.MIN_DECIMAL, BigDecimal.valueOf(Long
        .MIN_VALUE), BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)), BigDecimal.ZERO , BigDecimal
        .ONE, BigDecimal.valueOf(2), BigDecimal.valueOf(Long.MAX_VALUE), DecimalUtils.MAX_DECIMAL};

      for (BigDecimal arg1 : val1) {
        for (BigDecimal arg2 : val2) {
          assertAdditionIsCorrect(arg1, arg2, bufferValueResult);
        }
      }
    }
  }

  private void assertAdditionIsCorrect(BigDecimal val, BigDecimal val2, ArrowBuf bufferValueResult) {
    getAdditionResult(val, val2, val.scale(), bufferValueResult);
    BigDecimal expected = val.add(val2);
    boolean overFlow = expected.precision() > 38;
    BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(bufferValueResult, 0, expected.scale(), DecimalVector.TYPE_WIDTH);
    Assert.assertTrue(overFlow || expected.compareTo(actual) == 0);
  }

  private boolean getAdditionResult(BigDecimal value1, BigDecimal value2, int scale, ArrowBuf
    result) {
    byte [] value1InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value1.setScale(scale));
    byte [] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2.setScale(scale));

    try (ArrowBuf bufferValue1 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);
         ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      bufferValue1.setBytes(0, value1InBytes, 0, value1InBytes.length);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      try {
        DecimalUtils.addSignedDecimalInLittleEndianBytes(bufferValue1, 0,
          bufferValue2, 0, result, 0);
      } catch (ArithmeticException e) {
        return true;
      }
    }
    return false;
  }

}
