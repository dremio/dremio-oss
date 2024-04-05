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

import com.dremio.common.AutoCloseables;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DecimalUtilsTest extends DremioTest {
  public static final String MIN_HALF = "-9999999999999999999";
  public static final String MAX_HALF = "9999999999999999999";
  private BufferAllocator testAllocator;
  private static final long LONG_MASK = Long.MIN_VALUE; // 0xFFFFFFFFL;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

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

    int compare = getCompareResult(value1, value2, 3);
    Assert.assertTrue(compare < 0);

    value1 = new BigDecimal("0.00000000001");
    value2 = new BigDecimal("-0.01");
    compare = getCompareResult(value1, value2, 11);
    Assert.assertTrue(compare > 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare > 0);
  }

  @Test
  public void testBigDecimalComparisonBothPositive() {
    BigDecimal value1 = new BigDecimal("32.001");
    BigDecimal value2 = new BigDecimal("0.01");

    int compare = getCompareResult(value1, value2, 3);
    Assert.assertTrue(compare > 0);

    value1 = new BigDecimal("0.00000000009");
    value2 = new BigDecimal("0.01");
    compare = getCompareResult(value1, value2, 11);
    Assert.assertTrue(compare < 0);

    value1 = new BigDecimal("0.00000000001");
    value2 = new BigDecimal("0.00000000001");
    compare = getCompareResult(value1, value2, 11);
    Assert.assertTrue(compare == 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MAX_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare == 0);

    value1 = DecimalUtils.MAX_DECIMAL;
    value2 = DecimalUtils.MAX_DECIMAL.subtract(BigDecimal.valueOf(Long.MAX_VALUE));
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare > 0);

    value1 = new BigDecimal(MAX_HALF).add(BigDecimal.ONE);
    value2 = value1.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);
  }

  @Test
  public void testBigDecimalComparisonBothNegative() {
    BigDecimal value1 = new BigDecimal("-0.5");
    BigDecimal value2 = new BigDecimal("-1.0");
    int compare = getCompareResult(value1, value2, 1);
    Assert.assertTrue(compare > 0);

    value1 = new BigDecimal("-11.12345678901234567890");
    value2 = new BigDecimal("-2.00000000000000000000");
    compare = getCompareResult(value1, value2, 20);
    Assert.assertTrue(compare < 0);

    value1 = new BigDecimal(MIN_HALF);
    value2 = new BigDecimal("-999999999999999999");
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);

    value1 = new BigDecimal(Long.MIN_VALUE);
    value2 = value1.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);

    value1 = new BigDecimal(MIN_HALF).add(new BigDecimal(MIN_HALF));
    value2 = new BigDecimal(MIN_HALF);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = new BigDecimal(Long.MIN_VALUE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL;
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare == 0);

    value1 = DecimalUtils.MIN_DECIMAL;
    value2 = DecimalUtils.MIN_DECIMAL.add(BigDecimal.ONE);
    compare = getCompareResult(value1, value2, 0);
    Assert.assertTrue(compare < 0);
  }

  public int getCompareResult(BigDecimal value1, BigDecimal value2, int scale) {
    byte[] value1InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value1.setScale(scale));
    byte[] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2.setScale(scale));

    try (ArrowBuf bufferValue1 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);
        ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH); ) {
      bufferValue1.setBytes(0, value1InBytes, 0, value1InBytes.length);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      return compareSignedDecimalInLittleEndianBytes(bufferValue1, bufferValue2);
    }
  }

  private int compareSignedDecimalInLittleEndianBytes(ArrowBuf left, ArrowBuf right) {
    return DecimalUtils.compareSignedDecimalInLittleEndianBytes(left, 0, right, 0);
  }

  @Test
  public void testDecimalAdditions() {
    try (ArrowBuf bufferValueResult = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH); ) {
      BigDecimal[] val1 =
          new BigDecimal[] {
            DecimalUtils.MIN_DECIMAL,
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)),
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.valueOf(Long.MAX_VALUE),
            DecimalUtils.MAX_DECIMAL
          };

      BigDecimal[] val2 =
          new BigDecimal[] {
            DecimalUtils.MIN_DECIMAL,
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)),
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.valueOf(2),
            BigDecimal.valueOf(Long.MAX_VALUE),
            DecimalUtils.MAX_DECIMAL
          };

      for (BigDecimal arg1 : val1) {
        for (BigDecimal arg2 : val2) {
          assertAdditionIsCorrect(arg1, arg2, bufferValueResult);
        }
      }
    }
  }

  private void assertAdditionIsCorrect(
      BigDecimal val, BigDecimal val2, ArrowBuf bufferValueResult) {
    getAdditionResult(val, val2, val.scale(), bufferValueResult);
    BigDecimal expected = val.add(val2);
    boolean overFlow = expected.precision() > 38;
    BigDecimal actual =
        DecimalUtility.getBigDecimalFromArrowBuf(
            bufferValueResult, 0, expected.scale(), DecimalVector.TYPE_WIDTH);
    Assert.assertTrue(overFlow || expected.compareTo(actual) == 0);
  }

  private boolean getAdditionResult(
      BigDecimal value1, BigDecimal value2, int scale, ArrowBuf result) {
    byte[] value1InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value1.setScale(scale));
    byte[] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2.setScale(scale));

    try (ArrowBuf bufferValue1 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);
        ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH); ) {
      bufferValue1.setBytes(0, value1InBytes, 0, value1InBytes.length);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      try {
        DecimalUtils.addSignedDecimalInLittleEndianBytes(
            bufferValue1, 0, bufferValue2, 0, result, 0);
      } catch (ArithmeticException e) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testCompareDecimalsAsTwoLongs() {
    List<BigDecimal> values = new ArrayList<>();
    Random random = new Random(220013062023L);
    byte[] bytes = new byte[16];
    for (int i = 0; i < 100; ++i) {
      random.nextBytes(bytes);
      values.add(new BigDecimal(new BigInteger(bytes), 20));
    }
    Collections.sort(values);

    for (int left = 0, n = values.size(); left < n; ++left) {
      for (int right = 0; right < n; ++right) {
        BigInteger leftUnscaled = values.get(left).unscaledValue();
        long leftLow = leftUnscaled.longValue();
        long leftHigh = leftUnscaled.shiftRight(64).longValue();
        BigInteger rightUnscaled = values.get(right).unscaledValue();
        long rightLow = rightUnscaled.longValue();
        long rightHigh = rightUnscaled.shiftRight(64).longValue();

        int expected = left < right ? -1 : left > right ? 1 : 0;
        Assert.assertEquals(
            expected,
            DecimalUtils.compareDecimalsAsTwoLongs(leftHigh, leftLow, rightHigh, rightLow));
      }
    }
  }

  @Test
  public void testReadBigEndianIntoLong() {
    byte[] sourceArray = new byte[24];
    try (ArrowBuf sourceBuf = testAllocator.buffer(24)) {
      Random random = new Random(13062023);

      // Set garbage bits before and after
      random.nextBytes(sourceArray);
      sourceBuf.setLong(0, random.nextLong());
      sourceBuf.setLong(16, random.nextLong());

      for (int byteWidth = 1; byteWidth <= 8; ++byteWidth) {
        for (int i = 0; i < 100; ++i) {

          // Generate a random value in the boundaries related to byteWidth
          long expected = random.nextLong();
          int bitsToShift = (8 - byteWidth) * 8;
          expected <<= bitsToShift;
          expected >>= bitsToShift;

          // Write the long value in big endian byte order
          ByteBuffer.wrap(sourceArray, 8, 8).order(ByteOrder.BIG_ENDIAN).putLong(expected);
          sourceBuf.setLong(8, Long.reverseBytes(expected));

          // Read back only the meaningful byteWidth bytes
          long actual =
              DecimalUtils.readBigEndianIntoLong(sourceArray, 8 + (8 - byteWidth), byteWidth);
          Assert.assertEquals("<byte[]> byteWidth: " + byteWidth, expected, actual);
          actual = DecimalUtils.readBigEndianIntoLong(sourceBuf, 8 + (8 - byteWidth), byteWidth);
          Assert.assertEquals("<ArrowBuf> byteWidth: " + byteWidth, expected, actual);
        }
      }
    }
  }

  @Test
  public void testWriteDecimalAsTwoLongs() {
    BigDecimal[] values = {
      new BigDecimal("13160083730631869.05855370338472282155"),
      new BigDecimal("-10428434929042162.44916940774327827690"),
      new BigDecimal("2136278553926987.60878794309678511470"),
      new BigDecimal("919296940823423.82512659912324987068"),
      new BigDecimal("10336812515247389.51875852457569286957"),
      new BigDecimal("-1724073861930332.04787771806680092159"),
      new BigDecimal("-3861174555363047.50649673014895939355"),
      new BigDecimal("4801348183236962.42916427845552356154"),
      new BigDecimal("-2154924983101774.27403489486734608769"),
      BigDecimal.ZERO.setScale(20)
    };
    try (DecimalVector target =
        new DecimalVector("decimal-test", testAllocator, DecimalVector.MAX_PRECISION, 20)) {
      target.allocateNew(values.length);
      for (int i = 0; i < values.length; ++i) {
        BigInteger unscaled = values[i].unscaledValue();
        long lowValue = unscaled.longValue();
        long highValue = unscaled.shiftRight(64).longValue();
        DecimalUtils.writeDecimalAsTwoLongs(highValue, lowValue, target, i);
      }
      for (int i = 0; i < values.length; ++i) {
        BigDecimal expected = values[i];
        BigDecimal actual = target.getObject(i);
        Assert.assertEquals(expected, actual);
      }
    }
  }
}
