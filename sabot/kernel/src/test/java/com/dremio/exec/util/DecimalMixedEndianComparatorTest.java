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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class DecimalMixedEndianComparatorTest {
  private BufferAllocator testAllocator;

  @Before
  public void setupBeforeTest() {
    testAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  public void compareMixedEndianValues() {
    DecimalMixedEndianComparator decimalComparatorMixedEndian16Bytes = new DecimalMixedEndian16BytesComparator();
    DecimalMixedEndianComparator decimalComparatorMixedEndianGT8Bytes = new DecimalMixedEndianGT8BytesComparator();
    DecimalMixedEndianComparator decimalComparatorMixedEndianLE8Bytes = new DecimalMixedEndianLE8BytesComparator();

    BigDecimal[] leftValLE8Bytes = new BigDecimal[]{BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)), BigDecimal.ZERO, BigDecimal
      .ONE, BigDecimal.valueOf(Long.MAX_VALUE)};

    BigDecimal[] leftValGT8Bytes = new BigDecimal[]{BigDecimal.valueOf(Long.MAX_VALUE).multiply
      (BigDecimal.valueOf(100)), BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf
      (-10)), BigDecimal.valueOf(Long.MIN_VALUE).multiply(BigDecimal.valueOf(100))};

    BigDecimal[] leftVal16Bytes = new BigDecimal[]{DecimalUtils.MIN_DECIMAL, DecimalUtils.MAX_DECIMAL};

    BigDecimal[] val2 = new BigDecimal[]{DecimalUtils.MIN_DECIMAL, BigDecimal.valueOf(Long
      .MIN_VALUE), BigDecimal.ONE.multiply(BigDecimal.valueOf(-1)), BigDecimal.ZERO, BigDecimal
      .ONE, BigDecimal.valueOf(2), BigDecimal.valueOf(Long.MAX_VALUE), DecimalUtils.MAX_DECIMAL};

    for (BigDecimal arg1 : leftValLE8Bytes) {
      for (BigDecimal arg2 : val2) {
        int compare = getCompareResultForMixedEndianBytes(arg1, arg2, 0,
          decimalComparatorMixedEndianLE8Bytes);
        Assert.assertTrue(compare == arg1.compareTo(arg2));
      }
    }

    for (BigDecimal arg1 : leftValGT8Bytes) {
      for (BigDecimal arg2 : val2) {
        int compare = getCompareResultForMixedEndianBytes(arg1, arg2, 0,
          decimalComparatorMixedEndianGT8Bytes);
        Assert.assertTrue(compare == arg1.compareTo(arg2));
      }
    }

    for (BigDecimal arg1 : leftVal16Bytes) {
      for (BigDecimal arg2 : val2) {
        int compare = getCompareResultForMixedEndianBytes(arg1, arg2, 0,
          decimalComparatorMixedEndian16Bytes);
        Assert.assertTrue(compare == arg1.compareTo(arg2));
      }
    }

  }

  private int getCompareResultForMixedEndianBytes(BigDecimal value1, BigDecimal value2, int
    scale, DecimalMixedEndianComparator decimalComparatorMixedEndian16Bytes) {
    byte[] value1InBytes = value1.setScale(scale).unscaledValue().toByteArray();
    byte[] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2.setScale(scale));
    try (ArrowBuf bufferValue1 = testAllocator.buffer(value1InBytes.length);
         ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      bufferValue1.setBytes(0, value1InBytes, 0, value1InBytes.length);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      return decimalComparatorMixedEndian16Bytes.compare
        (bufferValue1, 0, value1InBytes.length, bufferValue2, 0);
    }
  }

  @Test
  public void compareMixedEndianValuesForInt() {
    int val1 = 40;
    BigDecimal val2 = BigDecimal.valueOf(40);
    int compare = getCompareResultForMixedEndianBytesInt(val1, val2);
    Assert.assertTrue(compare == 0);

    val1 = -40;
    val2 = BigDecimal.valueOf(-40);
    compare = getCompareResultForMixedEndianBytesInt(val1, val2);
    Assert.assertTrue(compare == 0);

    val1 = -40;
    val2 = BigDecimal.valueOf(-80);
    compare = getCompareResultForMixedEndianBytesInt(val1, val2);
    Assert.assertTrue(compare > 0);
  }

  @Test
  public void compareMixedEndianValuesForLong() {
    long val1 = Long.MIN_VALUE;
    BigDecimal val2 = BigDecimal.valueOf(Long.MIN_VALUE);
    int compare = getCompareResultForMixedEndianBytesLong(val1, val2);
    Assert.assertTrue(compare == 0);

    val1 = Long.MAX_VALUE;
    val2 = BigDecimal.valueOf(Long.MAX_VALUE - 1);
    compare = getCompareResultForMixedEndianBytesLong(val1, val2);
    Assert.assertTrue(compare > 0);

    val1 = Long.MIN_VALUE;
    val2 = BigDecimal.valueOf(-0);
    compare = getCompareResultForMixedEndianBytesLong(val1, val2);
    Assert.assertTrue(compare < 0);
  }

  private int getCompareResultForMixedEndianBytesInt(int value1, BigDecimal value2) {
    byte[] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2);
    try (ArrowBuf bufferValue1 = testAllocator.buffer(4);
         ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      bufferValue1.setInt(0, value1);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      return new IntDecimalComparator().compare(bufferValue1, 0, 4,
        bufferValue2, 0);
    }
  }

  private int getCompareResultForMixedEndianBytesLong(long value1, BigDecimal value2) {
    byte[] value2InBytes = DecimalUtils.convertBigDecimalToArrowByteArray(value2);
    try (ArrowBuf bufferValue1 = testAllocator.buffer(8);
         ArrowBuf bufferValue2 = testAllocator.buffer(DecimalUtils.DECIMAL_WIDTH);) {
      bufferValue1.setLong(0, value1);
      bufferValue2.setBytes(0, value2InBytes, 0, value2InBytes.length);
      return new LongDecimalComparator().compare(bufferValue1, 0
        , 8, bufferValue2, 0);
    }
  }
}
