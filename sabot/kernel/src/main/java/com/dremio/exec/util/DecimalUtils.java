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

import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteOrder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;

/** Utilities for the accumulators that operate over decimal values */
public final class DecimalUtils {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalUtils.class);

  public static final int DECIMAL_WIDTH = 16; // Decimals stored as 16-byte values
  public static final int OFFSET_LE_MSB = 15;
  public static final int MAX_PRECISION = 38;
  public static final int MIN_REDUCED_SCALE = 6;
  public static final BigInteger MAX_BIG_INT =
      java.math.BigInteger.valueOf(10).pow(MAX_PRECISION).subtract(java.math.BigInteger.ONE);
  public static final BigDecimal MAX_DECIMAL = new java.math.BigDecimal(MAX_BIG_INT, 0);

  public static final BigInteger MIN_BIG_INT = MAX_BIG_INT.multiply(BigInteger.valueOf(-1));
  public static final BigDecimal MIN_DECIMAL = new java.math.BigDecimal(MIN_BIG_INT, 0);
  public static final MathContext MATH = new MathContext(MAX_PRECISION, RoundingMode.UNNECESSARY);

  public static final int LENGTH_OF_LONG = 8;

  public static final String ROUND = "ROUND";
  public static final String TRUNCATE = "TRUNCATE";

  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private DecimalUtils() {}

  /**
   * Read a Decimal value from direct memory at address 'srcAddr'. Requires a temporary buffer, of
   * width DECIMAL_WIDTH
   *
   * @param srcAddr direct memory address where the Decimal value is located
   * @param buf temporary buffer of width DECIMAL_WIDTH
   * @return a java.math.BigDecimal built from the DECIMAL_WIDTH bytes at srcAddr
   */
  public static BigDecimal getBigDecimalFromLEBytes(long srcAddr, byte[] buf, final int scale) {
    // java.math.BigInteger is big endian
    // the Decimal ArrowBuf is little endian
    for (int b = 0; b < DECIMAL_WIDTH; b++) {
      final byte temp = PlatformDependent.getByte(srcAddr + DECIMAL_WIDTH - b - 1);
      buf[b] = temp;
    }
    BigInteger unscaledValue = new BigInteger(buf);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Given a byte array representing a decimal in big endian format Converts it to Arrow Little
   * Endian represenation of length 16 bytes.
   *
   * @param decimalBytesInBigEndian
   * @return byte array representation of decimal in arrow format.
   */
  public static byte[] convertDecimalBytesToArrowByteArray(byte[] decimalBytesInBigEndian) {
    Preconditions.checkArgument(
        decimalBytesInBigEndian.length <= 16, "Decimals of upto 16 bytes " + "are supported.");
    byte padValue = decimalBytesInBigEndian[0] < 0 ? (byte) 255 : (byte) 0;
    byte[] bytesLE = new byte[DECIMAL_WIDTH];

    int i;
    for (i = 0; i < decimalBytesInBigEndian.length; ++i) {
      bytesLE[i] = decimalBytesInBigEndian[decimalBytesInBigEndian.length - i - 1];
    }

    for (i = decimalBytesInBigEndian.length; i < DECIMAL_WIDTH; ++i) {
      bytesLE[i] = padValue;
    }

    return bytesLE;
  }

  public static byte[] convertBigDecimalToArrowByteArray(BigDecimal value) {
    byte[] bytes = value.unscaledValue().toByteArray();
    return convertDecimalBytesToArrowByteArray(bytes);
  }

  /*
   * Compares two decimals represented in the Arrowbuf in little endian
   * format in fixed length of 16 bytes.
   *
   * Assumptions
   * 1. Both values are of same scale.
   * 2. Padding is assumed to make the decimals equal length before
   * calling this method.
   *
   * Returns -ve if arg1< arg2, 0 if equal and +ve if arg1> arg2.
   *
   * Index is starting position of the bytes(and NOT element) in the given arrow buf.
   */
  public static int compareSignedDecimalInLittleEndianBytes(
      ArrowBuf left, int startIndexLeft, ArrowBuf right, int startIndexRight) {
    boolean isNegative1 = left.getByte(startIndexLeft + OFFSET_LE_MSB) < 0;
    boolean isNegative2 = right.getByte(startIndexRight + OFFSET_LE_MSB) < 0;

    // fast path, we are comparing one +ve and one -ve, return
    // appropriately.
    if (isNegative1 != isNegative2) {
      return isNegative1 ? -1 : 1;
    }

    long startingAddress = left.memoryAddress() + (startIndexLeft);
    long newValLow = PlatformDependent.getLong(startingAddress);
    long newValHigh = PlatformDependent.getLong(startingAddress + 8);

    long startingAddressRight = right.memoryAddress() + (startIndexRight);
    long curValLow = PlatformDependent.getLong(startingAddressRight);
    long curValHigh = PlatformDependent.getLong(startingAddressRight + 8);

    return compareDecimalsAsTwoLongs(newValHigh, newValLow, curValHigh, curValLow);
  }

  public static int compareDecimalsAsOneAndTwoLongs(long leftLow, long rightHigh, long rightLow) {
    return compareDecimalsAsTwoLongs(leftLow < 0 ? -1L : 0L, leftLow, rightHigh, rightLow);
  }

  public static int compareDecimalsAsTwoLongs(
      long leftHigh, long leftLow, long rightHigh, long rightLow) {
    boolean isNegative1 = leftHigh < 0;
    boolean isNegative2 = rightHigh < 0;
    if (isNegative1 != isNegative2) {
      return isNegative1 ? -1 : 1;
    }
    return compareUnsigned(leftHigh, leftLow, rightHigh, rightLow);
  }

  public static int compareUnsigned(long leftHigh, long leftLow, long rightHigh, long rightLow) {
    int compare = Long.compareUnsigned(leftHigh, rightHigh);
    if (compare == 0) {
      return Long.compareUnsigned(leftLow, rightLow);
    }
    return compare;
  }

  /**
   * Add two decimals stored in little endian bytes in arrow buffers as unsigned longs.
   *
   * @param left - first agg
   * @param startIndexLeft - starting index in the buffer for the decimal
   * @param right - second arg
   * @param startIndexRight - starting index in the buffer for the decimal
   * @param result - the buffer to store the result at
   * @param startIndexResult - the index to use in the buffer
   */
  public static void addSignedDecimalInLittleEndianBytes(
      ArrowBuf left,
      int startIndexLeft,
      ArrowBuf right,
      int startIndexRight,
      ArrowBuf result,
      int startIndexResult) {
    long startingAddress = left.memoryAddress() + (startIndexLeft);
    long leftValLow = PlatformDependent.getLong(startingAddress);
    long leftValHigh = PlatformDependent.getLong(startingAddress + 8);

    long startingAddressRight = right.memoryAddress() + (startIndexRight);
    long rightValLow = PlatformDependent.getLong(startingAddressRight);
    long rightValHigh = PlatformDependent.getLong(startingAddressRight + 8);

    long resultMemoryAddress = result.memoryAddress() + startIndexResult;
    addSignedDecimals(resultMemoryAddress, leftValLow, leftValHigh, rightValLow, rightValHigh);
  }

  /**
   * Used to add two 16 byte decimals represented as two longs.
   *
   * @param resultMemoryAddress - the memory address to store the result at. will be written as
   *     little endian values
   * @param leftValLow - long constructed using low order bytes of a 16 byte decimal.
   * @param leftValHigh - long constructed using higher order bytes of a 16 byte decimal.
   * @param rightValLow - second arg lower bytes
   * @param rightValHigh - secong arg higher bytes
   */
  public static void addSignedDecimals(
      long resultMemoryAddress,
      long leftValLow,
      long leftValHigh,
      long rightValLow,
      long rightValHigh) {
    boolean isNegative1 = leftValHigh < 0;
    boolean isNegative2 = rightValHigh < 0;

    long lowBits = 0, highBits = 0;
    long leftValLowUnsigned = toUnsigned(leftValLow);
    lowBits = leftValLowUnsigned + toUnsigned(rightValLow);
    highBits = toUnsigned(leftValHigh) + toUnsigned(rightValHigh);
    if (toUnsigned(lowBits) < leftValLowUnsigned) {
      highBits = toUnsigned(highBits) + toUnsigned(1);
    }

    PlatformDependent.putLong(resultMemoryAddress, lowBits);
    PlatformDependent.putLong(resultMemoryAddress + 8, highBits);

    // check for overflow. if sign is different there is no overflow.
    if (isNegative1 == isNegative2) {
      int cmp = compareDecimalsAsTwoLongs(highBits, lowBits, leftValHigh, leftValLow);
      // writing as nested ifs, so that number of branches reduces and might increase
      // cpu branch prediction.
      if (isNegative1) {
        if (cmp > 0) {
          logger.debug(
              "Overflow happened for decimal addition. Max precision is {}", MAX_PRECISION);
        }
      } else {
        if (cmp < 0) {
          logger.debug(
              "Overflow happened for decimal addition. Max precision is {}", MAX_PRECISION);
        }
      }
    }
  }

  private static long toUnsigned(long val) {
    return val + Long.MIN_VALUE;
  }

  /**
   * Gets the appropriate decimal precision for a literal value.
   *
   * @param value input value
   * @return precision to use when representing the value as a decimal.
   */
  public static int getPrecisionForValue(long value) {
    if ((value >= Integer.MIN_VALUE) && (value <= Integer.MAX_VALUE)) {
      return 10;
    } else {
      return 19;
    }
  }

  /**
   * Reads {@code byteWidth} (<= 8) bytes from {@code source} as a big endian value into a {@code
   * long}.
   */
  public static long readBigEndianIntoLong(ArrowBuf source, long sourceOffset, int byteWidth) {
    switch (byteWidth) {
      case 8:
        return Long.reverseBytes(source.getLong(sourceOffset));
      case 7:
        {
          long p0b32 = Integer.reverseBytes(source.getInt(sourceOffset));
          long p1b16 = 0xFFFF & Short.reverseBytes(source.getShort(sourceOffset + 4));
          long p2b8 = 0xFF & source.getByte(sourceOffset + 6);
          return p0b32 << 24 | p1b16 << 8 | p2b8;
        }
      case 6:
        {
          long p0b32 = Integer.reverseBytes(source.getInt(sourceOffset));
          long p1b16 = 0xFFFF & Short.reverseBytes(source.getShort(sourceOffset + 4));
          return p0b32 << 16 | p1b16;
        }
      case 5:
        {
          long p0b32 = Integer.reverseBytes(source.getInt(sourceOffset));
          long p1b8 = 0xFF & source.getByte(sourceOffset + 4);
          return p0b32 << 8 | p1b8;
        }
      case 4:
        return Integer.reverseBytes(source.getInt(sourceOffset));
      case 3:
        {
          long p0b16 = Short.reverseBytes(source.getShort(sourceOffset));
          long p1b8 = 0xFF & source.getByte(sourceOffset + 2);
          return p0b16 << 8 | p1b8;
        }
      case 2:
        return Short.reverseBytes(source.getShort(sourceOffset));
      case 1:
        return source.getByte(sourceOffset);
      default:
        throw new IllegalArgumentException(
            "Unsupported byte width to read into a long: " + byteWidth);
    }
  }

  /**
   * Reads {@code byteWidth} (<= 8) bytes from {@code source} as a big endian value into a {@code
   * long}.
   */
  public static long readBigEndianIntoLong(byte[] source, int sourceOffset, int byteWidth) {
    int offset = sourceOffset;

    // Set the most significant byte with signum prefix (see 2 complement)
    long res = ((long) source[offset++]) << ((byteWidth - 1) * 8);

    for (int bitShift = (byteWidth - 2) * 8; bitShift >= 0; bitShift -= 8) {
      res |= (source[offset++] & 0xFFL) << bitShift;
    }
    return res;
  }

  /** Writes a 128bit decimal value represented as two longs into {@code target}. */
  public static void writeDecimalAsTwoLongs(
      long highValue, long lowValue, DecimalVector target, int targetIndex) {
    // Implemented based on org.apache.arrow.vector.util.DecimalUtility#writeLongToArrowBuf(long,
    // ArrowBuf, int, int)
    BitVectorHelper.setBit(target.getValidityBuffer(), targetIndex);
    long addressOfValue = target.getDataBufferAddress() + targetIndex * DecimalVector.TYPE_WIDTH;
    if (LITTLE_ENDIAN) {
      PlatformDependent.putLong(addressOfValue, lowValue);
      PlatformDependent.putLong(addressOfValue + 8, highValue);
    } else {
      PlatformDependent.putLong(addressOfValue, highValue);
      PlatformDependent.putLong(addressOfValue + 8, lowValue);
    }
  }

  public static double pmod(double a, double b) {
    if (b == 0.0) {
      throw new IllegalArgumentException("Divide by zero error");
    }
    double mod = a % b;

    if (mod < 0 || b < 0) {
      mod += b;
    }
    return mod;
  }
}
