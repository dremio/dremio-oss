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

package org.apache.arrow.vector;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.memory.ArrowBuf;

import io.netty.buffer.ByteBuf;

public class DecimalHelper {

  public final static int MAX_DIGITS = 9;
  public final static int DIGITS_BASE = 1_000_000_000;
  public final static int INTEGER_SIZE = (Integer.SIZE/Byte.SIZE);
  // we use base 1 billion integer digits for our internal representation
  public final static BigDecimal BASE_BIGDECIMAL = new BigDecimal(DIGITS_BASE);
  public final static BigInteger BASE_BIGINT = BigInteger.valueOf(DIGITS_BASE);

  /**
   * Given an array of bytes, swap the bytes to change the byte order
   * @param bytes array of bytes.
   */
  public static void swapBytes(byte[] bytes) {
    for (int i = 0, j = bytes.length - 1; i < j; i++, j--) {
      final byte temp = bytes[i];
      bytes[i] = bytes[j];
      bytes[j] = temp;
    }
  }

  /**
   * Given an ArrowBuf containing decimal data in BE byte order, construct a BigDecimal
   * from particular position in the source buffer. No need
   * @param buffer source ArrowBuf containing decimal data
   * @param index position of the element (0, 1, 2, 3 ...)
   * @param scale scale of decimal element
   * @return decimal value as BigDecimal
   */
  public static BigDecimal getBigDecimalFromBEArrowBuf(ArrowBuf buffer, int index, int scale) {
    final int length = DecimalVector.TYPE_WIDTH;
    final int startIndex = index * length;
    byte[] value = new byte[length];
    buffer.getBytes(startIndex, value, 0, length);
    BigInteger unscaledValue = new BigInteger(value);
    return new BigDecimal(unscaledValue, scale);
  }

  public static void getSparseFromBigDecimal(BigDecimal input, ByteBuf data, int startIndex,
                                             int scale, int nDecimalDigits) {

    // Initialize the buffer
    for (int i = 0; i < nDecimalDigits; i++) {
      data.setInt(startIndex + (i * INTEGER_SIZE), 0);
    }

    boolean sign = false;

    if (input.signum() == -1) {
      // negative input
      sign = true;
      input = input.abs();
    }

    // Truncate the input as per the scale provided
    input = input.setScale(scale, BigDecimal.ROUND_HALF_UP);

    // Separate out the integer part
    BigDecimal integerPart = input.setScale(0, BigDecimal.ROUND_DOWN);

    int destIndex = nDecimalDigits - roundUp(scale) - 1;

    while (integerPart.compareTo(BigDecimal.ZERO) > 0) {
      // store the modulo as the integer value
      data.setInt(startIndex + (destIndex * INTEGER_SIZE), (integerPart.remainder(BASE_BIGDECIMAL)).intValue());
      destIndex--;
      // Divide by base 1 billion
      integerPart = (integerPart.divide(BASE_BIGDECIMAL)).setScale(0, BigDecimal.ROUND_DOWN);
    }

    /* Sparse representation contains padding of additional zeroes
     * so each digit contains MAX_DIGITS for ease of arithmetic
     */
    int actualDigits = scale % MAX_DIGITS;
    if (actualDigits != 0) {
      // Pad additional zeroes
      scale = scale + (MAX_DIGITS - actualDigits);
      input = input.setScale(scale, BigDecimal.ROUND_DOWN);
    }

    //separate out the fractional part
    BigDecimal fractionalPart = input.remainder(BigDecimal.ONE).movePointRight(scale);

    destIndex = nDecimalDigits - 1;

    while (scale > 0) {
      // Get next set of MAX_DIGITS (9) store it in the DrillBuf
      fractionalPart = fractionalPart.movePointLeft(MAX_DIGITS);
      BigDecimal temp = fractionalPart.remainder(BigDecimal.ONE);

      data.setInt(startIndex + (destIndex * INTEGER_SIZE), (temp.unscaledValue().intValue()));
      destIndex--;

      fractionalPart = fractionalPart.setScale(0, BigDecimal.ROUND_DOWN);
      scale -= MAX_DIGITS;
    }

    // Set the negative sign
    if (sign) {
      data.setInt(startIndex, data.getInt(startIndex) | 0x80000000);
    }
  }

  public static BigDecimal getBigDecimalFromSparse(ByteBuf data, int startIndex, int nDecimalDigits, int scale) {

    // For sparse decimal type we have padded zeroes at the end, strip them while converting to BigDecimal.
    int actualDigits;

    // Initialize the BigDecimal, first digit in the DrillBuf has the sign so mask it out
    BigInteger decimalDigits = BigInteger.valueOf((data.getInt(startIndex)) & 0x7FFFFFFF);

    for (int i = 1; i < nDecimalDigits; i++) {

      BigInteger temp = BigInteger.valueOf(data.getInt(startIndex + (i * INTEGER_SIZE)));
      decimalDigits = decimalDigits.multiply(BASE_BIGINT);
      decimalDigits = decimalDigits.add(temp);
    }

    // Truncate any additional padding we might have added
    if (scale > 0 && (actualDigits = scale % MAX_DIGITS) != 0) {
      BigInteger truncate = BigInteger.valueOf((int)Math.pow(10, (MAX_DIGITS - actualDigits)));
      decimalDigits = decimalDigits.divide(truncate);
    }

    // set the sign
    if ((data.getInt(startIndex) & 0x80000000) != 0) {
      decimalDigits = decimalDigits.negate();
    }

    BigDecimal decimal = new BigDecimal(decimalDigits, scale);

    return decimal;
  }

  /* Given the number of actual digits this function returns the
   * number of indexes it will occupy in the array of integers
   * which are stored in base 1 billion
   */
  public static int roundUp(int ndigits) {
    return (ndigits + MAX_DIGITS - 1)/MAX_DIGITS;
  }
}
