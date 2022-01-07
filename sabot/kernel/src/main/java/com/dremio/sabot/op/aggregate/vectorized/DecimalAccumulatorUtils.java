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
package com.dremio.sabot.op.aggregate.vectorized;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for the accumulators that operate over decimal values
 */
public final class DecimalAccumulatorUtils {
  public static final int DECIMAL_WIDTH = 16;  // Decimals stored as 16-byte values

  private DecimalAccumulatorUtils() {}

  /**
   * Read a Decimal value from direct memory at address 'srcAddr'.
   * Requires a temporary buffer, of width DECIMAL_WIDTH
   * @param srcAddr direct memory address where the Decimal value is located
   * @param buf temporary buffer of width DECIMAL_WIDTH
   * @return a java.math.BigDecimal built from the DECIMAL_WIDTH bytes at srcAddr
   */
  public static BigDecimal getBigDecimal(long srcAddr, byte[] buf, final int scale) {
    PlatformDependent.copyMemory(srcAddr, buf, 0, DECIMAL_WIDTH);
    // java.math.BigInteger is big endian
    // the Decimal ArrowBuf is little endian
    for (int b = 0; b < DECIMAL_WIDTH / 2; b++) {
      final byte temp = buf[b];
      buf[b] = buf[DECIMAL_WIDTH - b - 1];
      buf[DECIMAL_WIDTH - b - 1] = temp;
    }
    BigInteger unscaledValue = new BigInteger(buf);
    return new BigDecimal(unscaledValue, scale);
  }
}
