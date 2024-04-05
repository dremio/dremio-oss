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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;

public class TestBitSetHelper {

  @Test
  public void testSetBitPositionsCorrectness() {
    for (int idx = 0; idx < BitSetHelper.SIZE; idx++) {
      // Get the byte value
      byte byteVal = (byte) idx;
      // Construct the byte value from set indices
      int byteValConstructedFromSetBits = 0;
      List<Integer> setBits = BitSetHelper.getSetBitPositions(byteVal);
      // Ensure elements in setBits are in sorted order
      int prevBitPosition = -1;
      for (int bitPosition : setBits) {
        assertTrue("Bits positions are not in sorted order", bitPosition > prevBitPosition);
        byteValConstructedFromSetBits += Math.pow(2, bitPosition);
        prevBitPosition = bitPosition;
      }
      // Test if both are equal
      assertEquals(Byte.toUnsignedInt(byteVal), byteValConstructedFromSetBits);
    }
  }

  @Test
  public void testUnsetBitPositionsCorrectness() {
    for (int idx = 0; idx < BitSetHelper.SIZE; idx++) {
      // Get the byte value
      byte byteVal = (byte) idx;
      // Construct the byte value from unset indices
      int byteValConstructedFromUnSetBits = 255;
      List<Integer> unSetBits = BitSetHelper.getUnsetBitPositions(byteVal);
      // Ensure elements in setBits are in sorted order
      int prevBitPosition = -1;
      for (int bitPosition : unSetBits) {
        assertTrue("Bits positions are not in sorted order", bitPosition > prevBitPosition);
        byteValConstructedFromUnSetBits -= Math.pow(2, bitPosition);
        prevBitPosition = bitPosition;
      }
      // Test if both are equal
      assertEquals(Byte.toUnsignedInt(byteVal), byteValConstructedFromUnSetBits);
    }
  }
}
