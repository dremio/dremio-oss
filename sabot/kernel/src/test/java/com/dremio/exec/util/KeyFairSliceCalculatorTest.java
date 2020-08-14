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

import static com.dremio.exec.util.TestdataSetupUtils.newHashMap;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests for {@link KeyFairSliceCalculator}
 */
public class KeyFairSliceCalculatorTest {
    @Test (expected = IllegalArgumentException.class)
    public void testMoreKeysThenSupported() {
        new KeyFairSliceCalculator(newHashMap("k1", 1, "k2", 2, "k3", 4, "k4", 4, "k5", 8), 4);
    }

    @Test
    public void testUnderflowSize() {
        KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 4, "k2", 4, "k3", 4), 16);
        assertEquals("Invalid combined key size", 13, keyFairSliceCalculator.getTotalSize());
    }

    @Test
    public void testAllOverflowSizes() {
        KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 12, "k2", 8, "k3", 8, "k4", 6, "k5", 4), 16);
        assertEquals("Invalid combined key size", 16, keyFairSliceCalculator.getTotalSize());

        assertEquals(Integer.valueOf(3), keyFairSliceCalculator.getKeySlice("k1")); // biggest key gets the remainder
        assertEquals(Integer.valueOf(3), keyFairSliceCalculator.getKeySlice("k2"));
        assertEquals(Integer.valueOf(3), keyFairSliceCalculator.getKeySlice("k3"));
        assertEquals(Integer.valueOf(3), keyFairSliceCalculator.getKeySlice("k4"));
        assertEquals(Integer.valueOf(3), keyFairSliceCalculator.getKeySlice("k5"));
    }

    @Test
    public void testMixedOverflow() {
        KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 12, "k2", 8, "k3", 2, "k4", 2), 16);
        assertEquals("Invalid combined key size", 16, keyFairSliceCalculator.getTotalSize());

        assertEquals(Integer.valueOf(6), keyFairSliceCalculator.getKeySlice("k1"));
        assertEquals(Integer.valueOf(5), keyFairSliceCalculator.getKeySlice("k2"));
        assertEquals(Integer.valueOf(2), keyFairSliceCalculator.getKeySlice("k3"));
        assertEquals(Integer.valueOf(2), keyFairSliceCalculator.getKeySlice("k4"));
    }

    @Test
    public void testMixedOverflowWithVariableLengthKeys() {
        // Variable length keys have size = MAX_VALUE
        KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 12, "k2", Integer.MAX_VALUE, "k3", 2, "k4", Integer.MAX_VALUE), 16);
        assertEquals("Invalid combined key size", 16, keyFairSliceCalculator.getTotalSize());

        assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k1"));
        assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k2"));
        assertEquals(Integer.valueOf(2), keyFairSliceCalculator.getKeySlice("k3"));
        assertEquals(Integer.valueOf(5), keyFairSliceCalculator.getKeySlice("k4"));
    }

    @Test
    public void testOrderByKeysForSameSize() {
        // Variable length keys have size = MAX_VALUE
        KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap( "k9", Integer.MAX_VALUE, "k1", Integer.MAX_VALUE, "k4", Integer.MAX_VALUE), 16);
        assertEquals("Invalid combined key size", 16, keyFairSliceCalculator.getTotalSize());

        assertEquals(Integer.valueOf(5), keyFairSliceCalculator.getKeySlice("k1"));
        assertEquals(Integer.valueOf(5), keyFairSliceCalculator.getKeySlice("k4"));
        assertEquals(Integer.valueOf(5), keyFairSliceCalculator.getKeySlice("k9"));
    }

    @Test
    public void testOrderWithOnlyBitKeys() {
      KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", -1, "k2", -1), 16);
      assertEquals("Invalid key size", 1, keyFairSliceCalculator.getTotalSize());

      assertEquals(1, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
    }

    @Test
    public void testOrderWithMixedBitKeys() {
      KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 4, "k2", -1, "k3", Integer.MAX_VALUE), 16);
      assertEquals("Invalid key size", 16, keyFairSliceCalculator.getTotalSize());

      assertEquals(1, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
      assertEquals(Integer.valueOf(11), keyFairSliceCalculator.getKeySlice("k3"));
    }

    @Test
    public void testOrderWithOverflowingValidityRegion() {
      KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 4, "k2", -1, "k3", -1, "k4", -1, "k5", 4, "k6", Integer.MAX_VALUE), 16);
      assertEquals("Invalid key size", 16, keyFairSliceCalculator.getTotalSize());

      assertEquals(2, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k3"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k4"));
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k5"));
      assertEquals(Integer.valueOf(6), keyFairSliceCalculator.getKeySlice("k6"));

      keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", 4, "k2", -1, "k3", -1, "k4", -1, "k5", 4, "k6", 4), 16);
      assertEquals("Invalid key size", 14, keyFairSliceCalculator.getTotalSize());

      assertEquals(2, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k3"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k4"));
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k5"));
      assertEquals(Integer.valueOf(4), keyFairSliceCalculator.getKeySlice("k6"));
    }

    @Test
    public void testOrderWithOnlyBitValuesAndOverflow() {
      KeyFairSliceCalculator keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", -1, "k2", -1, "k3", -1, "k4", -1), 16);
      assertEquals("Invalid key size", 1, keyFairSliceCalculator.getTotalSize());

      assertEquals(1, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k3"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k4"));

      keyFairSliceCalculator = new KeyFairSliceCalculator(newHashMap("k1", -1, "k2", -1, "k3", -1, "k4", -1, "k5", -1), 16);
      assertEquals("Invalid key size", 2, keyFairSliceCalculator.getTotalSize());

      assertEquals(2, keyFairSliceCalculator.numValidityBytes());
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k1"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k2"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k3"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k4"));
      assertEquals(Integer.valueOf(0), keyFairSliceCalculator.getKeySlice("k5"));
    }

}
