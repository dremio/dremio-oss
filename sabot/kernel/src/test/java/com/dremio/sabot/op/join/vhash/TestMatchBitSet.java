/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.join.vhash;

import static org.junit.Assert.assertTrue;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;

/**
 * Unit test for {@link MatchBitSet}
 */
public class TestMatchBitSet extends BaseTestWithAllocator {
  private static final int WORD_BITS = 64;

  private void validateBits(final MatchBitSet matchBitSet, final BitSet bitSet, final int count) {
    assertTrue(matchBitSet.cardinality() == bitSet.cardinality());
    for (int i = 0; i < count; i ++) {
      assertTrue(matchBitSet.get(i) == bitSet.get(i));
      assertTrue(matchBitSet.nextUnSetBit(i) == bitSet.nextClearBit(i));
    }
  }

  @Test
  public void randomBits() throws Exception {
    final int count = 8*1024*1024;
    BitSet bitSet = new BitSet(count);
    final Random rand = new Random();
    try (MatchBitSet matchBitSet = new MatchBitSet(count, allocator)) {
      for (int i = 0; i < count; i ++)
      {

        int val = rand.nextInt(10);
        if (val > 3) {
          bitSet.set(i);
          matchBitSet.set(i);
        }
      }

      validateBits(matchBitSet, bitSet, count);
    }
  }

  @Test
  public void fullBits() throws Exception {
    final int count = 256*1024;
    BitSet bitSet = new BitSet(count);
    try (MatchBitSet matchBitSet = new MatchBitSet(count, allocator)) {
      for (int i = 0; i < count; i++) {
        bitSet.set(i);
        matchBitSet.set(i);
      }

      validateBits(matchBitSet, bitSet, count);
    }
  }

  @Test
  public void specifiedBits() throws Exception {
    final int count = 256*1024 + 13;
    BitSet bitSet = new BitSet(count);
    try (MatchBitSet matchBitSet = new MatchBitSet(count, allocator)) {
      for (int i = 0; i < count; i += WORD_BITS) {
        if ((i / WORD_BITS) % 3 == 0) {
          // all set: T, T, T, T, T, ...
          for (int j = 0; j < WORD_BITS; j++) {
            bitSet.set(i + j);
            matchBitSet.set(i + j);
          }
        } else if ((i / WORD_BITS) % 3 == 1) {
          // every 3rd one set: 0, 3, 6, ..., to T, F, F, T, F, F, T, ...
          for (int j = 0; j < WORD_BITS; j++) {
            if (j % 3 == 0) {
              bitSet.set(i + j);
              matchBitSet.set(i + j);
            }
          }
        } else {
          // all set: F, F, F, F, F, ....
        }
      }

      validateBits(matchBitSet, bitSet, count);
    }
  }
}
