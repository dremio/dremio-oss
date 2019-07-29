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
package com.dremio.sabot.op.join.vhash;

import org.apache.arrow.memory.BufferAllocator;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Implemented to support direct memory for BitSet, which is used for key match bit set.
 */
public class MatchBitSet implements AutoCloseable {

  private final static int LONG_TO_BITS_SHIFT = 6;
  private final static int BIT_OFFSET_MUSK = (1 << LONG_TO_BITS_SHIFT) - 1;
  private final static int BYTES_PER_WORD = 8;

  private final BufferAllocator allocator;
  // A buffer allocated by allocator to store the bits
  private final ArrowBuf buffer;
  // The memory address of buffer
  private final long bufferAddr;
  // The number of bits
  private final int numBits;
  // The number of words, whose length is 8 bytes.
  private final int numWords;

  /**
   * Creates MatchBitSet, allocates the buffer and initialized values to 0
   * @param numBits       the number of bits
   * @param allocator     the allocator used to allocate direct memory
   */
  public MatchBitSet(final int numBits, final BufferAllocator allocator) {
    Preconditions.checkArgument(numBits >= 0, "bits count in constructor of BitSet is invalid");

    this.allocator = allocator;
    this.numBits = numBits;
    numWords = bitsToWords(this.numBits);
    buffer = allocator.buffer(numWords * BYTES_PER_WORD);
    bufferAddr = buffer.memoryAddress();
    long maxBufferAddr = bufferAddr + numWords * BYTES_PER_WORD;
    for (long wordAddr = bufferAddr; wordAddr < maxBufferAddr; wordAddr += BYTES_PER_WORD) {
      PlatformDependent.putLong(wordAddr, 0);
    }
  }

  /**
   * Calculates the number of words that is needed to store the bits.
   * @param numBits   the number of bits
   * @return          the number of words
   */
  private static int bitsToWords(final int numBits) {
    int numLong = numBits >>> LONG_TO_BITS_SHIFT;
    if ((numBits & BIT_OFFSET_MUSK) != 0)
    {
      numLong++;
    }
    return numLong;
  }

  /**
   * Set the bit specified by index.
   * @param index   the index of the bit
   */
  public void set(final int index) {
    final int wordNum = index >>> LONG_TO_BITS_SHIFT;
    final int bit = index & BIT_OFFSET_MUSK;
    final long bitMask = 1L << bit;

    final long wordAddr = bufferAddr + wordNum * BYTES_PER_WORD;
    PlatformDependent.putLong(wordAddr, PlatformDependent.getLong(wordAddr) | bitMask);
  }

  /**
   * Get the bit specified by index.
   * @param index   the index of the bit
   * @return        true if the bit is set
   *                false if the bit is not set
   */
  public boolean get(final int index) {
    final int wordNum = index >>> LONG_TO_BITS_SHIFT;
    final int bit = index & BIT_OFFSET_MUSK;
    final long bitmask = 1L << bit;
    return (PlatformDependent.getLong(bufferAddr + wordNum * BYTES_PER_WORD) & bitmask) != 0;
  }

  /**
   * Get the index of the bit that is not set.
   * The index is the specified starting index or after it.
   * @param index     the starting index of bits
   * @return          the index of the bit that is not set
   *                  >= numBits if all left bits are set
   */
  public int nextUnSetBit(final int index) {
    final int wordNum = index >>> LONG_TO_BITS_SHIFT;
    final int bit = index & BIT_OFFSET_MUSK;

    long wordAddr = bufferAddr + wordNum * BYTES_PER_WORD;
    long word = ~PlatformDependent.getLong(wordAddr) >> bit;
    if (word != 0)
    {
      return index + Long.numberOfTrailingZeros(word);
    }

    final long maxAddr = bufferAddr + numWords * BYTES_PER_WORD;
    for (wordAddr += BYTES_PER_WORD; wordAddr < maxAddr; wordAddr += BYTES_PER_WORD) {
      word = ~PlatformDependent.getLong(wordAddr);
      if (word != 0)
      {
        return (int)(wordAddr - bufferAddr) * 8 + Long.numberOfTrailingZeros(word);
      }
    }

    return numWords << LONG_TO_BITS_SHIFT;
  }

  /**
   * Get the count of bits that are set.
   * @return    the count of bits that are set
   */
  public int cardinality() {
    int sum = 0;
    final long maxBufferAddr = bufferAddr + numWords * BYTES_PER_WORD;
    for (long wordAddr = bufferAddr; wordAddr < maxBufferAddr; wordAddr += BYTES_PER_WORD) {
      sum += Long.bitCount(PlatformDependent.getLong(wordAddr));
    }
    return sum;
  }

  @Override
  public void close() throws Exception {
    buffer.close();
  }
}
