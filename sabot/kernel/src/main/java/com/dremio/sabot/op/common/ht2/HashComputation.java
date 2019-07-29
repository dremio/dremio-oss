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
package com.dremio.sabot.op.common.ht2;

import io.netty.util.internal.PlatformDependent;

public final class HashComputation {
  private static final int EIGHT_BYTES = 8;

  private static final long fixedKeyHashCode(long keyDataAddr, int dataWidth, long seed){
    return mix(XXH64.xxHash64(keyDataAddr, dataWidth, seed));
  }

  private static final long keyHashCode(long keyDataAddr, int dataWidth, final long keyVarAddr,
                                        int varDataLen, long seed){
    final long fixedValue = XXH64.xxHash64(keyDataAddr, dataWidth, seed);
    return mix(XXH64.xxHash64(keyVarAddr + 4, varDataLen, fixedValue));
  }

  public static final void computeHash(final BlockChunk blockChunk) {
    final long keyFixedVectorAddr = blockChunk.keyFixedVectorAddr;
    final long keyVarVectorAddr = blockChunk.keyVarVectorAddr;
    final int blockWidth = blockChunk.blockWidth;
    long hashValueAddress = blockChunk.hashValueVectorAddr;

    long keyVarAddr;
    int keyVarLen;
    long keyHash;
    final int dataWidth;
    final long maxAddress = keyFixedVectorAddr + ((blockChunk.records - 1) * blockWidth);

    /*
     * we can write the loop once but that will require evaluating fixedOnly condition
     * multiple times.
     */
    if (blockChunk.isFixedOnly()) {
      dataWidth = blockWidth;
      for (long blockAddr = keyFixedVectorAddr; blockAddr <= maxAddress; blockAddr += blockWidth) {
        keyHash = fixedKeyHashCode(blockAddr, dataWidth, blockChunk.seed);
        PlatformDependent.putLong(hashValueAddress, keyHash);
        hashValueAddress += EIGHT_BYTES;
      }
    } else {
      dataWidth = blockWidth - LBlockHashTable.VAR_OFFSET_SIZE;
      for (long blockAddr = keyFixedVectorAddr; blockAddr <= maxAddress; blockAddr += blockWidth) {
        keyVarAddr = keyVarVectorAddr + PlatformDependent.getInt(blockAddr + dataWidth);
        keyVarLen = PlatformDependent.getInt(keyVarAddr);
        keyHash = keyHashCode(blockAddr, dataWidth, keyVarAddr, keyVarLen, blockChunk.seed);
        PlatformDependent.putLong(hashValueAddress, keyHash);
        hashValueAddress += EIGHT_BYTES;
      }
    }
  }

  /**
   * Used for hash computation by EightByteInnerLeftProbeOff in Vectorized Hash Join
   * @param hashValueAddress starting address of the buffer that stores the hashvalues.
   * @param srcDataAddr starting address of the source data (keys).
   * @param count number of records
   */
  public static final void computeHash(long hashValueAddress, long srcDataAddr, final int count) {
    final long maxDataAddr = srcDataAddr + (count * EIGHT_BYTES);
    for (long keyAddr = srcDataAddr; keyAddr < maxDataAddr; keyAddr += EIGHT_BYTES, hashValueAddress += EIGHT_BYTES) {
      final long key = PlatformDependent.getLong(keyAddr);
      final long keyHash = computeHash(key);
      PlatformDependent.putLong(hashValueAddress, keyHash);
    }
  }

  /**
   * Computes hash for a 64 bit key.
   * This code is borrowed from LHash.java for
   * extending the implementation to 64 bit hashvalues.
   * @param key input key for the hash computation
   * @return 64 bit hashvalue.
   */
  public static long computeHash(long key) {
    long h = key * -7046029254386353131L;
    h ^= h >> 32;
    return (h ^ h >> 16);
  }

  /**
   * Taken directly from koloboke
   */
  private static long mix(long hash) {
    return (hash & 0x7FFFFFFFFFFFFFFFL);
  }
}
