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

/**
 * This class represents the information required to do the hash computation
 * for vectorized hash agg and hash join operators when inserting the pivoted
 * data into the hash table. It encapsulates the information about pivot space,
 * target address for storing the hashvalues (since we compute hash for entire
 * batch at one go), seed for hash. Seed is needed here since with spilling
 * feature the operator controls the hashing (and partitioning) so it decides
 * the appropriate seed.
 */
public final class BlockChunk {
  protected final long keyFixedVectorAddr;
  protected final long keyVarVectorAddr;
  protected final long keyVarVectorSize;
  protected final boolean fixedOnly;
  protected final int blockWidth;
  protected final int records;
  protected final long hashValueVectorAddr;
  protected final long seed;


  public BlockChunk(long keyFixedVectorAddr, long keyVarVectorAddr, long keyVarVectorSize, boolean fixedOnly,
                    int blockWidth, int records, long hashValueVectorAddr, long seed) {
    this.keyFixedVectorAddr = keyFixedVectorAddr;
    this.keyVarVectorAddr = keyVarVectorAddr;
    this.keyVarVectorSize = keyVarVectorSize;
    this.fixedOnly = fixedOnly;
    this.blockWidth = blockWidth;
    this.records = records;
    this.hashValueVectorAddr = hashValueVectorAddr;
    this.seed = seed;
  }

  public long getKeyFixedVectorAddr() {
    return keyFixedVectorAddr;
  }

  public long getKeyVarVectorAddr() {
    return keyVarVectorAddr;
  }

  public long getKeyVarVectorSize() {
    return keyVarVectorSize;
  }

  public boolean isFixedOnly() {
    return fixedOnly;
  }

  public int getBlockWidth() {
    return blockWidth;
  }

  public int getNumRecords() {
    return records;
  }

  public long getHashValueVectorAddr() {
    return hashValueVectorAddr;
  }

  public long getSeed() {
    return seed;
  }
}
