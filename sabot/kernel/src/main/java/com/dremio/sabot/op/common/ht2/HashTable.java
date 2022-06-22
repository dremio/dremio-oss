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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.util.CloseableIterator;
import com.google.common.base.Preconditions;
import com.koloboke.collect.hash.HashConfig;

public interface HashTable {
  String NATIVE_HASHTABLE_CLASS = "dremio.joust.NativeHashTable.class";
  String LBLOCK_HASHTABLE_CLASS = "dremio.ht2.LBlockHashTable.class";

  static HashTable getInstance(SabotConfig sabotConfig, boolean useNative, HashTableCreateArgs createArgs) {
    if (useNative && sabotConfig.hasPath(NATIVE_HASHTABLE_CLASS)) {
      return sabotConfig.getInstance(NATIVE_HASHTABLE_CLASS, HashTable.class, createArgs);
    } else {
      Preconditions.checkArgument(sabotConfig.hasPath(LBLOCK_HASHTABLE_CLASS));
      return sabotConfig.getInstance(LBLOCK_HASHTABLE_CLASS, HashTable.class, createArgs);
    }
  }

  void computeHash(int numRecords, long keyFixedVectorAddr, long keyVarVectorAddr, long seed, long hashOutAddr8B);

  int add(int numRecords, long keyFixedVectorAddr, long keyVarVectorAddr, long hashVectorAddr8B, long ordinalOutAddr);

  void find(int numRecords, long keyFixedVectorAddr, long keyVarVectorAddr, long hashVectorAddr8B, long ordinalOutAddr);

  int addSv2(int numRecords, long sv2Addr, long keyFixedVectorAddr, long keyVarVectorAddr, long hashVectorAddr4B, long ordinalOutAddr);

  void findSv2(int numRecords, long sv2Addr, long keyFixedVectorAddr, long keyVarVectorAddr, long hashVectorAddr4B, long ordinalOutAddr);

  void copyKeysToBuffer(long keyOffsetAddr, int numRecords, long keyFixedAddr, long keyVarAddr);

  int getCumulativeVarKeyLength(long offsetVectorAddr, int numRecords);

  void getVarKeyLengths(long keyOffsetAddr, int numRecords, long outAddr);

  /* Size, for historical reasons, is the current ordinal (i.e total number of records + gaps) */
  int size();

  int capacity();

  int getRehashCount();

  long getRehashTime(TimeUnit timeUnit);

  class HashTableKeyAddress {
    private final long fixedKeyAddress;
    private final long varKeyAddress; /* 0, if no variable length columns present */

    public HashTableKeyAddress(long fixedKeyAddress, long varKeyAddress) {
      this.fixedKeyAddress = fixedKeyAddress;
      this.varKeyAddress = varKeyAddress;
    }

    public long getFixedKeyAddress() {
        return fixedKeyAddress;
      }

    public long getVarKeyAddress() {
        return varKeyAddress;
      }
  }

  CloseableIterator<HashTableKeyAddress> keyIterator();

  /* Used in API testing */
  long[] getDataPageAddresses();

  void close() throws Exception;

  /* XXX: Until tracing is supported by NativeHashTable, i.e DX-42630 */
  default void traceStart(int numRecords) {}

  default void traceEnd() {}

  default void traceInsertStart(int numRecords) {}

  default void traceInsertEnd() {}

  default void traceOrdinals(long outputAddr, int numRecords) {}

  default String traceReport() {
    return "";
  }

  /**
   * Args that passed to SabotConfig to create an instance.
   */
  class HashTableCreateArgs {
    private final HashConfig hashConfig;
    private final PivotDef pivot;
    private final BufferAllocator allocator;
    private final int initialSize;
    private final int defaultVarLengthSize;
    private final boolean enforceVarWidthBufferLimit;
    private final int maxHashTableBatchSize;
    private final NullComparator nullComparator;

    public HashTableCreateArgs(HashConfig hashConfig,
                               PivotDef pivot,
                               BufferAllocator allocator,
                               int initialSize,
                               int defaultVarLengthSize,
                               boolean enforceVarWidthBufferLimit,
                               int maxHashTableBatchSize,
                               NullComparator nullComparator) {
      this.hashConfig = hashConfig;
      this.pivot = pivot;
      this.allocator = allocator;
      this.initialSize = initialSize;
      this.defaultVarLengthSize = defaultVarLengthSize;
      this.enforceVarWidthBufferLimit = enforceVarWidthBufferLimit;
      this.maxHashTableBatchSize = maxHashTableBatchSize;
      this.nullComparator = nullComparator;
    }

    public HashConfig getHashConfig() {
      return hashConfig;
    }

    public PivotDef getPivot() {
      return pivot;
    }

    public BufferAllocator getAllocator() {
      return allocator;
    }

    public int getInitialSize() {
      return initialSize;
    }

    public int getDefaultVarLengthSize() {
      return defaultVarLengthSize;
    }

    public boolean isEnforceVarWidthBufferLimit() {
      return enforceVarWidthBufferLimit;
    }

    public int getMaxHashTableBatchSize() {
      return maxHashTableBatchSize;
    }

    public NullComparator getNullComparator() {
      return nullComparator;
    }
  }
}
