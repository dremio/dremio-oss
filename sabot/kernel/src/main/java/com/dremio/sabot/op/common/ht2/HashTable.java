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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionManager;
import com.koloboke.collect.hash.HashConfig;

public interface HashTable {
  Logger logger = LoggerFactory.getLogger(HashTable.class);

  String FACTORY_KEY = "dremio.ht2.implementation.factory"; // Replacing class with factory to keep with our current pattern...

  static HashTable getInstance(SabotConfig sabotConfig, OptionManager optionsManager, HashTableCreateArgs createArgs) {
    HashTableFactory factory = sabotConfig.getInstance(FACTORY_KEY, HashTableFactory.class, LBlockHashTableFactory.class);
    return factory.getInstance(optionsManager, createArgs);
  }

  void computeHash(int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar, long seed, ArrowBuf hashOut8B);

  int add(int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar, ArrowBuf hash8B, ArrowBuf outOrdinals);

  void find(int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar, ArrowBuf hash8B, ArrowBuf outOrdinals);

  int addSv2(ArrowBuf sv2, int pivotShift, int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar, ArrowBuf hash4B, ArrowBuf outOrdinals);

  void findSv2(ArrowBuf sv2, int pivotShift, int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar, ArrowBuf hash4B, ArrowBuf outOrdinals);

  void copyKeysToBuffer(ArrowBuf ordinals, int numRecords, ArrowBuf keyFixed, ArrowBuf keyVar);

  int getCumulativeVarKeyLength(ArrowBuf ordinals, int numRecords);

  void getVarKeyLengths(ArrowBuf ordinals, int numRecords, ArrowBuf outLengths);

  int getMaxOrdinal();

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

  Iterator<HashTableKeyAddress> keyIterator();

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
    private final boolean runtimeFilterEnabled;

    public HashTableCreateArgs(HashConfig hashConfig,
                               PivotDef pivot,
                               BufferAllocator allocator,
                               int initialSize,
                               int defaultVarLengthSize,
                               boolean enforceVarWidthBufferLimit,
                               int maxHashTableBatchSize,
                               NullComparator nullComparator,
                               boolean runtimeFilterEnabled) {
      this.hashConfig = hashConfig;
      this.pivot = pivot;
      this.allocator = allocator;
      this.initialSize = initialSize;
      this.defaultVarLengthSize = defaultVarLengthSize;
      this.enforceVarWidthBufferLimit = enforceVarWidthBufferLimit;
      this.maxHashTableBatchSize = maxHashTableBatchSize;
      this.nullComparator = nullComparator;
      this.runtimeFilterEnabled = runtimeFilterEnabled;
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

    public boolean isRuntimeFilterEnabled() {
      return runtimeFilterEnabled;
    }
  }
}
