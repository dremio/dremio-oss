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
package com.dremio.sabot.op.join.vhash.spill;

import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.BloomFilter;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

import io.netty.util.internal.PlatformDependent;

public class EightByteInnerLeftProbeOff implements JoinTable {
  private static final Logger logger = LoggerFactory.getLogger(EightByteInnerLeftProbeOff.class);

  public static int FOUR_BYTE = 4;
  public static int EIGHT_BYTE = 8;
  public static final int WORD_BITS = 64;
  public static final int WORD_BYTES = 8;
  private static final long ALL_SET = 0xFFFFFFFFFFFFFFFFL;
  private static final long NONE_SET = 0;
  // Two null keys are equal or not, used to support IS_NOT_DISTINCT_FROM when it's true
  private final boolean isEqualForNullKey;

  private final LBlockHashTableEight map;
  private final FieldVector probe;
  private final FieldVector build;
  private final Stopwatch findWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private final BufferAllocator allocator;
  private final Stopwatch buildHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeHashComputationWatch = Stopwatch.createUnstarted();

  public EightByteInnerLeftProbeOff(BufferAllocator allocator, int initialSize, PivotDef probeDef, PivotDef buildDef, boolean isEqualForNullKey){
    Preconditions.checkArgument(probeDef.getFixedPivots().size() == 1);
    Preconditions.checkArgument(buildDef.getFixedPivots().size() == 1);
    this.allocator = allocator;
    this.probe = probeDef.getFixedPivots().get(0).getIncomingVector();
    this.build = buildDef.getFixedPivots().get(0).getIncomingVector();
    this.map = new LBlockHashTableEight(HashConfig.getDefault(), allocator, initialSize);
    this.isEqualForNullKey = isEqualForNullKey;
  }

  @Override
  public void insert(long outputAddr, int count) {
    insertWatch.start();

    long srcBitsAddr = build.getValidityBufferAddress();
    long srcDataAddr = build.getDataBufferAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);

    try(SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
      hashValues.allocateNew(count);
      final long hashValueVectorAddr = hashValues.getBufferAddress();

      /* populate the hash value vector with hashvalues for entire build batch */
      buildHashComputationWatch.start();
      HashComputation.computeHash(hashValueVectorAddr, srcDataAddr, count);
      buildHashComputationWatch.stop();

      /* decode word at a time, a single iteration of this loop handles
       * 64 words (8 byte each key) corresponding to a single word
       * from the validity buffer.
       * however, the hash computation is done above for the entire batch
       * of keys. it is probably worth considering if we should do the
       * hash computation at the same level (64 words at a time).
       * this way we need to allocate a hashvalues vector for 64 values only
       * and not the entire batch. secondly, we need not compute garbage hash
       * for the NONE_SET case where the validity buffer word is all zeros.
       * from the cache locality point of view, doesn't matter which way
       * we do since in either case we are filling a cache line
       * with 8 keys.
       * computing hashvalues upfront does seem to benefit from
       * temporal locality w.r.t OS paging.
       */
      long hashValueAddress = hashValueVectorAddr; /* start from first hashvalue */
      while (srcDataAddr < finalWordAddr) {
        // get the validity buffer word corresponding to all 64 keys.
        final long bitValues = PlatformDependent.getLong(srcBitsAddr);
        if (bitValues == NONE_SET) {
          // CASE 1: noop (all nulls)
          srcDataAddr += (WORD_BITS * EIGHT_BYTE);
          // skip corresponding hashvalues
          hashValueAddress += (WORD_BITS * EIGHT_BYTE);
          for (int i = 0; i < WORD_BITS; i++, outputAddr += FOUR_BYTE) {
            // for null key, call map.insertNull to insert null key,
            //it will return the ordinal of existing null key if null key already inserted in hash table.
            PlatformDependent.putInt(outputAddr, map.insertNull());
          }
        } else if (bitValues == ALL_SET) {
          // CASE 2: all set, skip individual checks.
          for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
            PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr), keyHash));
            hashValueAddress += EIGHT_BYTE;
          }
        } else {
          // CASE 3: some nulls, some not, update each value to zero or the value, depending on the null bit.
          for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int bitVal = ((int) (bitValues >>> i)) & 1;
            if (bitVal == 1) {
              final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
              PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr), keyHash));
            } else {
              // for null key, call map.insertNull to insert null key,
              // it will return the ordinal of existing null key if null key already inserted in hash table.
              PlatformDependent.putInt(outputAddr, map.insertNull());
            }
            hashValueAddress += EIGHT_BYTE;
          }
        }
        srcBitsAddr += WORD_BYTES;
      }

      // do the remaining bits..
      if (remainCount > 0) {
        final long bitValues = PlatformDependent.getLong(srcBitsAddr);
        if (bitValues == NONE_SET) {
          // noop (all nulls).
          for (int i = 0; i < remainCount; i++, outputAddr += FOUR_BYTE) {
            // for null key, call map.insertNull to insert null key,
            // it will return the ordinal of existing null key if null key already inserted in hash table.
            PlatformDependent.putInt(outputAddr, map.insertNull());
          }
        } else if (bitValues == ALL_SET) {
          // all set,
          for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
            PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr), keyHash));
            hashValueAddress += EIGHT_BYTE;
          }
        } else {
          // some nulls,
          for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int bitVal = ((int) (bitValues >>> i)) & 1;
            if (bitVal == 1) {
              final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
              PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr), keyHash));
            } else {
              // for null key, call map.insertNull to insert null key,
              //it will return the ordinal of existing null key if null key already inserted in hash table.
              PlatformDependent.putInt(outputAddr, map.insertNull());
            }
            hashValueAddress += EIGHT_BYTE;
          }
        }
      }
      insertWatch.stop();
    }
  }

  @Override
  public void find(long outputAddr, final int count) {
    findWatch.start();

    long srcBitsAddr = probe.getValidityBufferAddress();
    long srcDataAddr = probe.getDataBufferAddress();
    boolean isEqualForNullKey = this.isEqualForNullKey;

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);

    try(SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
      hashValues.allocateNew(count);
      final long hashValueVectorAddr = hashValues.getBufferAddress();

      /* populate the hash value vector with hashvalues for entire probe batch */
      probeHashComputationWatch.start();
      HashComputation.computeHash(hashValueVectorAddr, srcDataAddr, count);
      probeHashComputationWatch.stop();

      // decode word at a time.
      long hashValueAddress = hashValueVectorAddr;
      while (srcDataAddr < finalWordAddr) {
        final long bitValues = PlatformDependent.getLong(srcBitsAddr);
        if (bitValues == NONE_SET) {
          // noop (all nulls).
          srcDataAddr += (WORD_BITS * EIGHT_BYTE);
          // skip corresponding hashvalues
          hashValueAddress += (WORD_BITS * EIGHT_BYTE);
          // if null keys are equal, get the ordinal of null key in hash table, otherwise set to NO_MATCH.
          final int nullKeyId = isEqualForNullKey ? map.getNull() : LBlockHashTableEight.NO_MATCH;
          for (int i = 0; i < WORD_BITS; i++, outputAddr += FOUR_BYTE) {
            PlatformDependent.putInt(outputAddr, nullKeyId);
          }
        } else if (bitValues == ALL_SET) {
          // all set, skip individual checks.
          for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
            PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr), keyHash));
            hashValueAddress += EIGHT_BYTE;
          }
        } else {
          // some nulls, some not, update each value to zero or the value, depending on the null bit.
          // if null keys are equal, get the ordinal of null key in hash table, otherwise set to NO_MATCH.
          final int nullKeyId = isEqualForNullKey ? map.getNull() : LBlockHashTableEight.NO_MATCH;
          for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int bitVal = ((int) (bitValues >>> i)) & 1;
            if(bitVal == 1){
              final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
              PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr), keyHash));
            } else {
              PlatformDependent.putInt(outputAddr, nullKeyId);
            }
            hashValueAddress += EIGHT_BYTE;
          }
        }
        srcBitsAddr += WORD_BYTES;
      }

      // do the remaining bits..
      if(remainCount > 0) {
        final long bitValues = PlatformDependent.getLong(srcBitsAddr);
        if (bitValues == NONE_SET) {
          // noop (all nulls).
          // if null keys are equal, get the ordinal of null key in hash table, otherwise set to NO_MATCH.
          final int nullKeyId = isEqualForNullKey ? map.getNull() : LBlockHashTableEight.NO_MATCH;
          for (int i = 0; i < remainCount; i++, outputAddr += FOUR_BYTE) {
            PlatformDependent.putInt(outputAddr, nullKeyId);
          }
        } else if (bitValues == ALL_SET) {
          // all set,
          for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
            PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr), keyHash));
            hashValueAddress += EIGHT_BYTE;
          }
        } else {
          // some nulls,
          // if null keys are equal, get the ordinal of null key in hash table, otherwise set to NO_MATCH.
          final int nullKeyId = isEqualForNullKey ? map.getNull() : LBlockHashTableEight.NO_MATCH;
          for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
            final int bitVal = ((int) (bitValues >>> i)) & 1;
            if(bitVal == 1){
              final int keyHash = (int) PlatformDependent.getLong(hashValueAddress);
              PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr), keyHash));
            } else {
              PlatformDependent.putInt(outputAddr, nullKeyId);
            }
            hashValueAddress += EIGHT_BYTE;
          }
        }
      }

      findWatch.stop();
    }
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public int capacity() {
    return map.capacity();
  }

  @Override
  public int getRehashCount() {
    return map.getRehashCount();
  }

  @Override
  public long getRehashTime(TimeUnit unit) {
    return map.getRehashTime(unit);
  }

  @Override
  public long getProbePivotTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getProbeFindTime(TimeUnit unit) {
    return findWatch.elapsed(unit);
  }

  @Override
  public long getBuildPivotTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getInsertTime(TimeUnit unit) {
    return insertWatch.elapsed(unit);
  }

  @Override
  public long getBuildHashComputationTime(TimeUnit unit){
    return buildHashComputationWatch.elapsed(unit);
  }

  @Override
  public long getProbeHashComputationTime(TimeUnit unit){
    return probeHashComputationWatch.elapsed(unit);
  }

  @Override
  public void close() throws Exception {
    map.close();
  }

  @Override
  public AutoCloseable traceStart(int numRecords) {
    return AutoCloseables.noop();
  }

  @Override
  public String traceReport() {
    return "";
  }

  @Override
  public Optional<BloomFilter> prepareBloomFilter(List<String> fieldNames, boolean sizeDynamically, int maxKeySize) {
    try {
      if (CollectionUtils.isEmpty(fieldNames)) {
        return Optional.empty();
      }
      checkArgument(fieldNames.size() == 1, "VECTORIZED_BIGINT mode supports only a single field of type bigint. Found more - {}.", fieldNames);
      if (!fieldNames.get(0).equalsIgnoreCase(build.getName())) {
        logger.debug("The required field name {} is not available in the build pivot fields {}. Skipping the filter.", fieldNames.get(0), build.getName());
        return Optional.empty();
      }

      return map.prepareBloomFilter(sizeDynamically);
    } catch (Exception e) {
      logger.warn("Error while creating bloomfilter for " + fieldNames, e);
      return Optional.empty();
    }
  }
}
