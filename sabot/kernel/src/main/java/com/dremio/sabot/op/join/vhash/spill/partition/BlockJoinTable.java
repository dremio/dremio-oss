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
package com.dremio.sabot.op.join.vhash.spill.partition;

import static com.dremio.sabot.op.join.vhash.VectorizedProbe.SKIP;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.NullComparator;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

import io.netty.util.internal.PlatformDependent;

public class BlockJoinTable implements JoinTable {

  private static final int MAX_VALUES_PER_BATCH = 4096;
  private final LBlockHashTable table;
  private final PivotDef buildPivot;
  private final PivotDef probePivot;
  private final NullComparator nullMask;
  private final Stopwatch probeFindWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private boolean tableTracing;

  public BlockJoinTable(PivotDef buildPivot, PivotDef probePivot, BufferAllocator allocator, NullComparator nullMask, int minSize, int varFieldAverageSize) {
    super();
    this.table = new LBlockHashTable(HashConfig.getDefault(), buildPivot, allocator, minSize,
        varFieldAverageSize, false, MAX_VALUES_PER_BATCH);
    this.buildPivot = buildPivot;
    this.probePivot = probePivot;
    this.nullMask = nullMask;
    this.tableTracing = false;
  }

  /* Copy the keys of the records specified in keyOffsetAddr to destination memory
   * keyOffsetAddr contains all the ordinals of keys
   * count is the number of keys
   * keyFixedAddr is the destination memory for fiexed keys
   * keyVarAddr is the destination memory for variable keys
   */
  public void copyKeyToBuffer(final long keyOffsetAddr, final int count, final long keyFixedAddr, final long keyVarAddr) {
    table.copyKeyToBuffer(keyOffsetAddr, count, keyFixedAddr, keyVarAddr);
  }

  // Get the length of the variable keys of the record specified by ordinal in hash table.
  public int getVarKeyLength(int ordinal) {
    return table.getVarKeyLength(ordinal);
  }

  @Override
  public long getProbeFindTime(TimeUnit unit){
    return probeFindWatch.elapsed(unit);
  }

  @Override
  public long getInsertTime(TimeUnit unit){
    return insertWatch.elapsed(unit);
  }

  /**
   * Prepares a bloomfilter from the selective field keys. Since this is an optimisation, errors are not propagated to
   * the consumer. Instead, they get an empty optional.
   * @param fieldNames
   * @param sizeDynamically Size the filter according to the number of entries in table.
   * @return
   */
  @Override
  public Optional<BloomFilter> prepareBloomFilter(List<String> fieldNames, boolean sizeDynamically, int maxKeySize) {
    return table.prepareBloomFilter(fieldNames, sizeDynamically, maxKeySize);
  }

  @Override
  public Optional<ValueListFilter> prepareValueListFilter(String fieldName, int maxElements) {
    return table.prepareValueListFilter(fieldName, maxElements);
  }

  @Override
  public void insertPivoted(long sv2Addr, int records,
                            long tableHashAddr4B, FixedBlockVector fixed, VariableBlockVector variable,
                            long outputAddr) {
    if (tableTracing) {
      table.traceInsertStart(records);
    }

    insertWatch.start();
    final long keyFixedVectorAddr = fixed.getMemoryAddress();
    final long keyVarVectorAddr = variable.getMemoryAddress();
    for (int index = 0 ; index < records; index++, outputAddr += 4) {
      final int ordinal = SV2UnsignedUtil.read(sv2Addr, index);
      final int keyHash = PlatformDependent.getInt(tableHashAddr4B + ordinal * 4);

      PlatformDependent.putInt(outputAddr,
        table.add(keyFixedVectorAddr, keyVarVectorAddr, ordinal, keyHash));
    }
    insertWatch.stop();

    if (tableTracing) {
      table.traceOrdinals(outputAddr, records);
      table.traceInsertEnd();
    }
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public void findPivoted(long sv2Addr, int records, long tableHashAddr4B, FixedBlockVector fixed, VariableBlockVector variable, long outputAddr) {
    final LBlockHashTable table = this.table;
    final int blockWidth = probePivot.getBlockWidth();
    final long keyFixedVectorAddr = fixed.getMemoryAddress();
    final long keyVarVectorAddr = variable.getMemoryAddress();

    probeFindWatch.start();
    final NullComparator compare = nullMask;
    switch(compare.getMode()){
      case NONE:
        for(int index = 0; index < records; index++, outputAddr += 4) {
          final int ordinal = SV2UnsignedUtil.read(sv2Addr, index);
          final int keyHash = PlatformDependent.getInt(tableHashAddr4B + ordinal * 4);
          PlatformDependent.putInt(outputAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, ordinal, keyHash));
        }
        break;

      // 32 bits to consider.
      case FOUR: {
        final int nullMask = compare.getFour();
        for(int index = 0; index < records; index++, outputAddr += 4){
          final int ordinal = SV2UnsignedUtil.read(sv2Addr, index);
          final long bitsAddr = keyFixedVectorAddr + blockWidth * ordinal;
          if((PlatformDependent.getInt(bitsAddr) & nullMask) == nullMask){
            // the nulls are not comparable. as such, this doesn't match.
            final int keyHash = PlatformDependent.getInt(tableHashAddr4B + ordinal * 4);
            PlatformDependent.putInt(outputAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, ordinal, keyHash));
          } else {
            PlatformDependent.putInt(outputAddr, SKIP);
          }
        }
        break;
      }

      // 64 bits to consider.
      case EIGHT: {
        final long nullMask = compare.getEight();
        for(int index = 0; index < records; index++, outputAddr += 4){
          final int ordinal = SV2UnsignedUtil.read(sv2Addr, index);
          final long bitsAddr = keyFixedVectorAddr + blockWidth * ordinal;
          if((PlatformDependent.getLong(bitsAddr) & nullMask) == nullMask){
            final int keyHash = PlatformDependent.getInt(tableHashAddr4B + ordinal * 4);
            PlatformDependent.putInt(outputAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, ordinal, keyHash));
          } else {
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(outputAddr, SKIP);
          }
        }
        break;
      }

      // more than 64 bits to consider.
      case BIG: {
        for(int index = 0; index < records; index++, outputAddr += 4){
          final int ordinal = SV2UnsignedUtil.read(sv2Addr, index);
          long bitsAddr = keyFixedVectorAddr + blockWidth * ordinal;
          if(compare.isComparableBigBits(bitsAddr)){
            final int keyHash = PlatformDependent.getInt(tableHashAddr4B + ordinal * 4);
            PlatformDependent.putInt(outputAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, ordinal, keyHash));
          } else {
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(outputAddr, SKIP);
          }
        }
        break;
      }


      default:
        throw new IllegalStateException();
    }
    probeFindWatch.stop();
  }

  @Override
  public int capacity() {
    return table.capacity();
  }

  @Override
  public int getRehashCount() {
    return table.getRehashCount();
  }

  @Override
  public long getRehashTime(TimeUnit unit) {
    return table.getRehashTime(unit);
  }

  @Override
  public void close() throws Exception {
    table.close();
  }

  @Override
  public AutoCloseable traceStart(int numRecords) {
    tableTracing = true;
    table.traceStart(numRecords);
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        table.traceEnd();
        tableTracing = false;
      }
    };
  }

  @Override
  public String traceReport() {
    return table.traceReport();
  }

}
