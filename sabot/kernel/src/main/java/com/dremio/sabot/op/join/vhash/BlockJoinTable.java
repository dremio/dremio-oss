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

import static com.dremio.sabot.op.join.vhash.VectorizedProbe.SKIP;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SimpleBigIntVector;

import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.sabot.op.common.ht2.BlockChunk;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

import io.netty.util.internal.PlatformDependent;

public class BlockJoinTable implements JoinTable {

  private static final int MAX_VALUES_PER_BATCH = 4096;
  private LBlockHashTable table;
  private PivotDef buildPivot;
  private PivotDef probePivot;
  private BufferAllocator allocator;
  private NullComparator nullMask;
  private final Stopwatch probePivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeFindWatch = Stopwatch.createUnstarted();
  private final Stopwatch pivotBuildWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private boolean tableTracing;
  private final Stopwatch buildHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeHashComputationWatch = Stopwatch.createUnstarted();
  private boolean fixedOnly;

  public BlockJoinTable(PivotDef buildPivot, PivotDef probePivot, BufferAllocator allocator, NullComparator nullMask, int minSize, int varFieldAverageSize) {
    super();
    this.table = new LBlockHashTable(HashConfig.getDefault(), buildPivot, allocator, minSize,
      varFieldAverageSize, false, MAX_VALUES_PER_BATCH);
    this.buildPivot = buildPivot;
    this.probePivot = probePivot;
    this.allocator = allocator;
    this.nullMask = nullMask;
    this.tableTracing = false;
    this.fixedOnly = buildPivot.getVariableCount() == 0;
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
  public long getProbePivotTime(TimeUnit unit){
    return probePivotWatch.elapsed(unit);
  }

  @Override
  public long getProbeFindTime(TimeUnit unit){
    return probeFindWatch.elapsed(unit);
  }

  @Override
  public long getBuildPivotTime(TimeUnit unit){
    return pivotBuildWatch.elapsed(unit);
  }

  @Override
  public long getInsertTime(TimeUnit unit){
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
  public void insert(long findAddr, int records) {
    try(FixedBlockVector fbv = new FixedBlockVector(allocator, buildPivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, buildPivot.getVariableCount());
        ){
      // STEP 1: first we pivot.
      pivotBuildWatch.start();
      Pivots.pivot(buildPivot, records, fbv, var);
      pivotBuildWatch.stop();
      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      final boolean fixedOnly = this.fixedOnly;

      if (tableTracing) {
        table.traceInsertStart(records);
      }

      try(SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)){
        // STEP 2: then we do the hash computation on entire batch
        hashValues.allocateNew(records);
        final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
          buildPivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
        buildHashComputationWatch.start();
        HashComputation.computeHash(blockChunk);
        buildHashComputationWatch.stop();

        // STEP 3: then we insert build side into hash table
        insertWatch.start();
        for(int keyIndex = 0 ; keyIndex < records; keyIndex++, findAddr += 4) {
          final int keyHash = (int)hashValues.get(keyIndex);
          PlatformDependent.putInt(findAddr, table.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
        }
        insertWatch.stop();
      }

      if (tableTracing) {
        table.traceOrdinals(findAddr, records);
        table.traceInsertEnd();
      }
    }
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public void find(long offsetAddr, final int records) {
    final LBlockHashTable table = this.table;
    final int blockWidth = probePivot.getBlockWidth();
    try(FixedBlockVector fbv = new FixedBlockVector(allocator, probePivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, probePivot.getVariableCount());
        SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)){
      // STEP 1: first we pivot.
      probePivotWatch.start();
      Pivots.pivot(probePivot, records, fbv, var);
      probePivotWatch.stop();
      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      final boolean fixedOnly = this.fixedOnly;

      // STEP 2: then we do the hash computation on entire batch
      hashValues.allocateNew(records);
      probeHashComputationWatch.start();
      final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
        buildPivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
      HashComputation.computeHash(blockChunk);
      probeHashComputationWatch.stop();

      // STEP 3: then we probe hash table.
      probeFindWatch.start();
      final NullComparator compare = nullMask;
      switch(compare.getMode()){
      case NONE:
        for(int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += 4) {
          final int keyHash = (int)hashValues.get(keyIndex);
          PlatformDependent.putInt(offsetAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
        }
        break;

      // 32 bits to consider.
      case FOUR: {
        long bitsAddr = keyFixedVectorAddr;
        final int nullMask = compare.getFour();
        for(int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += 4, bitsAddr += blockWidth){
          if((PlatformDependent.getInt(bitsAddr) & nullMask) == nullMask){
            // the nulls are not comparable. as such, this doesn't match.
            final int keyHash = (int)hashValues.get(keyIndex);
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
          } else {
            PlatformDependent.putInt(offsetAddr, SKIP);
          }
        }
        break;
      }

      // 64 bits to consider.
      case EIGHT: {
        long bitsAddr = keyFixedVectorAddr;
        final long nullMask = compare.getEight();
        for(int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += 4, bitsAddr += blockWidth){
          if((PlatformDependent.getLong(bitsAddr) & nullMask) == nullMask){
            final int keyHash = (int)hashValues.get(keyIndex);
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
          } else {
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(offsetAddr, SKIP);
          }
        }
        break;
      }

      // more than 64 bits to consider.
      case BIG: {
        long bitsAddr = keyFixedVectorAddr;
        for(int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += 4, bitsAddr += blockWidth){
          if(compare.isComparableBigBits(bitsAddr)){
            final int keyHash = (int)hashValues.get(keyIndex);
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
          } else {
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(offsetAddr, SKIP);
          }
        }
        break;
      }


      default:
        throw new IllegalStateException();
      }


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
