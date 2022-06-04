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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.SimpleBigIntVector;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

public class BlockJoinTable implements JoinTable {

  private static final int MAX_VALUES_PER_BATCH = 4096;
  private final HashTable table;
  private final PivotDef buildPivot;
  private final PivotDef probePivot;
  private final BufferAllocator allocator;
  private final Stopwatch probePivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeFindWatch = Stopwatch.createUnstarted();
  private final Stopwatch pivotBuildWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private boolean tableTracing;
  private final Stopwatch buildHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeHashComputationWatch = Stopwatch.createUnstarted();

  public BlockJoinTable(PivotDef buildPivot, PivotDef probePivot, BufferAllocator allocator, NullComparator nullMask,
                        int minSize, int varFieldAverageSize, SabotConfig sabotConfig, OptionManager optionManager) {
    super();
    Preconditions.checkArgument(buildPivot.getBlockWidth() == probePivot.getBlockWidth());
    this.table = HashTable.getInstance(sabotConfig,
      optionManager.getOption(ExecConstants.ENABLE_NATIVE_HASHTABLE_FOR_JOIN),
      new HashTable.HashTableCreateArgs(HashConfig.getDefault(), buildPivot, allocator, minSize,
        varFieldAverageSize, false, MAX_VALUES_PER_BATCH, nullMask));
    this.buildPivot = buildPivot;
    this.probePivot = probePivot;
    this.allocator = allocator;
    this.tableTracing = false;
  }

  /* Copy the keys of the records specified in keyOffsetAddr to destination memory
   * keyOffsetAddr contains all the ordinals of keys
   * count is the number of keys
   * keyFixedAddr is the destination memory for fiexed keys
   * keyVarAddr is the destination memory for variable keys
   */
  public void copyKeysToBuffer(final long keyOffsetAddr, final int count, final long keyFixedAddr, final long keyVarAddr) {
    table.copyKeysToBuffer(keyOffsetAddr, count, keyFixedAddr, keyVarAddr);
  }

  // Get the total length of the variable keys for all the ordinals in offsetVectorAddr, from hash table
  public int getCumulativeVarKeyLength(long offsetVectorAddr, int numRecords) {
    return table.getCumulativeVarKeyLength(offsetVectorAddr, numRecords);
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
  public void insert(long outAddr, int records) {
    try(FixedBlockVector fbv = new FixedBlockVector(allocator, buildPivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, buildPivot.getVariableCount());
        ){
      // STEP 1: first we pivot.
      pivotBuildWatch.start();
      Pivots.pivot(buildPivot, records, fbv, var);
      pivotBuildWatch.stop();

      if (tableTracing) {
        table.traceInsertStart(records);
      }

      try (SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
        hashValues.allocateNew(records);

        final long keyFixedVectorAddr = fbv.getMemoryAddress();
        final long keyVarVectorAddr = var.getMemoryAddress();
        final long hashVectorAddr8B = hashValues.getBufferAddress();

        // STEP 2: then we do the hash computation on entire batch
        buildHashComputationWatch.start();
        table.computeHash(records, keyFixedVectorAddr, keyVarVectorAddr,
          0, hashVectorAddr8B);
        buildHashComputationWatch.stop();

        // STEP 3: then we insert build side into hash table
        insertWatch.start();
        table.add(records, keyFixedVectorAddr, keyVarVectorAddr,
          hashVectorAddr8B, outAddr);
        insertWatch.stop();
      }

      if (tableTracing) {
        table.traceOrdinals(outAddr, records);
        table.traceInsertEnd();
      }
    }
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public void find(long outAddr, final int records) {
    final HashTable table = this.table;

    try(FixedBlockVector fbv = new FixedBlockVector(allocator, probePivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, probePivot.getVariableCount());
        SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
      // STEP 1: first we pivot.
      probePivotWatch.start();
      Pivots.pivot(probePivot, records, fbv, var);
      probePivotWatch.stop();

      // STEP 2: then we do the hash computation on entire batch
      hashValues.allocateNew(records);

      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      final long hashVectorAddr8B = hashValues.getBufferAddress();

      probeHashComputationWatch.start();
      table.computeHash(records, keyFixedVectorAddr, keyVarVectorAddr,
        0, hashVectorAddr8B);
      probeHashComputationWatch.stop();

      // STEP 3: then we probe hash table.
      probeFindWatch.start();
      table.find(records, keyFixedVectorAddr, keyVarVectorAddr,
        hashVectorAddr8B, outAddr);
      probeFindWatch.stop();
    }
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
