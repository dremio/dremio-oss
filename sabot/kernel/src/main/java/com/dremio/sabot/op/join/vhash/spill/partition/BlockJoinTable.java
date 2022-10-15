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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

public class BlockJoinTable implements JoinTable {
  private static final int MAX_VALUES_PER_BATCH = 4096;
  private final HashTable table;
  private final BufferAllocator allocator;
  private final PivotDef buildPivot;
  private final Stopwatch probeFindWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private boolean tableTracing;

  public BlockJoinTable(PivotDef buildPivot, BufferAllocator allocator, NullComparator nullMask,
                        int minSize, int varFieldAverageSize, SabotConfig sabotConfig,
                        OptionManager optionManager, boolean runtimeFilterEnabled) {
    super();
    this.allocator = allocator.newChildAllocator("block-join", 0, allocator.getLimit());
    this.buildPivot = buildPivot;
    Preconditions.checkState(buildPivot.getBlockWidth() != 0);
    this.table = HashTable.getInstance(sabotConfig,
      optionManager.getOption(ExecConstants.ENABLE_NATIVE_HASHTABLE_FOR_JOIN),
      new HashTable.HashTableCreateArgs(HashConfig.getDefault(), buildPivot,
        allocator, minSize, varFieldAverageSize, false,
        MAX_VALUES_PER_BATCH, nullMask, runtimeFilterEnabled));
    this.tableTracing = false;
  }

  /* Copy the keys of the records specified in keyOffset to destination memory
   * keyOrdinals contains all the ordinals of keys
   * count is the number of keys
   * keyFixed is the destination memory for fixed portion of the keys
   * keyVar is the destination memory for variable portion of the keys
   */
  @Override
  public void copyKeysToBuffer(final ArrowBuf keyOrdinals, final int count, final ArrowBuf keyFixed, final ArrowBuf keyVar) {
    table.copyKeysToBuffer(keyOrdinals, count, keyFixed, keyVar);
  }

  // Get the total length of the variable keys for the all the ordinals in keyOrdinals, from hash table.
  @Override
  public int getCumulativeVarKeyLength(final ArrowBuf keyOrdinals, final int count) {
    return table.getCumulativeVarKeyLength(keyOrdinals, count);
  }

  @Override
  public void getVarKeyLengths(ArrowBuf keyOrdinals, int count, ArrowBuf out) {
    table.getVarKeyLengths(keyOrdinals, count, out);
  }

  @Override
  public long getProbeFindTime(TimeUnit unit){
    return probeFindWatch.elapsed(unit);
  }

  @Override
  public long getInsertTime(TimeUnit unit){
    return insertWatch.elapsed(unit);
  }

  @Override
  public void prepareBloomFilters(PartitionColFilters partitionColFilters) {
    partitionColFilters.prepareBloomFilters(table);
  }

  @Override
  public void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters) {
    nonPartitionColFilters.prepareValueListFilters(table);
  }

  @Override
  public void hashPivoted(int records, ArrowBuf keyFixed, ArrowBuf keyVar, long seed, ArrowBuf hashout8B) {
    table.computeHash(records, keyFixed, keyVar, seed, hashout8B);
  }

  @Override
  public int insertPivoted(ArrowBuf sv2, int pivotShift, int records,
                           ArrowBuf tableHash4B, FixedBlockVector fixed, VariableBlockVector variable,
                           ArrowBuf output) {
    if (tableTracing) {
      table.traceInsertStart(records);
    }

    insertWatch.start();
    int recordsAdded = table.addSv2(sv2, pivotShift, records, fixed.getBuf(), variable.getBuf(), tableHash4B, output);
    insertWatch.stop();

    if (tableTracing) {
      table.traceOrdinals(output.memoryAddress(), recordsAdded);
      table.traceInsertEnd();
    }
    return recordsAdded;
  }

  @Override
  public int getMaxOrdinal() {
    return table.getMaxOrdinal();
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public void findPivoted(ArrowBuf sv2, int pivotShift, int records, ArrowBuf tableHash4B, FixedBlockVector fixed, VariableBlockVector variable, ArrowBuf output) {
    final HashTable table = this.table;

    probeFindWatch.start();
    table.findSv2(sv2, pivotShift, records, fixed.getBuf(), variable.getBuf(), tableHash4B, output);
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
    AutoCloseables.close(allocator);
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
