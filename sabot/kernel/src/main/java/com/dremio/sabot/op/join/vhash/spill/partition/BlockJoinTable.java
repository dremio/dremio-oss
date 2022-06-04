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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

public class BlockJoinTable implements JoinTable {
  private static final int MAX_VALUES_PER_BATCH = 4096;
  private final HashTable table;
  private final Stopwatch probeFindWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private boolean tableTracing;

  public BlockJoinTable(PivotDef buildPivot, BufferAllocator allocator, NullComparator nullMask,
                        int minSize, int varFieldAverageSize, SabotConfig sabotConfig, OptionManager optionManager) {
    super();
    this.table = HashTable.getInstance(sabotConfig,
      optionManager.getOption(ExecConstants.ENABLE_NATIVE_HASHTABLE_FOR_JOIN),
      new HashTable.HashTableCreateArgs(HashConfig.getDefault(), buildPivot, allocator, minSize,
        varFieldAverageSize, false, MAX_VALUES_PER_BATCH, nullMask));
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

  // Get the total length of the variable keys for the all the ordinals in keyOffsetAddr, from hash table.
  public int getCumulativeVarKeyLength(final long keyOffsetAddr, final int count) {
    return table.getCumulativeVarKeyLength(keyOffsetAddr, count);
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
  public void hashPivoted(int records, long keyFixedVectorAddr, long keyVarVectorAddr, long seed, long hashoutAddr8B) {
    table.computeHash(records, keyFixedVectorAddr, keyVarVectorAddr, seed, hashoutAddr8B);
  }

  @Override
  public void insertPivoted(long sv2Addr, int records,
                            long tableHashAddr4B, FixedBlockVector fixed, VariableBlockVector variable,
                            long outputAddr) {
    if (tableTracing) {
      table.traceInsertStart(records);
    }

    final long keyFixedVectorAddr = fixed.getMemoryAddress();
    final long keyVarVectorAddr = variable.getMemoryAddress();

    insertWatch.start();
    table.addSv2(records, sv2Addr, keyFixedVectorAddr, keyVarVectorAddr, tableHashAddr4B, outputAddr);
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
    final HashTable table = this.table;
    final long keyFixedVectorAddr = fixed.getMemoryAddress();
    final long keyVarVectorAddr = variable.getMemoryAddress();

    probeFindWatch.start();
    table.findSv2(records, sv2Addr, keyFixedVectorAddr, keyVarVectorAddr,
      tableHashAddr4B, outputAddr);
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
