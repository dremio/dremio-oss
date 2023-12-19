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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Preconditions;
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

  public BlockJoinTable(PivotDef buildPivot, PivotDef probePivot, BufferAllocator allocator,
                        NullComparator nullMask, int minSize, int varFieldAverageSize, SabotConfig sabotConfig,
                        OptionManager optionManager, boolean runtimeFilterEnabled) {
    super();
    Preconditions.checkState(buildPivot.getBlockWidth() == probePivot.getBlockWidth());
    Preconditions.checkState(buildPivot.getBlockWidth() != 0);
    this.allocator = allocator.newChildAllocator("block-join", 0, allocator.getLimit());
    this.table = HashTable.getInstance(sabotConfig,
      optionManager, new HashTable.HashTableCreateArgs(HashConfig.getDefault(), buildPivot, allocator,
        minSize, varFieldAverageSize, false, MAX_VALUES_PER_BATCH,
        nullMask, runtimeFilterEnabled));
    this.buildPivot = buildPivot;
    this.probePivot = probePivot;
    this.tableTracing = false;
  }

  /**
   * Copy the keys of the records specified in ordinals to keyFixed/keyVar.
   * @param ordinals input buffer of ordinals
   * @param count count of ordinals
   * @param keyFixed dst for fixed portion of keys
   * @param keyVar dst for variable portion of keys
   */
  public void copyKeysToBuffer(ArrowBuf ordinals, int count, ArrowBuf keyFixed, ArrowBuf keyVar) {
    table.copyKeysToBuffer(ordinals, count, keyFixed, keyVar);
  }

  // Get the total length of the variable keys for all the specified ordinals, from hash table
  public int getCumulativeVarKeyLength(ArrowBuf ordinals, int numRecords) {
    return table.getCumulativeVarKeyLength(ordinals, numRecords);
  }

  @Override
  public long getProbePivotTime(TimeUnit unit) {
    return probePivotWatch.elapsed(unit);
  }

  @Override
  public long getProbeFindTime(TimeUnit unit) {
    return probeFindWatch.elapsed(unit);
  }

  @Override
  public long getBuildPivotTime(TimeUnit unit) {
    return pivotBuildWatch.elapsed(unit);
  }

  @Override
  public long getInsertTime(TimeUnit unit) {
    return insertWatch.elapsed(unit);
  }

  @Override
  public long getBuildHashComputationTime(TimeUnit unit) {
    return buildHashComputationWatch.elapsed(unit);
  }

  @Override
  public long getProbeHashComputationTime(TimeUnit unit) {
    return probeHashComputationWatch.elapsed(unit);
  }

  @Override
  public void prepareBloomFilters(PartitionColFilters partitionColFilters, boolean sizeDynamically) {
    partitionColFilters.prepareBloomFilters(table);
  }

  @Override
  public void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters) {
    nonPartitionColFilters.prepareValueListFilters(table);
  }

  @Override
  public void insert(ArrowBuf out, int records) {
    try(FixedBlockVector fbv = new FixedBlockVector(allocator, buildPivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, buildPivot.getVariableCount())) {
      // STEP 1: first we pivot.
      pivotBuildWatch.start();
      Pivots.pivot(buildPivot, records, fbv, var);
      pivotBuildWatch.stop();

      if (tableTracing) {
        table.traceInsertStart(records);
      }

      try (ArrowBuf hashValues = allocator.buffer(records * 8)) {
        // STEP 2: then we do the hash computation on entire batch
        buildHashComputationWatch.start();
        table.computeHash(records, fbv.getBuf(), var.getBuf(), 0, hashValues);
        buildHashComputationWatch.stop();

        // STEP 3: then we insert build side into hash table
        insertWatch.start();
        int recordsAdded = table.add(records, fbv.getBuf(), var.getBuf(), hashValues, out);
        insertWatch.stop();

        if (recordsAdded < records) {
          throw new OutOfMemoryException(String.format("Only %d records out of %d were added to the HashTable",
            recordsAdded, records));
        }
      }

      if (tableTracing) {
        table.traceOrdinals(out.memoryAddress(), records);
        table.traceInsertEnd();
      }
    }
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public void find(ArrowBuf out, final int records) {
    final HashTable table = this.table;

    try(FixedBlockVector fbv = new FixedBlockVector(allocator, probePivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, probePivot.getVariableCount());
        ArrowBuf hashValues = allocator.buffer(records * 8)) {
      // STEP 1: first we pivot.
      probePivotWatch.start();
      Pivots.pivot(probePivot, records, fbv, var);
      probePivotWatch.stop();

      // STEP 2: then we do the hash computation on entire batch
      probeHashComputationWatch.start();
      table.computeHash(records, fbv.getBuf(), var.getBuf(), 0, hashValues);
      probeHashComputationWatch.stop();

      // STEP 3: then we probe hash table.
      probeFindWatch.start();
      table.find(records, fbv.getBuf(), var.getBuf(), hashValues, out);
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
