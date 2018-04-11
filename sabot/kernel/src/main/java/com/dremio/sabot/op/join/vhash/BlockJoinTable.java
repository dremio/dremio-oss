/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.ResizeListener;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

import io.netty.util.internal.PlatformDependent;

public class BlockJoinTable implements JoinTable {

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

  public BlockJoinTable(PivotDef buildPivot, PivotDef probePivot, BufferAllocator allocator, NullComparator nullMask, int minSize, int varFieldAverageSize) {
    super();
    this.table = new LBlockHashTable(HashConfig.getDefault(), buildPivot, allocator, minSize, varFieldAverageSize, ResizeListener.NO_OP);
    this.buildPivot = buildPivot;
    this.probePivot = probePivot;
    this.allocator = allocator;
    this.nullMask = nullMask;
    this.tableTracing = false;
  }

  public long getProbePivotTime(TimeUnit unit){
    return probePivotWatch.elapsed(unit);
  }

  public long getProbeFindTime(TimeUnit unit){
    return probeFindWatch.elapsed(unit);
  }

  public long getBuildPivotTime(TimeUnit unit){
    return pivotBuildWatch.elapsed(unit);
  }

  public long getInsertTime(TimeUnit unit){
    return insertWatch.elapsed(unit);
  }

  @Override
  public void insert(final long findAddr, int records) {
    try(FixedBlockVector fbv = new FixedBlockVector(allocator, buildPivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(allocator, buildPivot.getVariableCount());
        ){
      // first we pivot.
      pivotBuildWatch.start();
      Pivots.pivot(buildPivot, records, fbv, var);
      pivotBuildWatch.stop();
      final long keyFixedAddr = fbv.getMemoryAddress();
      final long keyVarAddr = var.getMemoryAddress();

      if (tableTracing) {
        table.traceInsertStart(records);
      }
      insertWatch.start();
      long ordAddr = findAddr;
      for(int i =0 ; i < records; i++, ordAddr += 4){
        PlatformDependent.putInt(ordAddr, table.add(keyFixedAddr, keyVarAddr, i));
      }
      insertWatch.stop();
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
        ){
      // first we pivot.
      probePivotWatch.start();
      Pivots.pivot(probePivot, records, fbv, var);
      probePivotWatch.stop();
      final long keyFixedAddr = fbv.getMemoryAddress();
      final long keyVarAddr = var.getMemoryAddress();

      // then we add all values to table.
      probeFindWatch.start();
      final NullComparator compare = nullMask;
      switch(compare.getMode()){
      case NONE:
        for(int i = 0; i < records; i++, offsetAddr += 4){
          PlatformDependent.putInt(offsetAddr, table.find(keyFixedAddr, keyVarAddr, i));
        }
        break;

      // 32 bits to consider.
      case FOUR: {
        long bitsAddr = keyFixedAddr;
        final int nullMask = compare.getFour();
        for(int i = 0; i < records; i++, offsetAddr += 4, bitsAddr += blockWidth){
          if((PlatformDependent.getInt(bitsAddr) & nullMask) == nullMask){
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedAddr, keyVarAddr, i));
          } else {
            PlatformDependent.putInt(offsetAddr, SKIP);
          }
        }
        break;
      }

      // 64 bits to consider.
      case EIGHT: {
        long bitsAddr = keyFixedAddr;
        final long nullMask = compare.getEight();
        for(int i = 0; i < records; i++, offsetAddr += 4, bitsAddr += blockWidth){
          if((PlatformDependent.getLong(bitsAddr) & nullMask) == nullMask){
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedAddr, keyVarAddr, i));
          } else {
            // the nulls are not comparable. as such, this doesn't match.
            PlatformDependent.putInt(offsetAddr, SKIP);
          }
        }
        break;
      }

      // more than 64 bits to consider.
      case BIG: {
        long bitsAddr = keyFixedAddr;
        for(int i = 0; i < records; i++, offsetAddr += 4, bitsAddr += blockWidth){
          if(compare.isComparableBigBits(bitsAddr)){
            PlatformDependent.putInt(offsetAddr, table.find(keyFixedAddr, keyVarAddr, i));
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
