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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class EightByteInnerLeftProbeOff implements JoinTable {

  public static int FOUR_BYTE = 4;
  public static int EIGHT_BYTE = 8;
  private static final int WORD_BITS = 64;
  private static final int WORD_BYTES = 8;
  private static final long ALL_SET = 0xFFFFFFFFFFFFFFFFL;
  private static final long NONE_SET = 0;

  private final LBlockHashTableEight map;
  private final FieldVector probe;
  private final FieldVector build;
  private final Stopwatch findWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();

  public EightByteInnerLeftProbeOff(BufferAllocator allocator, int initialSize, PivotDef probeDef, PivotDef buildDef){
    Preconditions.checkArgument(probeDef.getFixedPivots().size() == 1);
    Preconditions.checkArgument(buildDef.getFixedPivots().size() == 1);
    this.probe = probeDef.getFixedPivots().get(0).getIncomingVector();
    this.build = buildDef.getFixedPivots().get(0).getIncomingVector();
    this.map = new LBlockHashTableEight(HashConfig.getDefault(), allocator, initialSize);
  }

  @Override
  public void insert(long outputAddr, int count) {
    insertWatch.start();
    final List<ArrowBuf> buffers = build.getFieldBuffers();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);


    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);

      if (bitValues == NONE_SET) {
        // noop (all nulls).
        srcDataAddr += (WORD_BITS * EIGHT_BYTE);
        for (int i = 0; i < WORD_BITS; i++, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
        }
      } else if (bitValues == ALL_SET) {
        // all set, skip individual checks.
        for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr)));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          if(bitVal == 1){
            PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr)));
          } else {
            PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
          }
        }
      }
      srcBitsAddr += WORD_BYTES;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
        for (int i = 0; i < remainCount; i++, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
        }
      } else if (bitValues == ALL_SET) {

        // all set,
        for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr)));
        }
      } else {
        // some nulls,
        for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          if(bitVal == 1){
            PlatformDependent.putInt(outputAddr, map.insert(PlatformDependent.getLong(srcDataAddr)));
          } else {
            PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
          }
        }
      }
    }

    insertWatch.stop();
  }

  @Override
  public void find(long outputAddr, final int count) {
    findWatch.start();
    final List<ArrowBuf> buffers = probe.getFieldBuffers();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);


    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);

      if (bitValues == NONE_SET) {
        // noop (all nulls).
        srcDataAddr += (WORD_BITS * EIGHT_BYTE);
        for (int i = 0; i < WORD_BITS; i++, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
        }

      } else if (bitValues == ALL_SET) {
        // all set, skip individual checks.
        for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr)));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          if(bitVal == 1){
            PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr)));
          } else {
            PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
          }
        }
      }
      srcBitsAddr += WORD_BYTES;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
        for (int i = 0; i < remainCount; i++, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
        }
      } else if (bitValues == ALL_SET) {

        // all set,
        for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr)));
        }
      } else {
        // some nulls,
        for (int i = 0; i < remainCount; i++, srcDataAddr += EIGHT_BYTE, outputAddr += FOUR_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          if(bitVal == 1){
            PlatformDependent.putInt(outputAddr, map.get(PlatformDependent.getLong(srcDataAddr)));
          } else {
            PlatformDependent.putInt(outputAddr, LBlockHashTableEight.NO_MATCH);
          }
        }
      }
    }

    findWatch.stop();
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


}
