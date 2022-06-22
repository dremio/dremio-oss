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
package com.dremio.sabot.op.join.nlje;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.AutoCloseables.RollbackCloseable;

import com.dremio.common.util.Closeable;
import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * A pair of offset vectors that describe the values of the probe and build values to scan.
 */
public class VectorRange extends DualRange implements Closeable {

  public static final int PROBE_OUTPUT_SIZE = 2;
  public static final int BUILD_OUTPUT_SIZE = 4;

  protected InputRangeIterator rangeIterator = InputRangeIterator.EMPTY;
  private ArrowBuf probeOffsets;
  private ArrowBuf buildOffsets;
  protected final int maxOutputRange;
  protected final IntRange totalOutputRange;
  protected final int targetOutputSize;
  protected final IntRange currentOutputRange;

  public VectorRange(
      int maxInterOutput,
      int targetOutputSize) {
    Preconditions.checkArgument(targetOutputSize <= maxInterOutput);
    this.maxOutputRange = maxInterOutput;
    this.totalOutputRange = IntRange.EMPTY;
    this.targetOutputSize = targetOutputSize;
    this.currentOutputRange = IntRange.EMPTY;
  }

  public void provideIterator(InputRangeIterator iter) {
    this.rangeIterator = iter;
  }


  private VectorRange(
      int maxOutputRange,
      IntRange totalOutputRange,
      int targetOutputSize,
      IntRange currentOutputRange,
      InputRangeIterator rangeIterator,
      ArrowBuf probeOffsets,
      ArrowBuf buildOffsets) {
    this.maxOutputRange = maxOutputRange;
    this.totalOutputRange = totalOutputRange;
    this.targetOutputSize = targetOutputSize;
    this.currentOutputRange = currentOutputRange;
    this.rangeIterator = rangeIterator;
    this.probeOffsets = probeOffsets;
    this.buildOffsets = buildOffsets;
  }

  public void allocate(BufferAllocator allocator) throws Exception {
    try (RollbackCloseable rbc = new RollbackCloseable()){
      probeOffsets = rbc.add(allocator.buffer(maxOutputRange * PROBE_OUTPUT_SIZE));
      buildOffsets = rbc.add(allocator.buffer(maxOutputRange * BUILD_OUTPUT_SIZE));
      rbc.commit();
    }
  }

  public ArrowBuf getProbeOut() {
    return probeOffsets;
  }

  public ArrowBuf getBuildOut() {
    return buildOffsets;
  }

  @Override
  public boolean isEmpty() {
    return totalOutputRange.isEmpty();
  }

  public int getCurrentOutputCount() {
    return currentOutputRange.end - currentOutputRange.start;
  }

  public IntRange getCurrentOutputRange() {
    return currentOutputRange;
  }

  protected IntRange initialOutputRange(int recordsFound) {
    return IntRange.of(0, Math.min(recordsFound, targetOutputSize));
  }

  /**
   * Get the next output range for the current set of matched values. Can only be called if hasRemainingOutput is true.
   * @return The next range.
   */
  protected IntRange nextOutputRange() {
    Preconditions.checkArgument(hasRemainingOutput());
    return IntRange.of(currentOutputRange.end, Math.min(currentOutputRange.end + targetOutputSize, totalOutputRange.end));
  }

  protected boolean hasRemainingOutput() {
    return currentOutputRange.end < totalOutputRange.end;
  }

  public int getMaxOutputCount() {
    return maxOutputRange;
  }

  public long getProbeOffsets2() {
    return probeOffsets.memoryAddress();
  }

  public long getBuildOffsets4() {
    return buildOffsets.memoryAddress();
  }

  public static void set(long probeOutputAddr, long buildOutputAddr, int outputIndex, short probeIndex, int compoundBuildIndex) {
    PlatformDependent.putShort(probeOutputAddr + outputIndex * PROBE_OUTPUT_SIZE, probeIndex);
    PlatformDependent.putInt(buildOutputAddr + outputIndex * BUILD_OUTPUT_SIZE, compoundBuildIndex);
  }

  @Override
  public VectorRange asVectorRange() {
    return this;
  }

  @Override
  public boolean isIndexRange() {
    return false;
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(probeOffsets, buildOffsets, rangeIterator);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return hasRemainingOutput() || rangeIterator.hasNext();
  }

  @Override
  public VectorRange nextOutput() {

    if(hasRemainingOutput()) {
      return new VectorRange(maxOutputRange, totalOutputRange, targetOutputSize, nextOutputRange(), rangeIterator, probeOffsets, buildOffsets);
    }

    if(rangeIterator.hasNext()) {
      int records = rangeIterator.next();
      return new VectorRange(maxOutputRange, IntRange.of(0, records), targetOutputSize, IntRange.EMPTY, rangeIterator, probeOffsets, buildOffsets);
    }

    return new VectorRange(maxOutputRange, IntRange.EMPTY, targetOutputSize, IntRange.EMPTY, rangeIterator, probeOffsets, buildOffsets);
  }

  @Override
  public VectorRange startNextProbe(int records) {
    rangeIterator.startNextProbe(records);
    return new VectorRange(maxOutputRange, IntRange.EMPTY, targetOutputSize, IntRange.EMPTY, rangeIterator, probeOffsets, buildOffsets);
  }

  public VectorRange resetRecordsFound(int recordsFound) {
    return new VectorRange(maxOutputRange, IntRange.of(0, recordsFound), targetOutputSize, IntRange.EMPTY, rangeIterator, probeOffsets, buildOffsets);
  }
}
