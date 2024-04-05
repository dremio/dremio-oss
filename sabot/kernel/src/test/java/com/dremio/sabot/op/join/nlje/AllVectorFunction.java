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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.VectorAccessible;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A DualRange vector function that generates a VectorRange that is the same as InputRange to test
 * DualRange functionality in the NLJEOperator.
 */
public class AllVectorFunction implements DualRangeFunctionFactory {

  @Override
  public DualRange create(
      BufferAllocator allocator,
      VectorAccessible left,
      VectorAccessible right,
      int targetOutputSize,
      int targetGeneratedAtOnce,
      int[] buildCounts,
      LogicalExpression vectorExpression)
      throws Exception {
    // work on one probe at a time.
    VectorRange vectorRange = new VectorRange(targetGeneratedAtOnce, targetOutputSize);
    Iter iter = new Iter(targetGeneratedAtOnce, buildCounts, vectorRange);
    vectorRange.provideIterator(iter);
    vectorRange.allocate(allocator);
    return vectorRange;
  }

  private static class Iter implements InputRangeIterator {

    private final VectorRange output;
    private IndexRange incomingRange;

    public Iter(int targetGeneratedAtOnce, int[] buildCounts, VectorRange output) {
      this.incomingRange = new IndexRange(targetGeneratedAtOnce, buildCounts);
      this.output = output;
    }

    @Override
    public void startNextProbe(int probeRecords) {
      incomingRange = incomingRange.startNextProbe(probeRecords);
    }

    @Override
    public boolean hasNext() {
      return incomingRange.hasNext();
    }

    @Override
    public int next() {
      incomingRange = incomingRange.nextOutput();
      final long probeOutputAddr = output.getProbeOffsets2();
      final long buildOutputAddr = output.getBuildOffsets4();

      final int probeStart = incomingRange.getProbeStart();
      final int probeEnd = incomingRange.getProbeEnd();
      final int buildMax = incomingRange.getBuildBatchCount();
      final int buildBatchIndex = incomingRange.getBuildBatchIndex();

      int outputIndex = 0;

      for (int probeIndex = probeStart; probeIndex < probeEnd; probeIndex++) {
        for (int buildIndex = 0; buildIndex < buildMax; buildIndex++) {
          int compoundBuildIndex = (buildBatchIndex << 16) | (buildIndex & 65535);
          VectorRange.set(
              probeOutputAddr,
              buildOutputAddr,
              outputIndex,
              (short) probeIndex,
              compoundBuildIndex);
          outputIndex++;
        }
      }
      return outputIndex;
    }
  }
}
