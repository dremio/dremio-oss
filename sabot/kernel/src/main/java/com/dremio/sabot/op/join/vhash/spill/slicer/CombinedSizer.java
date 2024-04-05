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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

class CombinedSizer implements Sizer {

  private final ImmutableList<Sizer> sizers;

  public CombinedSizer(List<Sizer> sizers) {
    this.sizers = ImmutableList.copyOf(sizers);
  }

  @Override
  public void reset() {
    sizers.forEach(Sizer::reset);
  }

  @Override
  public int computeBitsNeeded(ArrowBuf sv2, int startIdx, int numberOfRecords) {
    return sizers.stream().mapToInt(s -> s.computeBitsNeeded(sv2, startIdx, numberOfRecords)).sum();
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(int ordinal, int numberOfRecords) {
    return sizers.stream()
        .mapToInt(s -> s.getSizeInBitsStartingFromOrdinal(ordinal, numberOfRecords))
        .sum();
  }

  @Override
  public Copier getCopier(
      BufferAllocator allocator,
      ArrowBuf sv2,
      int startIdx,
      int count,
      List<FieldVector> vectorOutput) {
    return new CombinedCopier(
        sizers.stream()
            .map(s -> s.getCopier(allocator, sv2, startIdx, count, vectorOutput))
            .collect(Collectors.toList()));
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    return sizers.stream().mapToInt(Sizer::getEstimatedRecordSizeInBits).sum();
  }

  private static class CombinedCopier implements Copier {
    private final List<Copier> copiers;

    public CombinedCopier(List<Copier> copiers) {
      this.copiers = copiers;
    }

    @Override
    public void copy(final Page page) {
      for (Copier c : copiers) {
        c.copy(page);
      }
    }
  }
}
