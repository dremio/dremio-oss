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
package com.dremio.sabot.op.common.ht2;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;

import io.netty.buffer.ArrowBuf;

public class TestBitPivot extends BaseTestWithAllocator {

  @Test
  public void pivotWithNulls() throws Exception {
    final int count = 4;
    try (
      BitVector in = new BitVector("in", allocator);
      BitVector out = new BitVector("out", allocator);
    ) {

      in.allocateNew(count);
      ArrowBuf validityBuf = in.getValidityBuffer();
      ArrowBuf valueBuf = in.getValidityBuffer();

      // index 0 : valid and true
      BitVectorHelper.setValidityBit(validityBuf, 0, 1);
      BitVectorHelper.setValidityBit(valueBuf, 0, 1);

      // index 1 : valid and false
      BitVectorHelper.setValidityBit(validityBuf, 1, 1);
      BitVectorHelper.setValidityBit(valueBuf, 1, 0);

      // index 2 : invalid and true
      BitVectorHelper.setValidityBit(validityBuf,2, 0);
      BitVectorHelper.setValidityBit(valueBuf, 2, 1);

      // index 3 : invalid and false
      BitVectorHelper.setValidityBit(validityBuf,3, 0);
      BitVectorHelper.setValidityBit(valueBuf, 3, 0);

      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        Unpivots.unpivot(pivot, fbv, vbv, 0, count);

        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

}
