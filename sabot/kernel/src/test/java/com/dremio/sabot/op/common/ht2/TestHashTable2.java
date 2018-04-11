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

import static org.junit.Assert.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.junit.Test;

import com.dremio.exec.record.VectorContainer;
import com.koloboke.collect.hash.HashConfig;

public class TestHashTable2 {

  @Test
  public void simple() throws Exception {

    String[] col1arr = {"hello", "my", "hello", "hello", null, null};
    String[] col2arr = {"every", "every", "every", "none", null, null};
    Integer[] col3arr = {1, 1, 1, 1, 1, 1};

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer();) {

      NullableVarCharVector col1 = new NullableVarCharVector("col1", allocator);
      TestVarBinaryPivot.populate(col1, col1arr);
      c.add(col1);
      NullableVarCharVector col2 = new NullableVarCharVector("col2", allocator);
      TestVarBinaryPivot.populate(col2, col2arr);
      c.add(col2);
      NullableIntVector col3 = new NullableIntVector("col3", allocator);
      TestIntPivot.populate(col3, col3arr);
      c.add(col3);
      final int records = c.setAllCount(col1arr.length);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
          );
      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, ResizeListener.NO_OP);) {
          final long keyFixedAddr = fbv.getMemoryAddress();
          final long keyVarAddr = var.getMemoryAddress();
          int[] expectedOrdinals = {0, 1, 0, 2, 3, 3};
          int[] actualOrdinals = new int[expectedOrdinals.length];
          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            actualOrdinals[keyIndex] = bht.add(keyFixedAddr, keyVarAddr, keyIndex);
          }
          assertArrayEquals(expectedOrdinals, actualOrdinals);

        }
      }

    }
  }

  @Test
  public void emptyvalues() throws Exception {

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer();) {

      NullableVarCharVector col1 = new NullableVarCharVector("col1", allocator);
      c.add(col1);
      NullableVarCharVector col2 = new NullableVarCharVector("col2", allocator);
      c.add(col2);
      NullableIntVector col3 = new NullableIntVector("col3", allocator);
      c.add(col3);
      c.allocateNew();
      final int records = c.setAllCount(2000);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
          );

      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, ResizeListener.NO_OP);) {
          final long keyFixedAddr = fbv.getMemoryAddress();
          final long keyVarAddr = var.getMemoryAddress();
          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            assertEquals(0, bht.add(keyFixedAddr, keyVarAddr, keyIndex));
          }
        }
      }

    }
  }
}
