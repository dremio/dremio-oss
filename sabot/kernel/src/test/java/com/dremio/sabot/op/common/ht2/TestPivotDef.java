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
package com.dremio.sabot.op.common.ht2;

import static org.junit.Assert.assertEquals;

import com.dremio.sabot.BaseTestWithAllocator;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldType;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

public class TestPivotDef extends BaseTestWithAllocator {

  @Test
  public void pivotDef() {
    try (IntVector col1 = new IntVector("col1", allocator);
        IntVector col2 = new IntVector("col2", allocator);
        BigIntVector col3 = new BigIntVector("col3", allocator);
        TimeMilliVector col4 = new TimeMilliVector("col4", allocator);
        VarCharVector col5 = new VarCharVector("col5", allocator);
        VarCharVector col6 = new VarCharVector("col6", allocator);
        BitVector col7 = new BitVector("col7", allocator); ) {

      PivotDef pivot =
          PivotBuilder.getBlockDefinition(
              FluentIterable.from(ImmutableList.of(col1, col2, col3, col4, col5, col6, col7))
                  .transform(
                      new Function<FieldVector, FieldVectorPair>() {

                        @Override
                        public FieldVectorPair apply(FieldVector input) {
                          return new FieldVectorPair(input, input);
                        }
                      })
                  .toList());

      assertEquals(2, pivot.getVariableCount());
      assertEquals(
          4
              + // allbits (8 total, 7 nullable + the bit vector)
              4
              + // col1 (int)
              4
              + // col2 (int)
              8
              + // col3 (bigint)
              4
              + // col4 (time)
              0
              + // varchar
              0
              + // varchar
              0
              + // bitvector
              4 // varoffset
          ,
          pivot.getBlockWidth());

      assertEquals(8, pivot.getBitCount());

      assertEquals(
          ImmutableList.of(
              new VectorPivotDef(FieldType.FOUR_BYTE, 0, 0, 4, col1, col1),
              new VectorPivotDef(FieldType.FOUR_BYTE, 0, 1, 8, col2, col2),
              new VectorPivotDef(FieldType.EIGHT_BYTE, 0, 2, 12, col3, col3),
              new VectorPivotDef(FieldType.FOUR_BYTE, 0, 3, 20, col4, col4),
              new VectorPivotDef(FieldType.BIT, 0, 6, 7, col7, col7)),
          pivot.getFixedPivots());

      assertEquals(
          ImmutableList.of(
              new VectorPivotDef(FieldType.VARIABLE, 0, 4, 0, col5, col5),
              new VectorPivotDef(FieldType.VARIABLE, 0, 5, 1, col6, col6)),
          pivot.getVariablePivots());
    }
  }
}
