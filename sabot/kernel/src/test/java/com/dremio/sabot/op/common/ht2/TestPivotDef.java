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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldType;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class TestPivotDef extends BaseTestWithAllocator {

  @Test
  public void pivotDef(){
    try(
        NullableIntVector col1 = new NullableIntVector("col1", allocator);
        NullableIntVector col2 = new NullableIntVector("col2", allocator);
        NullableBigIntVector col3 = new NullableBigIntVector("col3", allocator);
        NullableTimeMilliVector col4 = new NullableTimeMilliVector("col4", allocator);
        NullableVarCharVector col5 = new NullableVarCharVector("col5", allocator);
        NullableVarCharVector col6 = new NullableVarCharVector("col6", allocator);
        NullableBitVector col7 = new NullableBitVector("col7", allocator);
        ){

      PivotDef pivot = PivotBuilder.getBlockDefinition(FluentIterable
          .from(ImmutableList.of(col1, col2, col3, col4, col5, col6, col7))
          .transform(new Function<FieldVector, FieldVectorPair>(){

            @Override
            public FieldVectorPair apply(FieldVector input) {
              return new FieldVectorPair(input, input);
            }})
          .toList());

      assertEquals(2, pivot.getVariableCount());
      assertEquals(
          4 + // allbits (8 total, 7 nullable + the bit vector)
          4 + //col1 (int)
          4 + //col2 (int)
          8 + //col3 (bigint)
          4 + //col4 (time)
          0 + // varchar
          0 + // varchar
          0 + // bitvector
          4 // varoffset
          ,
          pivot.getBlockWidth());

      assertEquals(8, pivot.getBitCount());

      assertEquals(ImmutableList.of(
          new VectorPivotDef(FieldType.FOUR_BYTE, 0, 0, 4, col1, col1),
          new VectorPivotDef(FieldType.FOUR_BYTE, 0, 1, 8, col2, col2),
          new VectorPivotDef(FieldType.EIGHT_BYTE, 0, 2, 12, col3, col3),
          new VectorPivotDef(FieldType.FOUR_BYTE, 0, 3, 20, col4, col4),
          new VectorPivotDef(FieldType.BIT, 0, 6, 7, col7, col7)
          ), pivot.getFixedPivots());

      assertEquals(ImmutableList.of(
          new VectorPivotDef(FieldType.VARIABLE, 0, 4, 0, col5, col5),
          new VectorPivotDef(FieldType.VARIABLE, 0, 5, 1, col6, col6)
          ), pivot.getVariablePivots());

    }
  }

}
