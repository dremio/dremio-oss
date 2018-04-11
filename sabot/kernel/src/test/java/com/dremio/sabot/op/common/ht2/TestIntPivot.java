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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.arrow.vector.NullableIntVector;
import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;

import io.netty.buffer.ArrowBuf;

public class TestIntPivot extends BaseTestWithAllocator {

  private static void validateIntValues(int blockWidth, VectorPivotDef def, FixedBlockVector block, Integer[] expected){
    int[] expectNulls = new int[expected.length];
    int[] expectValues = new int[expected.length];
    for(int i =0; i < expected.length; i++){
      Integer e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e;
      }
    }
    int[] actualNulls = new int[expectNulls.length];
    int[] actualValues = new int[expectNulls.length];
    int nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    for(int i =0; i < expectNulls.length; i++){
      actualNulls[i] =  (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
      actualValues[i] = buf.getInt((i * blockWidth) + def.getOffset());
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  static void populate(NullableIntVector vector, Integer[] values){
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < values.length; i++){
      Integer val = values[i];
      if(val != null){
        vector.setSafe(i, val);
      } else {
        // add noise so we make sure not to read/see noise.
        vector.setSafe(i, 0, r.nextInt());
      }
    }
    vector.setValueCount(values.length);
  }


  @Test
  public void testIntPivot(){
    final Integer[] vectA = {15, null, Integer.MAX_VALUE - 45, null, Integer.MIN_VALUE + 17};
    final Integer[] vectB = {null, 45, null, 65, 77};

    try(
        NullableIntVector col1 = new NullableIntVector("col1", allocator);
        NullableIntVector col2 = new NullableIntVector("col2", allocator);){
      PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2)
          );
      populate(col1, vectA);
      populate(col2, vectB);

      assertEquals(12, pivot.getBlockWidth());

      try(
          FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth());
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        Pivots.pivot(pivot, 5, fixed, variable);
        validateIntValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(0), fixed, vectA);
        validateIntValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(1), fixed, vectB);
      }
    }

  }



  @Test
  public void testIntPivotLarge(){
    Random r = new Random();
    final int count = 6000;
    final Integer[] vectA = new Integer[count];
    final Integer[] vectB = new Integer[count];

    for(int i =0; i < count; i++){
      if(r.nextBoolean()){
        vectA[i] = r.nextInt();
      }

      if(r.nextBoolean()){
        vectB[i] = r.nextInt();
      }
    }
    testDualIntVectors(count, vectA, vectB);

  }

  private void testDualIntVectors(int count, Integer[] vectA, Integer[] vectB){
    try(
        NullableIntVector col1 = new NullableIntVector("col1", allocator);
        NullableIntVector col2 = new NullableIntVector("col2", allocator);){
      PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2)
          );
      populate(col1, vectA);
      populate(col2, vectB);

      assertEquals(12, pivot.getBlockWidth());

      try(
          FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth());
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        Pivots.pivot(pivot, count, fixed, variable);
        validateIntValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(0), fixed, vectA);
        validateIntValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(1), fixed, vectB);
      }
    }

  }

  @Test
  public void testIntPivotLargeWithRuns(){
    Random r = new Random();
    final int count = 55555;
    final Integer[] vectA = new Integer[count];
    final Integer[] vectB = new Integer[count];

    boolean bool = false;
    for(int i =0; i < count; i++){
      if(i % 80 == 0){
        bool = r.nextBoolean();
      }

      if(bool){
        vectA[i] = r.nextInt();
      }

      if(r.nextBoolean()){
        vectB[i] = r.nextInt();
      }
    }

    testDualIntVectors(count, vectA, vectB);
  }
}
