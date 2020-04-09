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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;
import com.google.common.base.Charsets;

import io.netty.buffer.ArrowBuf;

public class TestVarBinaryPivot extends BaseTestWithAllocator {

  static void populate(VarCharVector vector, String[] values){
    populate(vector, Arrays.stream(values).map(input -> {
      if (input == null) {
        return null;
      }
      return input.getBytes(Charsets.UTF_8);
    }).toArray(byte[][]::new));
  }

  static void populate(VarCharVector vector, byte[][] values){
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < values.length; i++){
      byte[] val = values[i];
      if(val != null){
        vector.setSafe(i, val, 0, val.length);
      } else {
        // add noise. this confirms that after pivot, noise is gone.
        byte[] bytes = new byte[r.nextInt(15)];
        r.nextBytes(bytes);
        vector.setSafe(i, bytes, 0, bytes.length);
        vector.setNull(i);
      }
    }
    vector.setValueCount(values.length);
  }

  static void populate(VarBinaryVector vector, byte[][] values){
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < values.length; i++){
      byte[] val = values[i];
      if(val != null){
        vector.setSafe(i, val, 0, val.length);
      } else {
        // add noise. this confirms that after pivot, noise is gone.
        byte[] bytes = new byte[r.nextInt(15)];
        r.nextBytes(bytes);
        vector.setSafe(i, bytes, 0, bytes.length);
        vector.setNull(i);
      }
    }
    vector.setValueCount(values.length);
  }

  private static void validateVarBinaryValues(int blockWidth, VectorPivotDef def, FixedBlockVector fixed, VariableBlockVector variable, byte[][] expected){
    int[] expectedNulls = new int[expected.length];
    byte[][] expectedValues = new byte[expected.length][];
    for(int i =0; i < expected.length; i++){
      byte[] e = expected[i];
      if(e != null){
        expectedNulls[i] = 1;
        expectedValues[i] = e;
      }
    }
    final int[] actualNulls = new int[expectedNulls.length];
    final byte[][] actualValues = new byte[expectedNulls.length][];
    final int nullBitOffset = def.getNullBitOffset();
    final int dataWidth = blockWidth - LBlockHashTable.VAR_OFFSET_SIZE;
    final ArrowBuf fixedBuf = fixed.getUnderlying();
    final ArrowBuf variableBuf = variable.getUnderlying();
    byte[] bytes = new byte[LargeMemoryUtil.checkedCastToInt(variableBuf.capacity())];
    variableBuf.getBytes(0, bytes);
    for(int i =0; i < expectedNulls.length; i++){
      actualNulls[i] =  (fixedBuf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffset) & 1;
      // now we need to read the varoffset
      int varOffset = fixedBuf.getInt( (i * blockWidth) + dataWidth);
      varOffset += 4; // move past variable length
      if(actualNulls[i] == 0) {
        continue;
      }


      // now we'll need to step over any values that are before the variable field we're interested in.
      int skip = def.getOffset();
      while(skip > 0){
        int skipLen = variableBuf.getInt(varOffset);
        varOffset += 4;
        varOffset += skipLen;
        skip--;
      }

      int len = variableBuf.getInt(varOffset);
      varOffset += 4;
      byte[] actual = new byte[len];
      variableBuf.getBytes(varOffset, actual, 0, len);
      actualValues[i] = actual;
    }

    assertArrayEquals(expectedNulls, actualNulls);

    assertArrayEquals(s(expectedValues), s(actualValues));
  }

  private static String[] s(byte[]... values){
    return Arrays.stream(values).map(input -> {
      if (input == null) {
        return null;
      }
      return new String(input, Charsets.UTF_8);
    }).toArray(String[]::new);
  }

  private static byte[] b(String s){
    return s.getBytes(Charsets.UTF_8);
  }

  @Test
  public void testVarBinaryPivot(){
    final byte[][] vectA = {b("hello"), null, b("my friend"), null, b("take a joy ride")};
    final byte[][] vectB = {null, b("you fool"), null, b("Cinderella"), b("Story")};

    try(
        VarBinaryVector col1 = new VarBinaryVector("col1", allocator);
        VarBinaryVector col2 = new VarBinaryVector("col2", allocator);){
      PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2));
      populate(col1, vectA);
      populate(col2, vectB);

      assertEquals(8, pivot.getBlockWidth());

      try(
          FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth());
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        Pivots.pivot(pivot, 5, fixed, variable);
        validateVarBinaryValues(pivot.getBlockWidth(), pivot.getVectorPivots().get(0), fixed, variable, vectA);
        validateVarBinaryValues(pivot.getBlockWidth(), pivot.getVectorPivots().get(1), fixed, variable, vectB);
      }
    }

  }
}
