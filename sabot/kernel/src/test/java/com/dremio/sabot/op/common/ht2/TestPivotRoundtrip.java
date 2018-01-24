/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.BaseTestWithAllocator;
import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;

public class TestPivotRoundtrip extends BaseTestWithAllocator {


  @Test
  public void intRoundtrip(){
    final int count = 1024;
    try(
        NullableIntVector in = new NullableIntVector("in", allocator);
        NullableIntVector out = new NullableIntVector("out", allocator);
        ){

      in.allocateNew(count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try(
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot4Bytes(pivot.getFixedPivots().get(0), fbv, count);
        Unpivots.unpivot(pivot, fbv, vbv, count);

        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }


  @Test
  public void intManyRoundtrip() throws Exception{
    final int count = 1024;
    final int mult = 80;
    NullableIntVector[] in = new NullableIntVector[mult];
    NullableIntVector[] out = new NullableIntVector[mult];
    List<FieldVectorPair> pairs = new ArrayList<>();
    try {

      for(int x =0; x < mult; x++){
        NullableIntVector inv = new NullableIntVector("in", allocator);
        in[x] = inv;
        inv.allocateNew(count);
        NullableIntVector outv = new NullableIntVector("out", allocator);
        out[x] = outv;
        for(int i = 0; i < count; i++){
          if(i % 5 == 0){
            inv.setSafe(i, Integer.MAX_VALUE - i);
          }
        }
        inv.setValueCount(count);
        pairs.add(new FieldVectorPair(inv, outv));
      }

      final PivotDef pivot = PivotBuilder.getBlockDefinition(pairs);
      try(
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        fbv.ensureAvailableBlocks(count);

        for(int x = 0; x < mult; x++){
          Pivots.pivot4Bytes(pivot.getFixedPivots().get(x), fbv, count);
        }

        Unpivots.unpivot(pivot, fbv, vbv, count);

        for(int x = 0; x < mult; x++){
          NullableIntVector inv = in[x];
          NullableIntVector outv = out[x];
          for(int i =0; i < count; i++){
            assertEquals("Field: " + x, inv.getObject(i), outv.getObject(i));
          }
        }
      }
    } finally {
      AutoCloseables.close(FluentIterable.of(in));
      AutoCloseables.close(FluentIterable.of(out));
    }
  }



  @Test
  public void bigintRoundtrip(){
    final int count = 1024;
    try(
        NullableBigIntVector in = new NullableBigIntVector("in", allocator);
        NullableBigIntVector out = new NullableBigIntVector("out", allocator);
        ){

      in.allocateNew(count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try(
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        Unpivots.unpivot(pivot, fbv, vbv, count);

        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }


  @Test
  public void varcharRoundtrip(){
    final int count = 1024;
    try(
        NullableVarCharVector in = new NullableVarCharVector("in", allocator);
        NullableVarCharVector out = new NullableVarCharVector("out", allocator);
        ){

      in.allocateNew(count * 8, count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try(
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
          ){
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);
        Unpivots.unpivot(pivot, fbv, vbv, count);

        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }
}
