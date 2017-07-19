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
package com.dremio.sabot.copier;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.junit.Test;

import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.BaseTestWithAllocator;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

public class TestCopierRoundTrip extends BaseTestWithAllocator {

  private static void copy(List<FieldBufferCopier> copiers, SelectionVector2 sv2){
    for(FieldBufferCopier fbc : copiers){
      fbc.copy(sv2.memoryAddress(), sv2.getCount());
    }
  }

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
          in.getMutator().setSafe(i, i);
        }
      }
      in.getMutator().setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
          final SelectionVector2 sv2 = new SelectionVector2(allocator);
          ){

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.getMutator().setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getAccessor().getObject(i), out.getAccessor().getObject(i));
        }
      }
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
          in.getMutator().setSafe(i, i);
        }
      }
      in.getMutator().setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
          final SelectionVector2 sv2 = new SelectionVector2(allocator);
          ){

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.getMutator().setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getAccessor().getObject(i), out.getAccessor().getObject(i));
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
          in.getMutator().setSafe(i, data, 0, data.length);
        }
      }
      in.getMutator().setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
          final SelectionVector2 sv2 = new SelectionVector2(allocator);
          ){

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }

        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.getMutator().setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getAccessor().getObject(i), out.getAccessor().getObject(i));
        }
      }
    }
  }
}
