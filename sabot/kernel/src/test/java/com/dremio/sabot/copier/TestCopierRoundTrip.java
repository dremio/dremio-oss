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
package com.dremio.sabot.copier;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.BaseTestWithAllocator;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopier.Cursor;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

public class TestCopierRoundTrip extends BaseTestWithAllocator {

  private static void copy(List<FieldBufferCopier> copiers, SelectionVector2 sv2){
    for(FieldBufferCopier fbc : copiers){
      fbc.copy(sv2.memoryAddress(), sv2.getCount());
    }
  }

  private static void append(List<FieldBufferCopier> copiers, SelectionVector2 sv2) {
    int count = sv2.getCount();

    for (FieldBufferCopier fbc : copiers) {
      Cursor cursor = fbc.copy(sv2.memoryAddress(), count / 2, null);
      fbc.copy(sv2.memoryAddress() + count, count - count / 2, cursor);
    }
  }

  @Test
  public void intRoundtrip(){
    final int count = 1024;
    try(
        IntVector in = new IntVector("in", allocator);
        IntVector out = new IntVector("out", allocator);
        ){

      in.allocateNew(count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

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

        out.setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }


  @Test
  public void intAppend(){
    final int count = 1024;
    try(
      IntVector in = new IntVector("in", allocator);
      IntVector out = new IntVector("out", allocator);
    ){

      in.allocateNew(count);
      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
        final SelectionVector2 sv2 = new SelectionVector2(allocator);
      ){

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }
        sv2.setRecordCount(count);

        append(copiers, sv2);

        out.setValueCount(count / 2);
        for(int i =0; i < count / 2; i++){
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintRoundtrip(){
    final int count = 1024;
    try(
        BigIntVector in = new BigIntVector("in", allocator);
        BigIntVector out = new BigIntVector("out", allocator);
        ){

      in.allocateNew(count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

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

        out.setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintAppend(){
    final int count = 1024;
    try(
      BigIntVector in = new BigIntVector("in", allocator);
      BigIntVector out = new BigIntVector("out", allocator);
    ){

      in.allocateNew(count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
        final SelectionVector2 sv2 = new SelectionVector2(allocator);
      ){

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }
        sv2.setRecordCount(count);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for(int i =0; i < count / 2; i++){
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharRoundtrip(){
    final int count = 1024;
    try(
        VarCharVector in = new VarCharVector("in", allocator);
        VarCharVector out = new VarCharVector("out", allocator);
        ){

      in.allocateNew(count * 8, count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

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

        out.setValueCount(count);
        for(int i =0; i < count; i++){
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharAppend(){
    final int count = 1024;
    try(
      VarCharVector in = new VarCharVector("in", allocator);
      VarCharVector out = new VarCharVector("out", allocator);
    ){

      in.allocateNew(count * 8, count);

      for(int i = 0; i < count; i++){
        if(i % 5 == 0){
          byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers = FieldBufferCopier.getCopiers(ImmutableList.<FieldVector>of(in), ImmutableList.<FieldVector>of(out));
      try(
        final SelectionVector2 sv2 = new SelectionVector2(allocator);
      ){

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for(long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem+=2){
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for(int i =0; i < count / 2; i++){
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }
}
