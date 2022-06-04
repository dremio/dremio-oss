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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopierFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

public class TestCopier6RoundTrip extends BaseTestOperator {
  private static final int SV6_SIZE = 6;

  private static void copy(List<FieldBufferCopier> copiers, long offsetAddr, int count){
    for(FieldBufferCopier fbc : copiers){
      fbc.copy(offsetAddr, count);
    }
  }

  private static void append(List<FieldBufferCopier> copiers, long offsetAddr, int count) {
    for (FieldBufferCopier fbc : copiers) {
      FieldBufferCopier.Cursor cursor = new FieldBufferCopier.Cursor();
      fbc.copy(offsetAddr, count / 2, cursor);
      fbc.copy(offsetAddr + (count * SV6_SIZE) / 2, count - count / 2, cursor);
    }
  }

  private void fillSV6Full(ArrowBuf sv6, int firstBatchCount, int secondBatchCount) {
    int totalCount = firstBatchCount + secondBatchCount;
    for (int idx = 0; idx < totalCount; ++idx) {
      int batchIdx = idx < firstBatchCount ? 0 : 1;
      int recordIdxInBatch = idx < firstBatchCount ? idx : idx - firstBatchCount;

      long mem = sv6.memoryAddress() + idx * SV6_SIZE;
      PlatformDependent.putInt(mem, batchIdx);
      PlatformDependent.putShort(mem + 4, (short)recordIdxInBatch);
    }
  }

  private void fillSV6Alternate(ArrowBuf sv6, int firstBatchCount, int secondBatchCount) {
    int totalCount = (firstBatchCount + secondBatchCount) / 2;
    for (int idx = 0; idx < totalCount; ++idx) {
      int actualIdx = idx * 2;
      int batchIdx = actualIdx < firstBatchCount ? 0 : 1;
      int recordIdxInBatch = actualIdx < firstBatchCount ? actualIdx : actualIdx - firstBatchCount;

      long mem = sv6.memoryAddress() + idx * SV6_SIZE;
      PlatformDependent.putInt(mem, batchIdx);
      PlatformDependent.putShort(mem + 4, (short)recordIdxInBatch);
    }
  }

  @Test
  public void intRoundTrip(){
    try(
      IntVector in1 = new IntVector("in1", allocator);
      IntVector in2 = new IntVector("in2", allocator);
      IntVector out = new IntVector("out", allocator);
    ){
      IntVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        IntVector vec = in[vecIdx];
        vec.allocateNew(count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          if (i % 5 == 0) {
            vec.setSafe(i, i);
          }
        }
        vec.setValueCount(count[vecIdx]);
      }

      int totalCount = count[0] + count[1];
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try(
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ){
        // create full sv6.
        fillSV6Full(sv6, count[0], count[1]);

        // do the copy
        copy(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int batchIdx = idx < count[0] ? 0 : 1;
          int recordIdxInBatch = idx < count[0] ? idx : idx - count[0];
          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void intAppend(){
    try(
      IntVector in1 = new IntVector("in1", allocator);
      IntVector in2 = new IntVector("in2", allocator);
      IntVector out = new IntVector("out", allocator);
    ){

      IntVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        IntVector vec = in[vecIdx];
        vec.allocateNew(count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          if (i % 5 == 0) {
            vec.setSafe(i, i);
          }
        }
        vec.setValueCount(count[vecIdx]);
      }

      // set alternate elements.
      int totalCount = (count[0] + count[1]) / 2;
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try(
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ){
        fillSV6Alternate(sv6, count[0], count[1]);

        // do the copy
        append(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int actualIdx = idx * 2;
          int batchIdx = actualIdx < count[0] ? 0 : 1;
          int recordIdxInBatch = actualIdx < count[0] ? actualIdx : actualIdx - count[0];

          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void varcharRoundTrip(){
    try(
      VarCharVector in1 = new VarCharVector("in1", allocator);
      VarCharVector in2 = new VarCharVector("in2", allocator);
      VarCharVector out = new VarCharVector("out", allocator);
    ){
      VarCharVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        VarCharVector vec = in[vecIdx];
        vec.allocateNew(count[vecIdx] * 8, count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          if (i % 5 == 0) {
            byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
            vec.setSafe(i, data, 0, data.length);
          }
        }
        vec.setValueCount(count[vecIdx]);
      }

      int totalCount = count[0] + count[1];
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try(
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ){
        // create full sv6.
        fillSV6Full(sv6, count[0], count[1]);

        // do the copy
        copy(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int batchIdx = idx < count[0] ? 0 : 1;
          int recordIdxInBatch = idx < count[0] ? idx : idx - count[0];
          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void varcharAppend(){
    try(
      VarCharVector in1 = new VarCharVector("in1", allocator);
      VarCharVector in2 = new VarCharVector("in2", allocator);
      VarCharVector out = new VarCharVector("out", allocator);
    ){
      VarCharVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        VarCharVector vec = in[vecIdx];
        vec.allocateNew(count[vecIdx] * 8, count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          if (i % 5 == 0) {
            byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
            vec.setSafe(i, data, 0, data.length);
          }
        }
        vec.setValueCount(count[vecIdx]);
      }

      // set alternate elements.
      int totalCount = (count[0] + count[1]) / 2;
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try(
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ){
        fillSV6Alternate(sv6, count[0], count[1]);

        // do the copy
        append(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int actualIdx = idx * 2;
          int batchIdx = actualIdx < count[0] ? 0 : 1;
          int recordIdxInBatch = actualIdx < count[0] ? actualIdx : actualIdx - count[0];

          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void bitAppend(){
    try(
      BitVector in1 = new BitVector("in1", allocator);
      BitVector in2 = new BitVector("in2", allocator);
      BitVector out = new BitVector("out", allocator);
    ){
      BitVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        BitVector vec = in[vecIdx];
        vec.allocateNew(count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          if (i % 5 == 0) {
            vec.setSafe(i, vecIdx);
          }
        }
        vec.setValueCount(count[vecIdx]);
      }

      // set alternate elements.
      int totalCount = (count[0] + count[1]) / 2;
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteConditionalCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try(
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ){
        fillSV6Alternate(sv6, count[0], count[1]);

        // do the copy
        append(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int actualIdx = idx * 2;
          int batchIdx = actualIdx < count[0] ? 0 : 1;
          int recordIdxInBatch = actualIdx < count[0] ? actualIdx : actualIdx - count[0];

          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void structRoundtrip() {
    final FieldType structType = CompleteType.struct(
      CompleteType.VARCHAR.toField("string"),
      CompleteType.INT.toField("integer")
    ).toField("struct").getFieldType();

    try (
      StructVector in1 = new StructVector("in1", allocator, structType, null);
      StructVector in2 = new StructVector("in2", allocator, structType, null);
      StructVector out = new StructVector("out", allocator, structType, null);
      ArrowBuf tempBuf = allocator.buffer(2048);
    ) {

      Field stringField = new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field intField = new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      StructVector[] in = {in1, in2};
      int[] count = {512, 512};
      int totalCount = count[0] + count[1];

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        in[vecIdx].initializeChildrenFromFields(ImmutableList.of(stringField, intField));
        AllocationHelper.allocateNew(in[vecIdx], count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          NullableStructWriter writer = in[vecIdx].getWriter();
          if (i % 5 == 0) {
            in[vecIdx].setIndexDefined(i);
            writer.setPosition(i);
            byte[] data = ("Item-" + (i / 5)).getBytes(Charsets.UTF_8);
            tempBuf.setBytes(0, data, 0, data.length);
            writer.varChar("string").writeVarChar(0, data.length, tempBuf);
            writer.integer("integer").writeInt(i / 5);
          } else {
            in[vecIdx].setNull(i);
          }
          in[vecIdx].setValueCount(count[vecIdx]);
        }
      }

      out.initializeChildrenFromFields(ImmutableList.of(stringField, intField));
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ) {
        fillSV6Full(sv6, count[0], count[1]);

        // do the copy
        copy(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int batchIdx = idx < count[0] ? 0 : 1;
          int recordIdxInBatch = idx < count[0] ? idx : idx - count[0];
          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }

  @Test
  public void structAppend() {
    final FieldType structType = CompleteType.struct(
      CompleteType.VARCHAR.toField("string"),
      CompleteType.INT.toField("integer")
    ).toField("struct").getFieldType();

    try (
      StructVector in1 = new StructVector("in1", allocator, structType, null);
      StructVector in2 = new StructVector("in2", allocator, structType, null);
      StructVector out = new StructVector("out", allocator, structType, null);
      ArrowBuf tempBuf = allocator.buffer(2048);
    ) {

      Field stringField = new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field intField = new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      StructVector[] in = {in1, in2};
      int[] count = {512, 512};

      for (int vecIdx = 0; vecIdx < 2; ++vecIdx) {
        in[vecIdx].initializeChildrenFromFields(ImmutableList.of(stringField, intField));
        AllocationHelper.allocateNew(in[vecIdx], count[vecIdx]);
        for (int i = 0; i < count[vecIdx]; i++) {
          NullableStructWriter writer = in[vecIdx].getWriter();
          if (i % 5 == 0) {
            in[vecIdx].setIndexDefined(i);
            writer.setPosition(i);
            byte[] data = ("Item-" + (i / 5)).getBytes(Charsets.UTF_8);
            tempBuf.setBytes(0, data, 0, data.length);
            writer.varChar("string").writeVarChar(0, data.length, tempBuf);
            writer.integer("integer").writeInt(i / 5);
          } else {
            in[vecIdx].setNull(i);
          }
          in[vecIdx].setValueCount(count[vecIdx]);
        }
      }

      out.initializeChildrenFromFields(ImmutableList.of(stringField, intField));
      int totalCount = (count[0] + count[1]) / 2;
      List<FieldBufferCopier> copiers = new FieldBufferCopierFactory(testContext.getOptions()).getSixByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (
        final ArrowBuf sv6 = allocator.buffer(SV6_SIZE * totalCount);
      ) {
        // set alternate elements.
        fillSV6Alternate(sv6, count[0], count[1]);

        // do the copy
        append(copiers, sv6.memoryAddress(), totalCount);
        out.setValueCount(totalCount);

        // verify
        for (int idx = 0; idx < totalCount; ++idx) {
          int actualIdx = idx * 2;
          int batchIdx = actualIdx < count[0] ? 0 : 1;
          int recordIdxInBatch = actualIdx < count[0] ? actualIdx : actualIdx - count[0];

          assertEquals(in[batchIdx].getObject(recordIdxInBatch), out.getObject(idx));
        }
      }
    }
  }
}
