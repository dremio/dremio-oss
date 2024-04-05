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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopier.Cursor;
import com.dremio.sabot.op.copier.FieldBufferCopierFactory;
import com.google.common.collect.ImmutableList;
import io.netty.util.internal.PlatformDependent;
import java.math.BigDecimal;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

public class TestCopierRoundTrip extends BaseTestOperator {

  private static void copy(List<FieldBufferCopier> copiers, SelectionVector2 sv2) {
    for (FieldBufferCopier fbc : copiers) {
      fbc.copy(sv2.memoryAddress(), sv2.getCount());
    }
  }

  private static void append(List<FieldBufferCopier> copiers, SelectionVector2 sv2) {
    int count = sv2.getCount();

    for (FieldBufferCopier fbc : copiers) {
      Cursor cursor = new Cursor();
      fbc.copy(sv2.memoryAddress(), count / 2, cursor);
      fbc.copy(sv2.memoryAddress() + count, count - count / 2, cursor);
    }
  }

  @Test
  public void intRoundtrip() {
    final int count = 1024;
    try (IntVector in = new IntVector("in", allocator);
        IntVector out = new IntVector("out", allocator); ) {

      in.allocateNew(count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void intAppend() {
    final int count = 1024;
    try (IntVector in = new IntVector("in", allocator);
        IntVector out = new IntVector("out", allocator); ) {

      in.allocateNew(count);
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }
        sv2.setRecordCount(count / 2);

        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintRoundtrip() {
    final int count = 1024;
    try (BigIntVector in = new BigIntVector("in", allocator);
        BigIntVector out = new BigIntVector("out", allocator); ) {

      in.allocateNew(count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintAppend() {
    final int count = 1024;
    try (BigIntVector in = new BigIntVector("in", allocator);
        BigIntVector out = new BigIntVector("out", allocator); ) {

      in.allocateNew(count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }
        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharRoundtrip() {
    final int count = 1024;
    try (VarCharVector in = new VarCharVector("in", allocator);
        VarCharVector out = new VarCharVector("out", allocator); ) {

      in.allocateNew(count * 8, count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          byte[] data = ("hello-" + i).getBytes(UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }

        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharAppend() {
    final int count = 1024;
    try (VarCharVector in = new VarCharVector("in", allocator);
        VarCharVector out = new VarCharVector("out", allocator); ) {

      in.allocateNew(count * 8, count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          byte[] data = ("hello-" + i).getBytes(UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void structRoundtrip() {
    final int count = 1024;

    final FieldType structType =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("string"), CompleteType.INT.toField("integer"))
            .toField("struct")
            .getFieldType();

    try (StructVector in = new StructVector("in", allocator, structType, null);
        StructVector out = new StructVector("out", allocator, structType, null);
        ArrowBuf tempBuf = allocator.buffer(2048); ) {

      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(stringField, intField));
      out.initializeChildrenFromFields(ImmutableList.of(stringField, intField));

      AllocationHelper.allocateNew(in, count);

      NullableStructWriter writer = in.getWriter();

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setIndexDefined(i);
          writer.setPosition(i);
          byte[] data = ("Item-" + (i / 5)).getBytes(UTF_8);
          tempBuf.setBytes(0, data, 0, data.length);
          writer.varChar("string").writeVarChar(0, data.length, tempBuf);
          writer.integer("integer").writeInt(i / 5);
        } else {
          in.setNull(i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void structAppend() {
    final int count = 1024;

    final FieldType structType =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("string"), CompleteType.INT.toField("integer"))
            .toField("struct")
            .getFieldType();

    try (StructVector in = new StructVector("in", allocator, structType, null);
        StructVector out = new StructVector("out", allocator, structType, null);
        ArrowBuf tempBuf = allocator.buffer(2048); ) {

      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(stringField, intField));
      out.initializeChildrenFromFields(ImmutableList.of(stringField, intField));

      AllocationHelper.allocateNew(in, count);

      NullableStructWriter writer = in.getWriter();

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setIndexDefined(i);
          writer.setPosition(i);
          byte[] data = ("Item-" + (i / 5)).getBytes(UTF_8);
          tempBuf.setBytes(0, data, 0, data.length);
          writer.varChar("string").writeVarChar(0, data.length, tempBuf);
          writer.integer("integer").writeInt(i / 5);
        } else {
          in.setNull(i);
        }
      }
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void boolInsideListRoundtrip() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.BIT.toField("bool")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField = new Field("bool", new FieldType(true, new ArrowType.Bool(), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          BitVector childVector = (BitVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), i % 10);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);
        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void boolInsideListAppend() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.BIT.toField("bool")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField = new Field("bool", new FieldType(true, new ArrowType.Bool(), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          BitVector childVector = (BitVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), i % 10);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void intInsideListRoundtrip() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.INT.toField("integer")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          IntVector childVector = (IntVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), i / 5);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);
        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void intInsideListAppend() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.INT.toField("integer")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          IntVector childVector = (IntVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), i / 5);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void decimalInsideListRoundtrip() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.DECIMAL.toField("decimal")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField =
          new Field("decimal", new FieldType(true, new ArrowType.Decimal(38, 10, 128), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          DecimalVector childVector = (DecimalVector) in.getDataVector();
          childVector.setSafe(
              childVector.getValueCount(),
              new BigDecimal("1234567891234567891234567891.0123456789"));
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);
        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void decimalInsideListAppend() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.DECIMAL.toField("decimal")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field intField =
          new Field("decimal", new FieldType(true, new ArrowType.Decimal(38, 10, 128), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(intField));
      out.initializeChildrenFromFields(ImmutableList.of(intField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          DecimalVector childVector = (DecimalVector) in.getDataVector();
          childVector.setSafe(
              childVector.getValueCount(),
              new BigDecimal("1234567891234567891234567891.0123456789"));
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharInsideListRoundtrip() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.VARCHAR.toField("string")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(stringField));
      out.initializeChildrenFromFields(ImmutableList.of(stringField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          VarCharVector childVector = (VarCharVector) in.getDataVector();
          for (int j = 0; j < 2; j++) {
            byte[] data = ("Item-" + (i / 5 + j)).getBytes(UTF_8);
            childVector.setSafe((i / 5 * 2 + j), data, 0, data.length);
          }
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 2);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void varcharInsideListAppend() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.VARCHAR.toField("string")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(stringField));
      out.initializeChildrenFromFields(ImmutableList.of(stringField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          VarCharVector childVector = (VarCharVector) in.getDataVector();
          for (int j = 0; j < 2; j++) {
            byte[] data = ("Item-" + (i / 5 + j)).getBytes(UTF_8);
            childVector.setSafe((i / 5 * 2 + j), data, 0, data.length);
          }
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 2);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintInsideListRoundtrip() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.BIGINT.toField("bigInt")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field bigIntField =
          new Field("integer", new FieldType(true, new ArrowType.Int(64, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(bigIntField));
      out.initializeChildrenFromFields(ImmutableList.of(bigIntField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          BigIntVector childVector = (BigIntVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), (long) i / 5);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void bigintInsideListAppend() {
    final int count = 1024;

    final FieldType listType =
        new CompleteType(
                ArrowType.List.INSTANCE, ImmutableList.of(CompleteType.BIGINT.toField("bigInt")))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null); ) {

      Field bigIntField =
          new Field("integer", new FieldType(true, new ArrowType.Int(64, true), null), null);

      in.initializeChildrenFromFields(ImmutableList.of(bigIntField));
      out.initializeChildrenFromFields(ImmutableList.of(bigIntField));

      AllocationHelper.allocateNew(in, count);

      int offsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          offsetVal = i / 5;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal);
          BigIntVector childVector = (BigIntVector) in.getDataVector();
          childVector.setSafe(childVector.getValueCount(), (long) i / 5);
          childVector.setValueCount(childVector.getValueCount() + 1);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, offsetVal + 1);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, offsetVal + 1);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void structInsideListRoundtrip() {
    final int count = 1024;

    final Field structType =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("string"), CompleteType.INT.toField("integer"))
            .toField("struct");

    final FieldType listType =
        new CompleteType(ArrowType.List.INSTANCE, ImmutableList.of(structType))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null);
        ArrowBuf tempBuf = allocator.buffer(2048); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);
      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field structField =
          new Field(
              "struct",
              new FieldType(true, new ArrowType.Struct(), null),
              ImmutableList.of(stringField, intField));

      in.initializeChildrenFromFields(ImmutableList.of(structField));
      out.initializeChildrenFromFields(ImmutableList.of(structField));

      AllocationHelper.allocateNew(in, count);

      int outerOffsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          outerOffsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal);

          StructVector childVector = (StructVector) in.getDataVector();
          NullableStructWriter structWriter = childVector.getWriter();
          for (int j = 0; j < 2; j++) {
            childVector.setIndexDefined(i / 5 * 2 + j);
            structWriter.setPosition(i / 5 * 2 + j);
            byte[] data = ("Item-" + (i / 5 + j)).getBytes(UTF_8);
            tempBuf.setBytes(0, data, 0, data.length);
            structWriter.varChar("string").writeVarChar(0, data.length, tempBuf);
            structWriter.integer("integer").writeInt(i / 5 + j);
            childVector.setValueCount(childVector.getValueCount() + 1);
          }
          in.setLastSet(i);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void structInsideListAppend() {
    final int count = 1024;

    final Field structType =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("string"), CompleteType.INT.toField("integer"))
            .toField("struct");

    final FieldType listType =
        new CompleteType(ArrowType.List.INSTANCE, ImmutableList.of(structType))
            .toField("list")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, listType, null);
        ListVector out = new ListVector("out", allocator, listType, null);
        ArrowBuf tempBuf = allocator.buffer(2048); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);
      Field stringField =
          new Field("string", new FieldType(true, new ArrowType.Utf8(), null), null);
      Field structField =
          new Field(
              "struct",
              new FieldType(true, new ArrowType.Struct(), null),
              ImmutableList.of(stringField, intField));

      in.initializeChildrenFromFields(ImmutableList.of(structField));
      out.initializeChildrenFromFields(ImmutableList.of(structField));

      AllocationHelper.allocateNew(in, count);

      int outerOffsetVal = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          outerOffsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal);

          StructVector childVector = (StructVector) in.getDataVector();
          NullableStructWriter structWriter = childVector.getWriter();
          for (int j = 0; j < 2; j++) {
            childVector.setIndexDefined(i / 5 * 2 + j);
            structWriter.setPosition(i / 5 * 2 + j);
            byte[] data = ("Item-" + (i / 5 + j)).getBytes(UTF_8);
            tempBuf.setBytes(0, data, 0, data.length);
            structWriter.varChar("string").writeVarChar(0, data.length, tempBuf);
            structWriter.integer("integer").writeInt(i / 5 + j);
            childVector.setValueCount(childVector.getValueCount() + 1);
          }
          in.setLastSet(i);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
        }
      }
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void listInsideListRoundtrip() {
    final int count = 1024;

    final FieldType outerlistType =
        new CompleteType(
                ArrowType.List.INSTANCE,
                ImmutableList.of(
                    new CompleteType(
                            ArrowType.List.INSTANCE,
                            ImmutableList.of(CompleteType.INT.toField("integer")))
                        .toField("innerList")))
            .toField("outerList")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, outerlistType, null);
        ListVector out = new ListVector("out", allocator, outerlistType, null); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);
      Field innerList =
          new Field(
              "innerList",
              new FieldType(true, new ArrowType.List(), null),
              ImmutableList.of(intField));

      in.initializeChildrenFromFields(ImmutableList.of(innerList));
      out.initializeChildrenFromFields(ImmutableList.of(innerList));

      AllocationHelper.allocateNew(in, count);

      int outerOffsetVal = 0;
      int childBufTracker = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          outerOffsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal);
          // Set the inner child ListVector
          ListVector childVector = (ListVector) in.getDataVector();
          for (int j = 0; j < 2; j++) {
            childVector.setNotNull(childBufTracker++);
            childVector
                .getOffsetBuffer()
                .setInt((outerOffsetVal + j) * ListVector.OFFSET_WIDTH, (outerOffsetVal + j) * 2);
          }
          // Set the underlying int data vector inside the child vector
          IntVector innerDataVector = (IntVector) childVector.getDataVector();
          for (int j = 0; j < 4; j++) {
            innerDataVector.setSafe(innerDataVector.getValueCount(), i / 5 + j);
            innerDataVector.setValueCount(innerDataVector.getValueCount() + 1);
          }
          childVector.setLastSet(outerOffsetVal + 1);
          in.setLastSet(i);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
        }
      }
      in.getDataVector()
          .getOffsetBuffer()
          .setInt((count / 5 * 2 + 2) * ListVector.OFFSET_WIDTH, (count / 5 * 2 + 2) * 2);
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count);
        // create full sv2.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count * 2; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x++;
        }
        sv2.setRecordCount(count);
        copy(copiers, sv2);

        out.setValueCount(count);
        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }

  @Test
  public void listInsideListAppend() {
    final int count = 1024;

    final FieldType outerlistType =
        new CompleteType(
                ArrowType.List.INSTANCE,
                ImmutableList.of(
                    new CompleteType(
                            ArrowType.List.INSTANCE,
                            ImmutableList.of(CompleteType.INT.toField("integer")))
                        .toField("innerList")))
            .toField("outerList")
            .getFieldType();

    try (ListVector in = new ListVector("in", allocator, outerlistType, null);
        ListVector out = new ListVector("out", allocator, outerlistType, null); ) {

      Field intField =
          new Field("integer", new FieldType(true, new ArrowType.Int(32, true), null), null);
      Field innerList =
          new Field(
              "innerList",
              new FieldType(true, new ArrowType.List(), null),
              ImmutableList.of(intField));

      in.initializeChildrenFromFields(ImmutableList.of(innerList));
      out.initializeChildrenFromFields(ImmutableList.of(innerList));

      AllocationHelper.allocateNew(in, count);

      int outerOffsetVal = 0;
      int childBufTracker = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setNotNull(i);
          outerOffsetVal = i / 5 * 2;
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal);
          // Set the inner child ListVector
          ListVector childVector = (ListVector) in.getDataVector();
          for (int j = 0; j < 2; j++) {
            childVector.setNotNull(childBufTracker++);
            childVector
                .getOffsetBuffer()
                .setInt((outerOffsetVal + j) * ListVector.OFFSET_WIDTH, (outerOffsetVal + j) * 2);
          }
          // Set the underlying int data vector inside the child vector
          IntVector innerDataVector = (IntVector) childVector.getDataVector();
          for (int j = 0; j < 4; j++) {
            innerDataVector.setSafe(innerDataVector.getValueCount(), i / 5 + j);
            innerDataVector.setValueCount(innerDataVector.getValueCount() + 1);
          }
          childVector.setLastSet(outerOffsetVal + 1);
          in.setLastSet(i);
        } else {
          in.setNull(i);
          in.getOffsetBuffer().setInt(i * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
        }
      }
      in.getDataVector()
          .getOffsetBuffer()
          .setInt((count / 5 * 2 + 2) * ListVector.OFFSET_WIDTH, (count / 5 * 2 + 2) * 2);
      in.getOffsetBuffer().setInt(count * ListVector.OFFSET_WIDTH, outerOffsetVal + 2);
      in.setValueCount(count);

      List<FieldBufferCopier> copiers =
          new FieldBufferCopierFactory(testContext.getOptions())
              .getTwoByteCopiers(ImmutableList.of(in), ImmutableList.of(out));
      try (final SelectionVector2 sv2 = new SelectionVector2(allocator); ) {

        sv2.allocateNew(count / 2);
        // set alternate elements.
        int x = 0;
        for (long mem = sv2.memoryAddress(); mem < sv2.memoryAddress() + count; mem += 2) {
          PlatformDependent.putShort(mem, (short) (char) x);
          x += 2;
        }

        sv2.setRecordCount(count / 2);
        append(copiers, sv2);

        out.setValueCount(count / 2);
        for (int i = 0; i < count / 2; i++) {
          assertEquals(in.getObject(i * 2), out.getObject(i));
        }
      }
    }
  }
}
