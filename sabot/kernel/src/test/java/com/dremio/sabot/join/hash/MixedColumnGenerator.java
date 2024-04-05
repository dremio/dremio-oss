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
package com.dremio.sabot.join.hash;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.Generator;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * A vector cotainer containing following types of vectors - IntVector, IntVector, StructVector
 *
 * <p>Used for unit tests in {@link TestVHashJoinSpillBuildAndReplay}
 *
 * @param <T>
 */
class MixedColumnGenerator<T extends FieldVector> implements Generator {
  private final int rows;
  private final VectorContainer result;
  private final List<T> vectors;
  private int offset;
  private final String postFix;
  private final BufferAllocator allocator;

  public MixedColumnGenerator(
      final BufferAllocator allocator, final int rows, final int offset, final String postFix) {

    this.allocator = allocator;
    this.rows = rows;
    result = new VectorContainer(allocator);

    this.offset = offset;
    this.postFix = postFix;

    final ImmutableList.Builder<T> vectorsBuilder = ImmutableList.builder();

    final Field fieldIdLeft =
        new Field(
            "id_" + this.postFix, new FieldType(true, new ArrowType.Int(32, true), null), null);

    final T idLeftVector = result.addOrGet(fieldIdLeft);
    vectorsBuilder.add(idLeftVector);

    final Field fieldInt =
        new Field(
            "int_" + this.postFix, new FieldType(true, new ArrowType.Int(32, true), null), null);

    final T intVector = result.addOrGet(fieldInt);
    vectorsBuilder.add(intVector);

    final Field fieldStruct =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("child_string"), CompleteType.INT.toField("child_int"))
            .toField("struct_" + this.postFix);

    final T structVector = result.addOrGet(fieldStruct);
    vectorsBuilder.add(structVector);

    this.vectors = vectorsBuilder.build();

    result.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  /**
   * Generate a new record.
   *
   * @param records
   * @return
   */
  @Override
  public int next(final int records) {
    final int count = Math.min(rows - offset, records);

    if (count <= 0) {
      return 0;
    }

    result.allocateNew();
    for (int i = 0; i < count; i++) {

      insertIntoIntVector(offset, offset, (BaseFixedWidthVector) vectors.get(0));

      int value = "left".equalsIgnoreCase(postFix) ? offset : offset + rows;
      insertIntoIntVector(offset, value, (BaseFixedWidthVector) vectors.get(1));

      insertIntoStructVector(allocator, offset, value, (StructVector) vectors.get(2));

      offset++;
    }
    result.setAllCount(count);
    result.buildSchema();
    return count;
  }

  @Override
  public VectorAccessible getOutput() {
    return result;
  }

  @Override
  public void close() throws Exception {
    result.close();
  }

  private static void insertIntoIntVector(
      final int index, final int value, final BaseFixedWidthVector vector) {
    IntVector vec = (IntVector) vector;
    vec.setSafe(index, value);
  }

  private static void insertIntoStructVector(
      final BufferAllocator allocator,
      final int index,
      final int value,
      final StructVector vector) {

    final NullableStructWriter structWriter = vector.getWriter();

    try (final ArrowBuf tempBuf = allocator.buffer(1024)) {

      structWriter.setPosition(index);

      structWriter.start();

      structWriter.integer("child_int").writeInt(value);

      final byte[] varCharVal = Integer.toString(value).getBytes();
      tempBuf.setBytes(0, varCharVal);
      structWriter.varChar("child_string").writeVarChar(0, varCharVal.length, tempBuf);

      structWriter.end();
    }
  }
}
