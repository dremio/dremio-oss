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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * A vector container containing following types of vectors - IntVector, UnionVector<INT, FLOAT>
 *
 * <p>Used for unit tests in {@link TestVHashJoinSpillBuildAndReplay}
 *
 * @param <T>
 */
class UnionColumnGenerator<T extends FieldVector> implements Generator {

  private final int rows;
  private final VectorContainer result;
  private final List<T> vectors;
  private int offset;
  private final String postFix;
  private final BufferAllocator allocator;

  public UnionColumnGenerator(
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

    final Field unionField =
        CompleteType.union(CompleteType.INT.toField("int"), CompleteType.FLOAT.toField("float"))
            .toField("union_" + this.postFix);

    final T unionVector = result.addOrGet(unionField);
    vectorsBuilder.add(unionVector);

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
      if (i % 2 == 0) {
        insertIntIntoUnionVector(offset, i, (UnionVector) vectors.get(1));
      } else {
        insertFloatIntoUnionVector(offset, (float) i, (UnionVector) vectors.get(1));
      }
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

  private static void insertIntIntoUnionVector(
      final int index, final int value, final UnionVector vector) {
    final UnionWriter unionWriter = new UnionWriter(vector);

    unionWriter.setPosition(index);

    unionWriter.writeInt(value);
  }

  private static void insertFloatIntoUnionVector(
      final int index, final float value, final UnionVector vector) {
    final UnionWriter unionWriter = new UnionWriter(vector);

    unionWriter.setPosition(index);

    unionWriter.writeFloat4(value);
  }
}
