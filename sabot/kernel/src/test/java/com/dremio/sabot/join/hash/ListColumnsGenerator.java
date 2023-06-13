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


import static com.dremio.common.expression.CompleteType.LIST;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.Generator;
import com.google.common.collect.ImmutableList;

/**
 * A vector container containing following types of vectors -
 * IntVector, ListVector of Int, ListVector of String.
 * Used for unit tests in {@link TestVHashJoinSpillBuildAndReplay}
 * @param <T>
 */
class ListColumnsGenerator<T extends FieldVector> implements Generator {

  private final int rows;
  private final VectorContainer result;
  private final List<T> vectors;

  private int offset;

  private final String postFix;

  private final BufferAllocator allocator;


  public ListColumnsGenerator(final BufferAllocator allocator, final int rows, final int offset, final String postFix) {

    this.allocator = allocator;
    this.rows = rows;
    result = new VectorContainer(allocator);

    this.offset = offset;
    this.postFix = postFix;

    final ImmutableList.Builder<T> vectorsBuilder = ImmutableList.builder();

    final Field fieldId = new Field("id_"+ this.postFix, new FieldType(true,
      new ArrowType.Int(32, true), null), null);

    final T idLeftVector = result.addOrGet(fieldId);
    vectorsBuilder.add(idLeftVector);



    final List<Field> childrenField1 = ImmutableList.of(CompleteType.INT.toField("integerCol"));

    final Field fieldIntList = new Field("ints_" + this.postFix, FieldType.nullable(LIST.getType()), childrenField1);

    final T intListVector = result.addOrGet(fieldIntList);
    vectorsBuilder.add(intListVector);

    final List<Field> childrenField2 = ImmutableList.of(CompleteType.VARCHAR.toField("strCol"));

    final Field fieldStringList = new Field("strings_" + this.postFix, FieldType.nullable(LIST.getType()), childrenField2);

    final T stringListVector = result.addOrGet(fieldStringList);
    vectorsBuilder.add(stringListVector);

    this.vectors = vectorsBuilder.build();

    result.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  /**
   * Generate a new record.
   * @param records
   * @return
   */
  @Override
  public int next(final int records) {


    final int count = Math.min(rows - offset, records);

    if (count <= 0) {
      return 0;
    }

    final List<List<Integer>> intValues = getIntList(count);
    final List<List<String>> stringValues = getStringList(count);

    result.allocateNew();
    for (int i = 0; i < count; i++) {

      insertIntoIntVector(offset, offset, (BaseFixedWidthVector) vectors.get(0));


      insetIntoIntListVector(intValues);

      insetIntoStringListVector(stringValues);

      offset++;
    }

    result.setAllCount(count);
    result.buildSchema();
    return count;
  }

  private void insetIntoIntListVector(List<List<Integer>> intValues) {
    final UnionListWriter listWriter = new UnionListWriter((ListVector) vectors.get(1));

    listWriter.setPosition(offset);
    listWriter.startList();

    for (List<Integer> intList : intValues) {
      for (int num : intList) {
        listWriter.writeInt(num);
      }

    }

    listWriter.endList();
  }

  private void insetIntoStringListVector(final List<List<String>> strValues) {
    final UnionListWriter listWriter = new UnionListWriter((ListVector) vectors.get(2));
    try (final ArrowBuf tempBuf =  allocator.buffer(1024)) {
      listWriter.setPosition(offset);
      listWriter.startList();
      for (List<String> intList : strValues) {
        for (String str : intList) {
          final byte[] varCharVal = str.getBytes();
          tempBuf.setBytes(0, varCharVal);
          listWriter.writeVarChar(0, varCharVal.length, tempBuf);
        }

      }

      listWriter.endList();

    }
  }

  private static List<List<Integer>> getIntList(final int size) {
    final List<List<Integer>> listOfLists = new JsonStringArrayList<>(size);
    final int listSize = 5;
    for (int i = 0; i < size; i++) {

      final List<Integer> list = new JsonStringArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(j);
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private static List<List<String>> getStringList(final int size) {
    final List<List<String>> listOfLists = new ArrayList<>(size);
    final int listSize = 5;

    for (int i = 0; i < size; i++) {
      final List<String> list = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(randomString(j));
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private static String randomString(final int num) {

    final StringBuilder builder = new StringBuilder();

    builder.append((char)('0' + num));

    return builder.toString();
  }

  @Override
  public VectorAccessible getOutput() {

    return result;
  }

  @Override
  public void close() throws Exception {
    result.close();
  }

  private static void insertIntoIntVector(final int index, final int value, final BaseFixedWidthVector vector) {
    final IntVector vec = (IntVector)vector;
    vec.setSafe(index, value);
  }
}
