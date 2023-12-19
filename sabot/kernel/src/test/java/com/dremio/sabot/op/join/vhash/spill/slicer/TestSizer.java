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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.AllocatorRule;

public class TestSizer {
  private BufferAllocator testAllocator;
  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-fixedlist-varchar-vector", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  @Test
  public void testListSizer(){
    final List<Pair<Integer, Integer>> listOfStruct = Arrays.asList(Pair.of(1, 2), Pair.of(3, 4),
      Pair.of(5, 6));
    try (ListVector vector = getFilledListOfStructVector(listOfStruct)) {
      final Sizer sizer = new ListSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testListSizerForEmptyListOfStruct(){
    try( ListVector vector = getFilledListOfStructVector(null)) {
      final Sizer sizer = new ListSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertEquals(size, 128);
    }
  }

  @Test
  public void testStructSizer(){
    final List<Integer> list = Arrays.asList(1, 2, 3, 4);
    try (StructVector vector = getFilledStructOfVector(list)){
      final Sizer sizer = new StructSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testUnionSizer(){
    final List<Pair<Integer, Integer>> listOfStruct = Arrays.asList(Pair.of(1, 2), Pair.of(3, 4),
      Pair.of(5, 6));
    try (UnionVector vector = getFilledUnionOfStructVector(listOfStruct)) {
      final Sizer sizer = new SparseUnionSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testUnionSizerForEmptyListOfStruct(){
    try( UnionVector vector = getFilledUnionOfStructVector(null)) {
      final Sizer sizer = new SparseUnionSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertEquals(size, 192);
    }
  }

  private ListVector getFilledListOfStructVector(final List<Pair<Integer, Integer>> listOfStruct) {
    final ListVector vector = new ListVector("list", testAllocator, FieldType.nullable(new ArrowType.List()), null);
    final UnionListWriter listWriter = new UnionListWriter(vector);
    int i = 0;
    listWriter.startList();
    if(listOfStruct == null){
      BaseWriter.StructWriter structWriter = listWriter.struct();
      structWriter.start();
      structWriter.integer("first").writeNull();
      structWriter.integer("second").writeNull();
      structWriter.end();
      vector.setValueCount(1);
      return vector;
    }
    for (final Pair<Integer, Integer> struct : listOfStruct) {
      listWriter.setPosition(i);

      BaseWriter.StructWriter structWriter = listWriter.struct();
      structWriter.start();
      structWriter.integer("first").writeInt(struct.getLeft());
      structWriter.integer("second").writeInt(struct.getRight());
      structWriter.end();
      i++;
    }
    listWriter.endList();
    vector.setValueCount(1);
    vector.getDataVector().setValueCount(listOfStruct.size());

    return vector;
  }

  private UnionVector getFilledUnionOfStructVector(final List<Pair<Integer, Integer>> listOfStruct) {
    final UnionVector vector = new UnionVector("union", testAllocator, FieldType.nullable(new ArrowType.List()), null);
    final UnionWriter unionWriter = new UnionWriter(vector);
    int i = 0;
    unionWriter.startList();
    if(listOfStruct == null){
      BaseWriter.StructWriter structWriter = unionWriter.struct();
      structWriter.start();
      structWriter.integer("first").writeNull();
      structWriter.integer("second").writeNull();
      structWriter.end();
      vector.setValueCount(1);
      return vector;
    }
    for (final Pair<Integer, Integer> struct : listOfStruct) {
      unionWriter.setPosition(i);

      BaseWriter.StructWriter structWriter = unionWriter.struct();
      structWriter.start();
      structWriter.integer("first").writeInt(struct.getLeft());
      structWriter.integer("second").writeInt(struct.getRight());
      structWriter.end();
      i++;
    }
    unionWriter.endList();
    vector.setValueCount(1);
    vector.getVector(0).setValueCount(listOfStruct.size());

    return vector;
  }

  private StructVector getFilledStructOfVector(final List<Integer> list) {
    final StructVector vector = new StructVector("struct", testAllocator, FieldType.nullable(new ArrowType.Struct()), null);
    final NullableStructWriter structWriter = new NullableStructWriter(vector);
    structWriter.start();
    BaseWriter.ListWriter listWriter = structWriter.list("list");
    listWriter.startList();
    for (final Integer value : list) {
      listWriter.integer().writeInt(value);
    }
    listWriter.endList();
    structWriter.end();
    vector.setValueCount(1);
    return vector;
  }

}
