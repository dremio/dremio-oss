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

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.test.AllocatorRule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestSizer {
  private static ArrowBuf tempBuf;
  private BufferAllocator testAllocator;
  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-fixedlist-varchar-vector", 0, Long.MAX_VALUE);
    tempBuf = testAllocator.buffer(100);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    tempBuf.close();
    testAllocator.close();
  }

  @Test
  public void testListSizer() {
    final List<Pair<Integer, Integer>> listOfStruct =
        Arrays.asList(Pair.of(1, 2), Pair.of(3, 4), Pair.of(5, 6));
    try (ListVector vector = getFilledListOfStructVector(listOfStruct)) {
      final Sizer sizer = new ListSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testListSizerForEmptyListOfStruct() {
    try (ListVector vector = getFilledListOfStructVector(null)) {
      final Sizer sizer = new ListSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertEquals(size, 128);
    }
  }

  @Test
  public void testStructSizer() {
    final List<Integer> list = Arrays.asList(1, 2, 3, 4);
    try (StructVector vector = getFilledStructOfVector(list)) {
      final Sizer sizer = new StructSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testVariableSizer() {
    try (VarCharVector vector = getFilledVarCharVector()) {
      final Sizer sizer = new VariableSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testEmptyVariableSizer() {
    try (VarCharVector vector = new VarCharVector("varchar", testAllocator); ) {
      final Sizer sizer = new VariableSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size == 0);
    }
  }

  @Test
  public void testUnionSizer() {
    final List<Pair<Integer, Integer>> listOfStruct =
        Arrays.asList(Pair.of(1, 2), Pair.of(3, 4), Pair.of(5, 6));
    try (UnionVector vector = getFilledUnionOfStructVector(listOfStruct)) {
      final Sizer sizer = new SparseUnionSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertTrue(size > 0);
    }
  }

  @Test
  public void testUnionSizerForEmptyListOfStruct() {
    try (UnionVector vector = getFilledUnionOfStructVector(null)) {
      final Sizer sizer = new SparseUnionSizer(vector);
      final long size = sizer.getSizeInBitsStartingFromOrdinal(0, vector.getValueCount());
      Assert.assertEquals(size, 192);
    }
  }

  @Test
  public void testMaxBatchSizeFixedLengthVectors() {
    /*
    input data -
    null 1 null
    null 1 null
    null 1 null
    null 1 null
    null 1 null
    null 1 2
    null 1 2
    null 1 2
    null 1 2
    null 1 2
     */
    Integer[][] inputData = new Integer[3][10];
    for (int index = 0; index < 10; index++) {
      inputData[0][index] = null;
    }
    for (int index = 0; index < 10; index++) {
      inputData[1][index] = 1;
    }
    for (int index = 0; index < 5; index++) {
      inputData[2][index] = null;
    }
    for (int index = 4; index < 10; index++) {
      inputData[2][index] = 2;
    }
    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addIntVectorsToContainer(inputData, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 8);
    }
  }

  @Test
  public void testMaxBatchSizeVariableLengthVectors() {
    /*
    input data -
     1 "abc" null
     1 "abc" "a"
     1 "abc" "ab"
     1 "abc" "abc"
     1 "abc" "abcd"
     1 "abc" "abcde"
     1 "abc" "abcdef"
     1 "abc" "abcdefg"
     1 "abc" "abcdefgh"
     1 "abc" "abcdefghi"
     */
    Integer[][] integerInput = new Integer[1][10];
    for (int index = 0; index < 10; index++) {
      integerInput[0][index] = 1;
    }
    String[][] stringInput = new String[2][10];
    for (int index = 0; index < 10; index++) {
      stringInput[0][index] = "abc";
    }
    for (int index = 0; index < 10; index++) {
      stringInput[1][index] = RandomStringUtils.randomAlphabetic(index);
    }

    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addIntVectorsToContainer(integerInput, container);
      addStringVectorToContainer(stringInput, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 16);
    }
  }

  @Test
  public void testMaxBatchSizeListVectors() {
    /*
    input data -
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3] "abc"
     1 [1,2,3,4] "abc"
     */
    Integer[][] integerInput = new Integer[10][3];
    for (int index = 0; index < 9; index++) {
      integerInput[index] = new Integer[] {1, 2, 3};
    }
    integerInput[9] = new Integer[] {1, 2, 3, 4};
    String[][] stringInput = new String[1][10];
    for (int index = 0; index < 10; index++) {
      stringInput[0][index] = "abc";
    }

    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addSingleIntListVectorToContainer(integerInput, container);
      addStringVectorToContainer(stringInput, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 19);
    }
  }

  @Test
  public void testMaxBatchSizeVariableVectors() {
    /*
    input data -

     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abc"] "abc"
     1 ["abc","abc","abcd"] "abc"
     1 ["abc","abc","abc"] null
     1 ["abc","abc", null] "abc"

     */
    String[][] stringListInput = new String[10][3];
    for (int index = 0; index < 7; index++) {
      stringListInput[index] = new String[] {"abc", "abc", "abc"};
    }

    stringListInput[7] = new String[] {"abc", "abc", "abcd"};
    stringListInput[8] = new String[] {"abc", "abc", "abc"};
    stringListInput[9] = new String[] {"abc", "abc", null};

    String[][] stringInput = new String[1][10];
    for (int index = 0; index < 8; index++) {
      stringInput[0][index] = "abc";
    }
    stringInput[0][8] = null;
    stringInput[0][9] = "abc";

    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addSingleVarcharListVectorToContainer(stringListInput, container);
      addStringVectorToContainer(stringInput, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 13);
    }
  }

  @Test
  public void testMaxBatchSizeStructVectors() {
    /*
    input data -

     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {null "abc"}
     {1 "abc"}

     */

    Integer[] integerInput = new Integer[10];
    for (int index = 0; index < 8; index++) {
      integerInput[index] = null;
    }
    integerInput[9] = 1;

    String[] stringListInput = new String[10];
    for (int index = 0; index < 10; index++) {
      stringListInput[index] = "abc";
    }
    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addSingleStructVectorToContainer("struct1", stringListInput, integerInput, container);
      addSingleStructVectorToContainer("struct2", stringListInput, integerInput, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 14);
    }
  }

  @Test
  public void testMaxBatchSizeUnionVectors() {
    /*
    input data -

     1.5 "abc"
     4 "abc"
     1.5  "abc"
     4 "abc"
     1.5  "abc"
     4 "abc"
     1.5  "abc"
     4 "abc"
     null "abc"
     4 null
     */

    Integer[] intInput = new Integer[5];
    for (int index = 0; index < 5; index++) {
      intInput[index] = 4;
    }

    Float[] doubleInput = new Float[5];
    for (int index = 0; index < 4; index++) {
      doubleInput[index] = 1.5F;
    }
    doubleInput[4] = null;

    String[][] stringListInput = new String[1][10];
    for (int index = 0; index < 9; index++) {
      stringListInput[0][index] = "abc";
    }
    stringListInput[0][9] = null;

    try (VectorContainer container = new VectorContainer(testAllocator)) {
      addSingleUnionVectorToContainer(intInput, doubleInput, container);
      addStringVectorToContainer(stringListInput, container);
      container.setAllCount(10);
      CombinedSizer sizer = getSizer(container);
      Assert.assertEquals(sizer.getMaxRowLengthInBatch(10), 7);
    }
  }

  CombinedSizer getSizer(VectorContainer container) {
    List<Sizer> sizerList = new ArrayList<>();
    for (VectorWrapper<?> vectorWrapper : container) {
      sizerList.add(Sizer.get(vectorWrapper.getValueVector()));
    }
    return new CombinedSizer(sizerList);
  }

  private ListVector getFilledListOfStructVector(final List<Pair<Integer, Integer>> listOfStruct) {
    final ListVector vector =
        new ListVector("list", testAllocator, FieldType.nullable(new ArrowType.List()), null);
    final UnionListWriter listWriter = new UnionListWriter(vector);
    int i = 0;
    listWriter.startList();
    if (listOfStruct == null) {
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

  private UnionVector getFilledUnionOfStructVector(
      final List<Pair<Integer, Integer>> listOfStruct) {
    final UnionVector vector =
        new UnionVector("union", testAllocator, FieldType.nullable(new ArrowType.List()), null);
    final UnionWriter unionWriter = new UnionWriter(vector);
    int i = 0;
    unionWriter.startList();
    if (listOfStruct == null) {
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
    final StructVector vector =
        new StructVector("struct", testAllocator, FieldType.nullable(new ArrowType.Struct()), null);
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

  private VarCharVector getFilledVarCharVector() {
    final VarCharVector vector = new VarCharVector("varchar", testAllocator);
    final VarCharWriterImpl varCharWriter = new VarCharWriterImpl(vector);
    varCharWriter.writeVarChar(0, 0, tempBuf);
    vector.setValueCount(1);
    return vector;
  }

  private void addIntVectorsToContainer(Integer[][] dataMatrix, VectorContainer container) {
    for (int row = 0; row < dataMatrix.length; row++) {
      IntVector intVector = new IntVector("int_" + row, testAllocator);
      for (int column = 0; column < dataMatrix[0].length; column++) {
        if (dataMatrix[row][column] == null) {
          intVector.setNull(column);
        } else {
          intVector.setSafe(column, dataMatrix[row][column]);
        }
      }
      container.add(intVector);
    }
  }

  private void addStringVectorToContainer(String[][] dataMatrix, VectorContainer container) {
    for (int row = 0; row < dataMatrix.length; row++) {
      VarCharVector varCharVector = new VarCharVector("str" + row, testAllocator);
      for (int column = 0; column < dataMatrix[0].length; column++) {
        if (dataMatrix[row][column] == null) {
          varCharVector.setNull(column);
        } else {
          varCharVector.setSafe(column, dataMatrix[row][column].getBytes());
        }
      }
      container.add(varCharVector);
    }
  }

  private void addSingleIntListVectorToContainer(
      Integer[][] dataMatrix, VectorContainer container) {
    ListVector listVector =
        new ListVector(
            new Field("intChild", new FieldType(true, new ArrowType.Int(32, true), null), null),
            testAllocator,
            null);

    final UnionListWriter listWriter = new UnionListWriter(listVector);
    listWriter.setPosition(0);
    for (int row = 0; row < dataMatrix.length; row++) {
      listWriter.startList();
      for (int column = 0; column < dataMatrix[row].length; column++) {
        if (dataMatrix[row][column] == null) {
          listWriter.writeNull();
        } else {
          listWriter.writeInt(dataMatrix[row][column]);
        }
      }
      listWriter.endList();
    }
    container.add(listVector);
  }

  private void addSingleVarcharListVectorToContainer(
      String[][] dataMatrix, VectorContainer container) {
    ListVector listVector =
        new ListVector(
            new Field("stringChild", new FieldType(true, new ArrowType.Utf8(), null), null),
            testAllocator,
            null);

    final UnionListWriter listWriter = new UnionListWriter(listVector);
    listWriter.setPosition(0);
    for (int row = 0; row < dataMatrix.length; row++) {
      listWriter.startList();
      for (int column = 0; column < dataMatrix[row].length; column++) {
        if (dataMatrix[row][column] == null) {
          listWriter.writeNull();
        } else {
          listWriter.writeVarChar(dataMatrix[row][column]);
        }
      }
      listWriter.endList();
    }
    container.add(listVector);
  }

  private void addSingleStructVectorToContainer(
      String name, String[] stringData, Integer[] integers, VectorContainer container) {
    final Field fieldStruct =
        CompleteType.struct(
                CompleteType.VARCHAR.toField("child_string"), CompleteType.INT.toField("child_int"))
            .toField(name);

    StructVector structVector = container.addOrGet(fieldStruct);
    final NullableStructWriter structWriter = structVector.getWriter();
    int index = 0;
    for (index = 0; index < stringData.length; index++) {
      structWriter.setPosition(index);
      structWriter.start();
      if (integers[index] == null) {
        structWriter.integer("child_int").writeNull();
      } else {
        structWriter.integer("child_int").writeInt(integers[index]);
      }
      if (stringData[index] == null) {
        structWriter.varChar("child_string").writeNull();
      } else {
        structWriter.varChar("child_string").writeVarChar(stringData[index]);
      }
      structWriter.end();
    }
    structWriter.setPosition(index);
    structWriter.start();
    structWriter.writeNull();
    structWriter.end();
  }

  private void addSingleUnionVectorToContainer(
      Integer[] integers, Float[] floats, VectorContainer container) {
    final Field unionField =
        CompleteType.union(CompleteType.INT.toField("int"), CompleteType.FLOAT.toField("float"))
            .toField("union");

    final UnionVector unionVector = container.addOrGet(unionField);
    final UnionWriter unionWriter = new UnionWriter(unionVector);
    int indexInInt = 0;
    int indexInFloat = 0;

    for (int index = 0; index < 10; index++) {
      unionWriter.setPosition(index);
      if (index % 2 != 0) {
        if (integers[indexInInt] == null) {
          unionWriter.writeNull();
        } else {
          unionWriter.writeInt(integers[indexInInt]);
        }
        indexInInt++;
      } else {
        if (floats[indexInFloat] == null) {
          unionWriter.writeNull();
        } else {
          unionWriter.writeFloat4(floats[indexInFloat]);
        }
        indexInFloat++;
      }
    }
  }
}
