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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

/**
 * Test {@link Merger} implementations of different vector types.
 */
public class TestMerger extends DremioTest {

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

  private List<List<Integer>> getRandomIntMatrix(final int numOfRows, final int numOfColumns) {

    final List<List<Integer>> numList = new ArrayList<>();

    for (int i = 0; i < numOfRows; i++) {

      final List<Integer> entry = getRandomIntList(numOfColumns);

      numList.add(entry);
    }

    return numList;
  }

  private List<Integer> getRandomIntList(final int count) {
    final List<Integer> entry = new ArrayList<>();
    final Random rn = new Random();

    for (int j = 0; j < count; j++) {
      //random int between 5 and 20
      entry.add(rn.nextInt(15)+5);
    }

    return entry;
  }

  /**
   * Returns an instance of FixedSizeListVector with values filled from given lists of integers
   * @param numList
   * @return
   */
  private ListVector getFilledListVector(final List<List<Integer>> numList) {

    final ListVector vector = new ListVector("fixed_list", testAllocator, FieldType.nullable(new ArrowType.FixedSizeList(numList.get(0).size())), null);

    final UnionListWriter listWriter = new UnionListWriter(vector);

    int i = 0;
    for (final List<Integer> list : numList) {

      listWriter.setPosition(i);
      listWriter.startList();

      for (final Integer num : list) {
        listWriter.writeInt(num);
      }
      listWriter.endList();
      i++;
    }

    vector.setValueCount(numList.size());

    return vector;
  }

  /**
   * Returns an instance of FixedSizeListVector with values filled from given lists of integers
   * @param numList
   * @return
   */
  private FixedSizeListVector getFilledFixedListVector(final List<List<Integer>> numList) {

    final FixedSizeListVector vector = new FixedSizeListVector("fixed_list", testAllocator, FieldType.nullable(new ArrowType.FixedSizeList(numList.get(0).size())), null);

    final UnionFixedSizeListWriter listWriter = new UnionFixedSizeListWriter(vector);

    int i = 0;
    for (final List<Integer> list : numList) {

      listWriter.setPosition(i);
      listWriter.startList();

      for (final Integer num : list) {
        listWriter.writeInt(num);
      }
      listWriter.endList();
      i++;
    }

    vector.setValueCount(numList.size());

    return vector;
  }


  /**
   * Given list vector is merged from vectors which used lists as inputs.
   *  This function verifies whether all input values are successfully merged into the vector.
   * @param listVector
   * @param inputList
   */
  private void verifyListContentsWithInputMatrix(final FixedSizeListVector listVector, final List<List<List<Integer>>> inputList) {

    int listPosition = 0;
    for (final List<List<Integer>> input : inputList) {
      for (int i = 0; i < input.size(); listPosition++) {
        if (!listVector.isNull(listPosition)) {
          final List<Integer> vectorList = (ArrayList<Integer>) listVector.getObject(listPosition);
          final List<Integer> innerList = input.get(i);

          Assert.assertEquals(vectorList.size(), innerList.size());

          for (int j = 0; j < vectorList.size(); j++) {
            Assert.assertEquals(innerList.get(j), vectorList.get(j));
          }
          i++;
        }
      }

    }

  }

  private void verifyListContentsWithInputMatrix(final ListVector listVector, final List<List<List<Integer>>> inputList) {

    int listPosition = 0;
    for (final List<List<Integer>> input : inputList) {
      for (int i = 0; i < input.size(); listPosition++) {
        if (!listVector.isNull(listPosition)) {
          final List<Integer> vectorList = (ArrayList<Integer>) listVector.getObject(listPosition);
          final List<Integer> innerList = input.get(i);

          Assert.assertEquals(vectorList.size(), innerList.size());

          for (int j = 0; j < vectorList.size(); j++) {
            Assert.assertEquals(innerList.get(j), vectorList.get(j));
          }
          i++;
        }
      }

    }

  }

  /**
   * Test by merging two ListVector's into a single one
   * @throws Exception
   */
  @Test
  public void testListMerger() throws Exception {

    try (final PagePool pages = new PagePool(testAllocator, 64_000, 0);
         final Page page = pages.newPage()) {

      final List<List<List<Integer>>> inputLists = new ArrayList<>();

      final Random rn = new Random();
      int rowCount = rn.nextInt(15)+5;
      int colCount = rn.nextInt(15)+5;

      //fill vector1
      final List<List<Integer>> inputMatrix = getRandomIntMatrix(rowCount, colCount);
      final ListVector vector = getFilledListVector(inputMatrix);

      inputLists.add(inputMatrix);

      //fill vector2
      rowCount = rn.nextInt(15)+5;
      colCount = rn.nextInt(15)+5;
      final List<List<Integer>> inputMatrix2 = getRandomIntMatrix(rowCount, colCount);
      final ListVector vector2 = getFilledListVector(inputMatrix2);

      inputLists.add(inputMatrix2);

      final Merger merger = Merger.get(vector, 0, testAllocator);
      final List<FieldVector> output = new ArrayList<>(); //output vector will be contained in this

      final List<ListVector> list = new ArrayList<>();
      list.add(vector);
      list.add(vector2);

      final VectorContainerList vectorContainerList = new VectorContainerList(list, 0);

      //merge two vectors
      merger.merge(vectorContainerList, page, output);

      final ListVector outputVector = (ListVector) output.get(0);

      //test if the vector is successfully merged
      verifyListContentsWithInputMatrix(outputVector, inputLists);

      outputVector.close();
      vector.close();
      vector2.close();

    }

  }

  /**
   * Test by merging two FixedSizeListVector's into a single one
   * @throws Exception
   */
  @Test
  public void testFixedListMerger() throws Exception {

    try (final PagePool pages = new PagePool(testAllocator, 64_000, 0);
         final Page page = pages.newPage()) {

      final List<List<List<Integer>>> inputLists = new ArrayList<>();

      final Random rn = new Random();
      int rowCount = rn.nextInt(15)+5;

      //column count stays the same for both vectors as this is fixed size list vector
      final int colCount = rn.nextInt(15)+5;

      //fill vector1
      final List<List<Integer>> inputMatrix = getRandomIntMatrix(rowCount, colCount);
      final FixedSizeListVector vector = getFilledFixedListVector(inputMatrix);

      inputLists.add(inputMatrix);

      //fill vector2
      rowCount = rn.nextInt(15)+5;
      final List<List<Integer>> inputMatrix2 = getRandomIntMatrix(rowCount, colCount);
      final FixedSizeListVector vector2 = getFilledFixedListVector(inputMatrix2);

      inputLists.add(inputMatrix2);

      final Merger merger = Merger.get(vector, 0, testAllocator);
      final List<FieldVector> output = new ArrayList<>(); //output vector will be contained in this

      final List<FixedSizeListVector> list = new ArrayList<>();
      list.add(vector);
      list.add(vector2);

      final VectorContainerList vectorContainerList = new VectorContainerList(list, 0);

      //merge two vectors
      merger.merge(vectorContainerList, page, output);

      final FixedSizeListVector outputVector = (FixedSizeListVector) output.get(0);

      //test if the vector is successfully merged
      verifyListContentsWithInputMatrix(outputVector, inputLists);

      outputVector.close();
      vector.close();
      vector2.close();

    }

  }
}
