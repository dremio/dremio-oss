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
package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

public class TestVectorHelper extends DremioTest {
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
  public void testListVectorElementLengthAndCount() {
    final Random rn = new Random();
    final int numOfRows = rn.nextInt(20) + 1;

    final List<List<String>> stringList = getRandomTextMatrix(numOfRows);

    final ListVector listVector = getFilledListVector(stringList);

    final int testIndex = rn.nextInt(numOfRows);

    ListVectorRecordInfo info = VectorHelper.getListVectorEntrySizeAndCount(listVector, testIndex);
    Assert.assertEquals(info.getSize(), getTotalStringArrayLength(stringList.get(testIndex)));
    Assert.assertEquals(info.getNumOfElements(), stringList.get(testIndex).size());

    //test null entry
    info = VectorHelper.getListVectorEntrySizeAndCount(listVector, stringList.size());
    Assert.assertEquals(info.getSize(), -1);
    Assert.assertEquals(info.getNumOfElements(), -1);

    //test empty list
    info = VectorHelper.getListVectorEntrySizeAndCount(listVector, stringList.size() + 1);
    Assert.assertEquals(info.getSize(), 0);
    Assert.assertEquals(info.getNumOfElements(), 1);

    listVector.close();
  }

  @Test
  public void testVarcharVectorElementLength() {
    final Random rn = new Random();
    final int count = rn.nextInt(20) + 1;

    final List<String> stringList = getRandomTextList(count);


    final VarCharVector vector = (VarCharVector) getFilledVarcharVector(stringList);
    final int testIndex = rn.nextInt(count);

    int valueLen = VectorHelper.getVariableWidthVectorValueLength(vector, testIndex);
    Assert.assertEquals(valueLen, stringList.get(testIndex).length());

    //test null value
    valueLen = VectorHelper.getVariableWidthVectorValueLength(vector, stringList.size());
    Assert.assertEquals(valueLen, -1);

    //test 0 length value
    valueLen = VectorHelper.getVariableWidthVectorValueLength(vector, stringList.size() + 1);
    Assert.assertEquals(valueLen, 0);

    vector.close();
  }

  int getTotalStringArrayLength(final List<String> words) {
    return words.stream()
      .mapToInt(String::length)
      .sum();
  }

  private List<List<String>> getRandomTextMatrix(final int numOfRows) {

    final Random rn = new Random();
    final List<List<String>> stringList = new ArrayList<>();

    for (int i = 0; i < numOfRows; i++) {

      final int numOfColumns = rn.nextInt(20) + 1;

      final List<String> entry = getRandomTextList(numOfColumns);

      stringList.add(entry);
    }

    return stringList;
  }

  private List<String> getRandomTextList(final int count) {
    final List<String> entry = new ArrayList<>();

    for (int j = 0; j < count; j++) {
      entry.add(generateNextWord());
    }

    return entry;
  }

  private String generateNextWord() {

    final Random rn = new Random();

    // words of length 5 through 20
    final char[] word = new char[rn.nextInt(15)+5];

    for (int k = 0; k < word.length; k++) {
      word[k] = (char)('a' + rn.nextInt(26));
    }

    return new String(word);

  }

  private BaseVariableWidthVector getFilledVarcharVector(final List<String> stringList) {
    final VarCharVector vector = new VarCharVector("colList", testAllocator);

    vector.allocateNew();

    for (int i = 0; i < stringList.size(); i++) {
      vector.set(i, stringList.get(i).getBytes());
    }

    //size + 1'th entry is empty, entry at size'th index will be null
    vector.set(stringList.size() + 1 , "".getBytes());

    return vector;

  }

  private ListVector getFilledListVector(final List<List<String>> stringList) {
    final ListVector listVector = new ListVector("colList", testAllocator, FieldType.nullable(ArrowType.List.INSTANCE), null);

    listVector.allocateNew();

    try (final ArrowBuf sampleDataBuf = testAllocator.buffer(512)) {
      final UnionListWriter listWriter = new UnionListWriter(listVector);

      int i = 0;
      for (final List<String> list : stringList) {

        listWriter.setPosition(i);
        listWriter.startList();

        for (final String str : list) {
          final byte[] varCharVal = str.getBytes();
          sampleDataBuf.setBytes(0, varCharVal);
          listWriter.writeVarChar(0, varCharVal.length, sampleDataBuf);
        }
        listWriter.endList();
        i++;
      }

      //set empty list at size + 1 index, value at size'th index will be null
      listWriter.setPosition(stringList.size() + 1);
      listWriter.startList();
      listWriter.endList();
    }

    return listVector;

  }
}
