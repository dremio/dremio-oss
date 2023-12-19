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
package com.dremio.exec.fn.impl;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestStringFunctions extends BaseTestQuery {

  @Test
  public void testLpadWithSingleChar() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 20, 'a') l1 ")
      .baselineColumns("l1")
      .baselineValues("aaaaaaaaaatestString")
      .go();
  }
  @Test
  public void testLpadWithMultiChars() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 20, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("charcharchtestString")
      .go();
  }

  @Test
  public void testLpadWithAddingPartOfFillValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 12, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("chtestString")
      .go();
  }

  @Test
  public void testLpadWithValueLengthMoreThenLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 2, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("te")
      .go();
  }

  @Test
  public void testLpadForEmptyString() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('', 12, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("charcharchar")
      .go();
  }

  //TODO maybe bug
  @Test
  public void testLpadForEmptyPadString() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 12, '') l1 ")
      .baselineColumns("l1")
      .baselineValues("testString")
      .go();
  }

  //TODO maybe bug
  @Test
  public void testLpadWithNegativeLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', -1, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("")
      .go();
  }

  @Test
  public void testLpadWithIntValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad(111, 20, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("charcharcharcharc111")
      .go();
  }

  @Test
  public void testLpadWithIntPadValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 20, 1) l1 ")
      .baselineColumns("l1")
      .baselineValues("1111111111testString")
      .go();
  }

  @Test
  public void testLpadWithoutPadValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "lpad('testString', 20) l1 ")
      .baselineColumns("l1")
      .baselineValues("          testString")
      .go();
  }

  @Test
  public void testRpadWithSingleChar() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 20, 'a') l1 ")
      .baselineColumns("l1")
      .baselineValues("testStringaaaaaaaaaa")
      .go();
  }
  @Test
  public void testRpadWithMultiChars() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 20, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("testStringcharcharch")
      .go();
  }

  @Test
  public void testRpadWithAddingPartOfFillValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 12, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("testStringch")
      .go();
  }

  @Test
  public void testRpadWithValueLengthMoreThenLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 2, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("te")
      .go();
  }

  @Test
  public void testRpadForEmptyString() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('', 12, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("charcharchar")
      .go();
  }

  //TODO maybe bug
  @Test
  public void testRpadForEmptyPadString() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 12, '') l1 ")
      .baselineColumns("l1")
      .baselineValues("testString")
      .go();
  }

  //TODO maybe bug
  @Test
  public void testRpadWithNegativeLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', -1, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("")
      .go();
  }

  @Test
  public void testRpadWithIntValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad(111, 20, 'char') l1 ")
      .baselineColumns("l1")
      .baselineValues("111charcharcharcharc")
      .go();
  }

  @Test
  public void testRpadWithIntPadValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "Rpad('testString', 20, 1) l1 ")
      .baselineColumns("l1")
      .baselineValues("testString1111111111")
      .go();
  }

  @Test
  public void testRpadWithoutPadValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "rpad('testString', 20) l1 ")
      .baselineColumns("l1")
      .baselineValues("testString          ")
      .go();
  }

  @Test
  public void testCharSubstringFromStart() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 1,2) l1 ")
      .baselineColumns("l1")
      .baselineValues("ch")
      .go();
  }

  @Test
  public void testCharSubstringFromMiddle() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 4,3) l1 ")
      .baselineColumns("l1")
      .baselineValues("ckC")
      .go();
  }

  @Test
  public void testCharSubstringInTheEnd() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 16,3) l1 ")
      .baselineColumns("l1")
      .baselineValues("ing")
      .go();
  }

  @Test
  public void testCharSubstringOffsetOutOfLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 20,3) l1 ")
      .baselineColumns("l1")
      .baselineValues("")
      .go();
  }

  @Test
  public void testCharSubstringLengthOutOfValueLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 4,20) l1 ")
      .baselineColumns("l1")
      .baselineValues("ckCharSubstring")
      .go();
  }

  @Test
  public void testCharSubstringWithOutLength() throws Exception {
    errorMsgTestHelper("SELECT charsubstring('checkCharSubstring', 4)",
      "Invalid number of arguments to function 'CHARSUBSTRING'. Was expecting 3 arguments");
  }

  @Test
  public void testCharSubstringWithOffsetLikeVarChar() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', '1', 2) l1 ")
      .baselineColumns("l1")
      .baselineValues("ch")
      .go();
  }

  @Test
  public void testCharSubstringWithLengthLikeVarChar() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 1, '2') l1 " )
      .baselineColumns("l1")
      .baselineValues("ch")
      .go();
  }

  @Test
  public void testCharSubstringForEmptyValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('', 2,2) l1 " )
      .baselineColumns("l1")
      .baselineValues("")
      .go();
  }

  @Test
  public void testCharSubstringWithNegativeOffset() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', -5,2) l1 ")
      .baselineColumns("l1")
      .baselineValues("tr")
      .go();
  }

  @Test
  public void testCharSubstringWithNegativeLength() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring('checkCharSubstring', 5,-2) l1 ")
      .baselineColumns("l1")
      .baselineValues("")
      .go();
  }

  @Test
  public void testCharSubstringWithIntegerValue() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "charsubstring(123456, 5,2) l1 ")
      .baselineColumns("l1")
      .baselineValues("56")
      .go();
  }

  @Test
  public void testCharSubstringIncompatibleTypeForOffset() throws Exception {
    errorMsgTestHelper("SELECT charsubstring('checkCharSubstring', 'bla', 2)",
      "Failure while attempting to cast value 'bla' to Bigint.");
  }

  @Test
  public void testCharSubstringIncompatibleTypeForLength() throws Exception {
    errorMsgTestHelper("SELECT charsubstring('checkCharSubstring', 2, 'bla')",
      "Failure while attempting to cast value 'bla' to Bigint.");
  }
}
