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

public class TestArrayContains extends BaseTestQuery {

  @Test
  public void arrayContainswithFloat() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(floatlist, castFloat8(2.1)) l1 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainswithBigInt() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(intList, castBigInt(2)) l1 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainswithVarchar() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(varcharlist, 'a') l1 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainswithMultibyteChars() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(multibyteList, '¥abdc') l1 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1")
      .baselineValues(false)
      .go();
  }

  //null is in the list and value searched is not in the list
  @Test
  public void arrayContainswithNullInList() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(col1, 8) l1 " +
        "FROM cp.\"nulllist.parquet\"")
      .baselineColumns("l1")
      .baselineValues(null)
      .go();
  }

  @Test
  public void arrayContainswithDateInList() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(col1, cast('2010-2-23' as date)) l1 " +
        "FROM cp.\"dates_list.parquet\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainswithTimestampInList() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(col1, cast('2010-01-01 04:40:10.000' as timestamp)) l1 " +
        "FROM cp.\"array_contains_timestamp.parquet\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainsIncompatibleTypes() throws Exception {
    errorMsgTestHelper("SELECT array_contains(intList, 2) FROM cp.\"array_contains.json\"",
      "List of BIGINT is not comparable with INT");
  }

  @Test
  public void arrayContainsInvalidInput() throws Exception {
    errorMsgTestHelper("SELECT array_contains(intList[0], 7) FROM cp.\"array_contains.json\"",
      "First parameter to array_contains must be a LIST. Was given: BIGINT");
  }

  @Test
  public void arrayContainswithBigIntLargeList() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(int_1000, -17813863988) l1 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1")
      .baselineValues(true)
      .go();
  }

  @Test
  public void arrayContainswithBigIntList() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT " +
        "array_contains(int_1000, -17813863988) l1 " + "," + "array_contains(int_1000, -17813863988) l2 " +
        "FROM cp.\"array_contains.json\"")
      .baselineColumns("l1", "l2")
      .baselineValues(true, true)
      .go();
  }
}
