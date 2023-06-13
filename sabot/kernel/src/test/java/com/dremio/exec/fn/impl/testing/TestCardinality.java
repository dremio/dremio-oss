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

package com.dremio.exec.fn.impl.testing;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;


public class TestCardinality extends BaseTestQuery {
  @Test
  public void testNonNullArray() throws Exception {
    //[1,2,3,4,5]
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(int_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 1")
      .baselineColumns("c1")
      .baselineValues(5)
      .go();
    //[a,b,c,d,e]
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(str_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 1")
      .baselineColumns("c1")
      .baselineValues(5)
      .go();
  }

  @Test
  public void testMixedArray() throws Exception {
    // [1,2,null,3,null,5]
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(int_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 2")
      .baselineColumns("c1")
      .baselineValues(6)
      .go();
    //[a, null, c, null, e]
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(str_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 2")
      .baselineColumns("c1")
      .baselineValues(5)
      .go();
  }

  @Test
  public void testNullElementsArray() throws Exception {
    // [null,null,null,null,null,null] in LIST<INT> field
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(int_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 3")
      .baselineColumns("c1")
      .baselineValues(6)
      .go();
    // [null,null,null,null,null] in LIST<STRING> field
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(str_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 3")
      .baselineColumns("c1")
      .baselineValues(5)
      .go();
  }

  @Test
  public void testNullArray() throws Exception {
    // null value in LIST<INT>
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(int_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 4")
      .baselineColumns("c1")
      .baselineValues(null)
      .go();
    // null value in LIST<STRING>
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(str_array) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 4")
      .baselineColumns("c1")
      .baselineValues(null)
      .go();
  }

  @Test
  public void testMapCardinality() throws Exception {
    // {1:'a', 2:'b'}
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(mapdata) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 1")
      .baselineColumns("c1")
      .baselineValues(2)
      .go();
  }

  @Test
  public void testMapCardinalityWithNullValues() throws Exception {
    // {1:null, 2:'b'}
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(mapdata) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 2")
      .baselineColumns("c1")
      .baselineValues(2)
      .go();
    // {1:null, 2:null}
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(mapdata) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 3")
      .baselineColumns("c1")
      .baselineValues(2)
      .go();
  }

  @Test
  public void testNullMap() throws Exception {
    // null value in MAP<INT,STRING>
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(mapdata) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 4")
      .baselineColumns("c1")
      .baselineValues(null)
      .go();
  }

  @Test
  public void testCardinalityNestedArray() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(CONVERT_FROM('[[1,2], [3,4], {5:\"a\",6:\"b\"}]', 'json')) c1")
      .baselineColumns("c1")
      .baselineValues(3)
      .go();
  }

  @Test
  public void testCardinalityStringArray() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT CARDINALITY(CONVERT_FROM('[\"a\", \"b\", \"c\", \"d\"]', 'json')) c1")
      .baselineColumns("c1")
      .baselineValues(4)
      .go();
  }

  @Test
  public void testCardinalityWithNullElements() throws Exception {
    testBuilder()
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set \"store.json.all_text_mode\"= true")
      .sqlQuery("SELECT CARDINALITY(CONVERT_FROM('[\"a\", null, \"b\"]', 'json')) c1")
      .baselineColumns("c1")
      .baselineValues(3)
      .go();
  }

  @Test
  public void testIncompatibleTypes() {
    Exception ex = Assert.assertThrows(Exception.class,
      () -> testBuilder()
        .unOrdered()
        .sqlQuery("SELECT CARDINALITY('1') c1")
        .baselineColumns("c1")
        .baselineValues(1)
        .go()
    );
    Assert.assertTrue(ex.getMessage().contains(
      "VALIDATION ERROR: Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY(<VARCHAR(1)>)'"));
  }

  @Test
  public void testStructInput() {
    // STRUCT{1:null, 2:'b'}
    Exception ex = Assert.assertThrows(Exception.class,
      () -> testBuilder()
        .unOrdered()
        .sqlQuery("SELECT CARDINALITY(struct_data) c1 FROM cp.\"null_str_int_array_map.parquet\" WHERE case_id = 1")
        .baselineColumns("c1")
        .baselineValues(1)
        .go()
    );
    Assert.assertTrue(ex.getMessage().contains(
      "VALIDATION ERROR: Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY(<RECORDTYPE(INTEGER A, INTEGER B)>)'"));
  }

  @Test
  public void testRuntimeTypeChecking() {
    Exception ex = Assert.assertThrows(Exception.class,
      () -> testBuilder()
        .unOrdered()
        .sqlQuery("SELECT CARDINALITY(CONVERT_FROM('{\"name\":\"Gnarly\", \"age\":7}', 'json'))")
        .baselineColumns("c1")
        .baselineValues(1)
        .go()
    );
    Assert.assertTrue(ex.getMessage().contains("Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY(<STRUCT>)'"));
  }
}
