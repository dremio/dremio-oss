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
package com.dremio.exec.store.parquet2;

import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import java.math.BigDecimal;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.junit.Test;

/** Class for testing mixed decimals for Parquet complex types */
public class TestComplexMixedDecimals extends BaseTestQuery {

  private final String workingPath = TestTools.getWorkingPath();
  private final String fullTestPath =
      workingPath + "/src/test/resources/parquet/decimal_nested_complex";

  @Test
  public void testCompatibleStructs() throws Exception {
    final String path = fullTestPath + "/compatible_struct";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);

    JsonStringHashMap<String, Number> struct1 = new JsonStringHashMap<>();
    JsonStringHashMap<String, Number> struct2 = new JsonStringHashMap<>();
    JsonStringHashMap<String, Number> struct3 = new JsonStringHashMap<>();

    struct1.put("int_col", 9786);
    struct1.put("dec_col", new BigDecimal("1322.2"));
    struct2.put("int_col", 2342);
    struct2.put("dec_col", new BigDecimal("12.2"));
    struct3.put("int_col", 350);
    struct3.put("dec_col", new BigDecimal("1234.5"));

    testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("string_col", "struct_col")
        .baselineValues("rio", struct1)
        .baselineValues("terry", struct2)
        .baselineValues("john", struct3)
        .go();
  }

  @Test
  public void testCompatibleLists() throws Exception {
    final String path = fullTestPath + "/compatible_list";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);

    JsonStringArrayList<BigDecimal> list1 = new JsonStringArrayList<>();
    JsonStringArrayList<BigDecimal> list2 = new JsonStringArrayList<>();
    JsonStringArrayList<BigDecimal> list3 = new JsonStringArrayList<>();
    JsonStringArrayList<BigDecimal> list4 = new JsonStringArrayList<>();

    list1.add(new BigDecimal("13.4567"));
    list1.add(new BigDecimal("14.9012"));
    list2.add(new BigDecimal("13.4700"));
    list2.add(new BigDecimal("-14.1256"));
    list3.add(new BigDecimal("13.1247"));
    list3.add(new BigDecimal("-25.8906"));
    list4.add(new BigDecimal("13.6947"));
    list4.add(new BigDecimal("-25.8906"));
    list4.add(new BigDecimal("12.4002"));

    testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("int_col", "string_col", "dec_col")
        .baselineValues(30, "John", list1)
        .baselineValues(-600, "Matt", list2)
        .baselineValues(4567, "Harry", list3)
        .baselineValues(41267, "Rose", list4)
        .go();
  }

  @Test
  public void testCompatibleNestedTypes() throws Exception {
    final String path = fullTestPath + "/compatible_nested";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);

    JsonStringArrayList<BigDecimal> innerList1 = new JsonStringArrayList<>();
    JsonStringArrayList<BigDecimal> innerList2 = new JsonStringArrayList<>();
    JsonStringHashMap<String, Object> struct1 = new JsonStringHashMap<>();
    JsonStringHashMap<String, Object> struct2 = new JsonStringHashMap<>();

    innerList1.add(new BigDecimal("100.12"));
    innerList1.add(new BigDecimal("123.45"));
    innerList2.add(new BigDecimal("-10.31"));
    innerList2.add(new BigDecimal("163.45"));
    innerList2.add(new BigDecimal("11.23"));

    struct1.put("inner_int", 20);
    struct1.put("list_col", innerList1);
    struct1.put("string_col", new Text("dremio"));
    struct2.put("inner_int", -650);
    struct2.put("list_col", innerList2);
    struct2.put("string_col", new Text("dremio"));

    testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("int_col", "struct_col")
        .baselineValues(500, struct1)
        .baselineValues(4530, struct2)
        .go();
  }

  @Test
  public void testMissingColumns() throws Exception {
    final String path = fullTestPath + "/missing_column";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);
    try {
      test(sqlQuery);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("schema"));
    }

    testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("dec_col", "int_col")
        .baselineValues(new BigDecimal("7654.132456"), null)
        .baselineValues(new BigDecimal("1234.23456"), 50)
        .go();
  }

  @Test
  public void testSwappedColumns() throws Exception {
    final String path = fullTestPath + "/swapped_column";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);

    testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("colb", "colc", "cola")
        .baselineValues(new BigDecimal("12345.56789"), "dremio", 100)
        .baselineValues(new BigDecimal("54321.56321"), "hyderabad", 200)
        .go();
  }

  @Test
  public void testColumnNameConflict() throws Exception {
    // DX-23685 - test case
    // 2 parquet files, which contain a struct column with following fields:
    // 1. StringCol - string, 2. INT_COL - int, 3. int_col - int
    // without the fix, query fails with 'IllegalStateException: Duplicate key'
    final String path = fullTestPath + "/case_sensitive";
    final String sqlQuery = String.format("select * from dfs.\"%s\"", path);
    test(sqlQuery);
  }
}
