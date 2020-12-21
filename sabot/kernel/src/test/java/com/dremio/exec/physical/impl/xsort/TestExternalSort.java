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
package com.dremio.exec.physical.impl.xsort;

import java.math.BigDecimal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestExternalSort extends BaseTestQuery {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testDecimalSortNullsFirst() throws Exception {
    try(AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);
        AutoCloseable decimalNew = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2,
          true);){
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a asc nulls first";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(null, "null")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .go();
    }

    // Test that without the v2 option also the new functions are fixed and sort is working as
    // expected.
    try(AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);){
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a asc nulls first";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(null, "null")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .go();
    }
  }

  @Test
  public void testDecimalSortNullsLast() throws Exception {
    try(AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);
        AutoCloseable decimalNew = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2,
          true);){
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a asc nulls last";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(null, "null")
        .go();
    }

    // Test that without the v2 option also the new functions are fixed and sort is working as
    // expected.
    try(AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);){
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a asc nulls last";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(null, "null")
        .go();
    }
  }

  @Test
  public void testDecimalSortDesc() throws Exception {
    try (AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);
         AutoCloseable decimalNew = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2,
           true);) {
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a desc nulls first";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(null, "null")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .go();
    }

    try (AutoCloseable decimal = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true);
         AutoCloseable decimalNew = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2,
           true);) {
      String query = "select * from cp.\"sort/sort-decimals.parquet\" " +
        " order by a desc nulls last";
      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(null, "null")
        .go();
    }
  }

}
