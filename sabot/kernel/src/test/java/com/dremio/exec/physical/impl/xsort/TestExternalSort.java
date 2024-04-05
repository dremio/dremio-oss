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

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.PlannerSettings;
import java.math.BigDecimal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestExternalSort extends BaseTestQuery {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testDecimalSortNullsFirst() throws Exception {
    String query =
        "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a asc nulls first";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(null, "null")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .go();

    // Test that without the v2 option also the new functions are fixed and sort is working as
    // expected.
    try (AutoCloseable ignored = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2, false)) {
      query = "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a asc nulls first";
      testBuilder()
          .sqlQuery(query)
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
    String query =
        "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a asc nulls last";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(null, "null")
        .go();

    // Test that without the v2 option also the new functions are fixed and sort is working as
    // expected.
    try (AutoCloseable ignored = withSystemOption(PlannerSettings.ENABLE_DECIMAL_V2, false)) {
      query = "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a asc nulls last";
      testBuilder()
          .sqlQuery(query)
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
    String query =
        "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a desc nulls first";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(null, "null")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .go();

    query = "select * from cp.\"sort/sort-decimals.parquet\" " + " order by a desc nulls last";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(new BigDecimal("2000000000000000000001.000000"), "third")
        .baselineValues(new BigDecimal("1000000000000000000002.000000"), "second")
        .baselineValues(new BigDecimal("1000000000000000000001.000000"), "first")
        .baselineValues(new BigDecimal("900000000000000000001.000000"), "zero")
        .baselineValues(null, "null")
        .go();
  }

  @Test
  public void testSortWithNaNValues() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"sort/sort_with_nan_values.parquet\" order by y2 asc")
        .ordered()
        .baselineColumns("y2")
        .baselineValues(Double.NEGATIVE_INFINITY)
        .baselineValues(Double.NEGATIVE_INFINITY)
        .baselineValues(0.0)
        .baselineValues(0.0)
        .baselineValues(3.141592653589793)
        .baselineValues(3.141592653589793)
        .baselineValues(Double.POSITIVE_INFINITY)
        .baselineValues(Double.POSITIVE_INFINITY)
        .baselineValues(Double.NaN)
        .baselineValues(Double.NaN)
        .baselineValues(null)
        .baselineValues(null)
        .go();

    testBuilder()
        .sqlQuery("select * from cp.\"sort/sort_with_nan_values.parquet\" order by y2 desc")
        .ordered()
        .baselineColumns("y2")
        .baselineValues(null)
        .baselineValues(null)
        .baselineValues(Double.NaN)
        .baselineValues(Double.NaN)
        .baselineValues(Double.POSITIVE_INFINITY)
        .baselineValues(Double.POSITIVE_INFINITY)
        .baselineValues(3.141592653589793)
        .baselineValues(3.141592653589793)
        .baselineValues(0.0)
        .baselineValues(0.0)
        .baselineValues(Double.NEGATIVE_INFINITY)
        .baselineValues(Double.NEGATIVE_INFINITY)
        .go();
  }
}
