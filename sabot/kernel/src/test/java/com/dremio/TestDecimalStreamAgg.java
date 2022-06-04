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
package com.dremio;

import java.math.BigDecimal;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.test.UserExceptionAssert;

/**
 * Tests streaming aggregates when no group by clause is present.
 * Uses test file that is equivalent of
 [
 {"a":"1", "val" : "123.12345678901234567890"},
 {"a":"1", "val" : "1.12345678901234567890"},
 {"a":"1", "val" : "-11.12345678901234567890"},
 {"a":"2", "val" : "11.12345678901234567890"},
 {"a":"2"},
 {"a":"2", "val" : "2.12345678901234567890"},
 {"a":"2", "val" : "0.01"},
 {"a":"2", "val" : "-2.0"},
 {"a":"3"},
 {"a":"3"},
 {"a":"4", "val" : "0.01"},
 {"a":"4", "val" : "2.12345678901234567890"},
 {"a":"4", "val" : "32.1020"}
 ]
 */
public class TestDecimalStreamAgg extends DecimalCompleteTest {

  @Test
  public void testDecimalSumAgg_Parquet() throws Exception {

    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("158.61582715604938271560"))
      .go();
  }

  @Test
  @Ignore("DX-11334")
  public void testDecimalSumAggOverflow_Parquet() {
    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\"";

    UserExceptionAssert.assertThatThrownBy(() -> test(query))
      .hasMessageContaining("Overflow happened for decimal addition. Max precision is 38.");
  }

  @Test
  public void testDecimalSumZeroAgg_Parquet() throws Exception {

    final String query = "select $sum0(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("158.61582715604938271560"))
      .go();
  }

  @Test
  @Ignore("DX-11334")
  public void testDecimalSumZeroAggOverflow_Parquet() {

    final String query = "select $sum0(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\"";

    UserExceptionAssert.assertThatThrownBy(() -> test(query))
      .hasMessageContaining("Overflow happened for decimal addition. Max precision is 38.");
  }

  @Test
  public void testDecimalMin_Parquet() throws Exception {

    final String query = "select min(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("-11.12345678901234567890"))
      .go();
  }

  @Test
  public void testDecimalMax_Parquet() throws Exception {

    final String query = "select max(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("123.12345678901234567890"))
      .go();
  }

  @Test
  public void testDecimalMaxByteValuesSameAsMinDecimal_Json() throws Exception {

    final String query = "select max(cast(a as decimal(38,2))) from cp" +
      ".\"decimal/test_agg_max.json\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("-999999999999999999999999999999999999.99"))
      .go();
  }

  @Test
  public void testDecimalAvg_Parquet() throws Exception {

    final String query = "select avg(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(15.86158271560494D)
      .go();
  }

  @Test
  public void testDecimalVariance_Parquet() throws Exception {

    final String query = "select variance(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(1548.4187808596055D)
      .go();
  }

  @Test
  public void testDecimalVarPop_Parquet() throws Exception {

    final String query = "select var_pop(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(1393.5769027736449D)
      .go();
  }

  @Test
  public void testDecimalStdDev_Parquet() throws Exception {

    final String query = "select stddev(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(39.34995274278745D)
      .go();
  }

  @Test
  public void testDecimalStdDevPop_Parquet() throws Exception {

    final String query = "select stddev_pop(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\"";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(37.33064294615946D)
      .go();
  }
}
