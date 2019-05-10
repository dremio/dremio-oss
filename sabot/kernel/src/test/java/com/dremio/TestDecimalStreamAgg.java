/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;

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

  @Rule
  public ExpectedException exception = ExpectedException.none();

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
  public void testDecimalSumAggOverflow_Parquet() throws Exception {

    exception.expect(UserException.class);
    exception.expectMessage("Overflow happened for decimal addition. Max precision is 38.");

    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\"";

    test(query);
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
  public void testDecimalSumZeroAggOverflow_Parquet() throws Exception {

    exception.expect(UserException.class);
    exception.expectMessage("Overflow happened for decimal addition. Max precision is 38.");

    final String query = "select $sum0(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\"";

    test(query);
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
}
