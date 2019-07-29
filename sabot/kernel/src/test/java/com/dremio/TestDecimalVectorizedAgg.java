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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;

/**
 * Tests decimal vectorized aggregations - sum, sum0, min, max, count
 * avg and std-dev.
 *
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
public class TestDecimalVectorizedAgg extends DecimalCompleteTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testDecimalSumAgg_Parquet() throws Exception {

    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("113.12345678901234567890"))
      .baselineValues(null)
      .baselineValues(new BigDecimal("11.25691357802469135780"))
      .baselineValues(new BigDecimal("34.23545678901234567890"))
      .go();
  }

  /**
   * Test that we are able to increase precision upto 38.
   * @throws Exception
   */
  @Test
  public void testDecimalSumAggLargeValues_Parquet() throws Exception {

    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/decimals-31-2.parquet\" group by department";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("33399999999999999999999855842467.08"))
      .baselineValues(new BigDecimal("33299999999999999999999856273909.96"))
      .baselineValues(new BigDecimal("33299999999999999999999856274242.96"))
      .go();
  }

  @Test
  @Ignore("DX-11334")
  public void testDecimalSumAggOverflow_Parquet() throws Exception {

    exception.expect(UserException.class);
    exception.expectMessage("Overflow happened for decimal addition. Max precision is 38.");

    final String query = "select sum(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\" group by department";

    test(query);
  }

  @Test
  public void testDecimalSum0Agg_Parquet() throws Exception {

    final String query = "select $sum0(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("113.12345678901234567890"))
      .baselineValues(BigDecimal.valueOf(0, 20))
      .baselineValues(new BigDecimal("11.25691357802469135780"))
      .baselineValues(new BigDecimal("34.23545678901234567890"))
      .go();
  }

  @Test
  @Ignore("DX-11334")
  public void testDecimalSum0AggOverflow_Parquet() throws Exception {

    exception.expect(UserException.class);
    exception.expectMessage("Overflow happened for decimal addition. Max precision is 38.");

    final String query = "select $sum0(val) from cp" +
      ".\"parquet/decimals/overflow.parquet\" group by department";

    test(query);
  }

  @Test
  public void testDecimalMin_Parquet() throws Exception {

    final String query = "select min(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("-11.12345678901234567890"))
      .baselineValues(null)
      .baselineValues(new BigDecimal("-2.00000000000000000000"))
      .baselineValues(new BigDecimal("0.01"))
      .go();
  }

  @Test
  public void testDecimalMax_Parquet() throws Exception {

    final String query = "select max(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("123.12345678901234567890"))
      .baselineValues(null)
      .baselineValues(new BigDecimal("11.12345678901234567890"))
      .baselineValues(new BigDecimal("32.1020"))
      .go();
  }

  @Test
  public void testDecimalAvg_Parquet() throws Exception {

    final String query = "select avg(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(11.411818929670781D)
      .baselineValues(null)
      .baselineValues(37.707818929670786D)
      .baselineValues(2.814228394506173D)
      .go();
  }

  /**
   * Tests when avg returns a decimal of scale larger than input scale.
   */
  @Test
  public void testDecimalAvgScaleUp_Parquet() throws Exception {

    final String query = "select avg(EXPR$0) from cp" +
      ".\"parquet/decimals/avg-scale-up.parquet\" group by c";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(418.09875D)
      .baselineValues(306.4714285714286D)
      .baselineValues(455.7892857142857D)
      .baselineValues(510.179D)
      .baselineValues(628.7854545454546D)
      .go();
  }

  @Test
  public void testDecimalVarPop_Parquet() throws Exception {

    final String query = "select var_pop(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(214.78624629467512D)
      .baselineValues(null)
      .baselineValues(3672.9134108236926)
      .baselineValues(25.140233461512075)
      .go();
  }

  @Test
  public void testDecimalVariance_Parquet() throws Exception {

    final String query = "select variance(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(5509.370116235539D)
      .baselineValues(null)
      .baselineValues(33.5203112820161D)
      .baselineValues(322.17936944201267D)
      .go();
  }

  @Test
  public void testDecimalStddev_Parquet() throws Exception {

    final String query = "select stddev(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(74.22513129820344D)
      .baselineValues(null)
      .baselineValues(17.94935568319968D)
      .baselineValues(5.789672813036684D)
      .go();
  }

  @Test
  public void testDecimalStddevPop_Parquet() throws Exception {

    final String query = "select stddev_pop(val) from cp" +
      ".\"parquet/decimals/simple-decimals-with-nulls.parquet\" group by a";

    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(60.60456592389465D)
      .baselineValues(null)
      .baselineValues(14.655587545188187D)
      .baselineValues(5.01400373568988D)
      .go();
  }

}
