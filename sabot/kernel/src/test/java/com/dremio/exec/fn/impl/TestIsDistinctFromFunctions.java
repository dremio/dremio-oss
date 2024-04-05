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

import com.dremio.PlanTestBase;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for IS [NOT] DISTINCT FROM functions.
 *
 * <p>Not an exhaustive test list, but contains a test case for each category of type.
 */
public class TestIsDistinctFromFunctions extends PlanTestBase {

  @Test
  public void intType() throws Exception {
    helper("INT_col", "BIGINT_col", "int_distinct_result");
  }

  @Test
  public void varCharType() throws Exception {
    helper("varchar_col1", "varchar_col2", "varchar_distinct_result");
  }

  @Test
  public void timeStampType() throws Exception {
    helper(
        "cast(TIMESTAMP_col1 as TIMESTAMP)",
        "cast(TIMESTAMP_col2 as TIMESTAMP)",
        "timestamp_distinct_result");
  }

  @Test
  @Ignore("decimal")
  public void decimalType() throws Exception {
    helper(
        "cast(DECIMAL9_col as DECIMAL(29, 2))",
        "cast(DECIMAL18_col as DECIMAL(29, 2))",
        "decimal_distinct_result");
  }

  @Test
  public void intervalYearType() throws Exception {
    helper(
        "cast(INTERVALYEAR_col1 as INTERVAL YEAR)",
        "cast(INTERVALYEAR_col2 as INTERVAL YEAR)",
        "intervalyear_distinct_result");
  }

  @Test
  public void intervalDayType() throws Exception {
    helper(
        "cast(INTERVALDAY_col1 as INTERVAL DAY)",
        "cast(INTERVALDAY_col2 as INTERVAL DAY)",
        "intervalday_distinct_result");
  }

  public void helper(String col1, String col2, String expCol) throws Exception {
    String query =
        String.format(
            "SELECT "
                + "%s is distinct from %s as col, %s "
                + "FROM cp.\"functions/distinct_from.json\"",
            col1, col2, expCol);

    testPlanSubstrPatterns(query, new String[] {"IS DISTINCT FROM"}, null);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col", expCol)
        .baselineValues(true, true)
        .baselineValues(false, false)
        .baselineValues(true, true)
        .baselineValues(true, true)
        .baselineValues(false, false)
        .go();

    query =
        String.format(
            "SELECT "
                + "%s is not distinct from %s as col, %s "
                + "FROM cp.\"functions/distinct_from.json\"",
            col1, col2, expCol);

    testPlanSubstrPatterns(query, new String[] {"IS NOT DISTINCT FROM"}, null);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col", expCol)
        .baselineValues(false, true)
        .baselineValues(true, false)
        .baselineValues(false, true)
        .baselineValues(false, true)
        .baselineValues(true, false)
        .go();
  }
}
