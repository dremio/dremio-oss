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
package com.dremio.exec.planner.logical;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestConvertCountToDirectScan extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestConvertCountToDirectScan.class);

  @Test
  public void ensureCaseDoesntConvertToDirectScan() throws Exception {
    testPlanMatchingPatterns(
        "select count(case when r_name = 'ALGERIA' and r_regionkey = 2 then r_regionkey else null end) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/directcount.parquet\"",
        new String[] { "CASE" },
        new String[]{});
  }

  @Test
  public void testConvertToDirectScanWithNoColStats() throws Exception {
    try {
      //Bug 9548
      String sql = "select count(l_orderkey) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/dremio-no-column-stats.parquet\"";
      testPlanMatchingPatterns(
        sql,
        new String[]{},
        new String[]{"Values"});

      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(60175L)
        .go();
    } catch (Exception e) {
      assert (false);
    }
  }

  @Test
  public void ensureConvertSimpleCountToDirectScan() throws Exception {
    final String sql = "select count(*) as cnt from cp.\"tpch/nation.parquet\"";
    testPlanMatchingPatterns(
        sql,
        new String[] { "Values" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureConvertSimpleCountConstToDirectScan() throws Exception {
    final String sql = "select count(100) as cnt from cp.\"tpch/nation.parquet\"";
    testPlanMatchingPatterns(
        sql,
        new String[] { "Values" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureConvertSimpleCountConstExprToDirectScan() throws Exception {
    final String sql = "select count(1 + 2) as cnt from cp.\"tpch/nation.parquet\"";
    testPlanMatchingPatterns(
        sql,
        new String[] { "Values" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

}
