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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;

public class TestFilterPastJoin extends PlanTestBase {

  @Test
  public void filterInOnClause() throws Exception {
    String sql = "SELECT count(*)\n" +
      "FROM cp.\"tpch/lineitem.parquet\" l1 JOIN cp.\"tpch/lineitem.parquet\" l2\n" +
      "ON l1.l_shipdate = l2.l_receiptdate AND l1.l_orderkey = 32";
    testPlanMatchingPatterns(sql, new String[]{ "(?s)Join.*Filter" }, "(?s)Filter.*Join");
  }

  @Test
  public void filterInWhereClause() throws Exception {
    String sql = "SELECT count(*)\n" +
      "FROM cp.\"tpch/lineitem.parquet\" l1 JOIN cp.\"tpch/lineitem.parquet\" l2\n" +
      "ON l1.l_shipdate = l2.l_receiptdate WHERE l1.l_orderkey = 32";
    testPlanMatchingPatterns(sql, new String[]{ "(?s)Join.*Filter" }, "(?s)Filter.*Join");
  }

  @Test
  public void filterInOnClauseRightJoin() throws Exception {
    String sql = "SELECT count(*)\n" +
      "FROM cp.\"tpch/lineitem.parquet\" l1 RIGHT JOIN cp.\"tpch/lineitem.parquet\" l2\n" +
      "ON l1.l_shipdate = l2.l_receiptdate AND l1.l_orderkey = 32";
    testPlanMatchingPatterns(sql, new String[]{ "(?s)Join.*Filter" }, "(?s)Filter.*Join");
  }

  @Test
  public void filterInOnClauseLeftJoinNegativeTest() throws Exception {
    String sql = "SELECT count(*)\n" +
      "FROM cp.\"tpch/lineitem.parquet\" l1 LEFT JOIN cp.\"tpch/lineitem.parquet\" l2\n" +
      "ON l1.l_shipdate = l2.l_receiptdate AND l1.l_orderkey = 32";

    try {
      test(sql);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("cartesian"));
    }
  }

  @Test
  public void filterInOnClauseTransitive() throws Exception {
    setSessionOption(PlannerSettings.TRANSITIVE_JOIN, "true");
    try {
      String sql = "SELECT count(*)\n" +
        "FROM cp.\"tpch/lineitem.parquet\" JOIN cp.\"tpch/orders.parquet\"\n" +
        "ON l_orderkey = o_orderkey AND l_orderkey = 32";
      testPlanMatchingPatterns(sql, new String[]{"(?s)Join.*Filter.*Filter"}, "(?s)Filter.*Join");
    } finally {
      resetSessionOption(PlannerSettings.TRANSITIVE_JOIN);
    }
  }
}
