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

import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;

public class TestMultipleCountDistinct extends PlanTestBase {
  @Test
  public void testPlan() throws Exception {
    try (AutoCloseable option = withOption(PlannerSettings.ENABLE_DISTINCT_AGG_WITH_GROUPING_SETS, true)) {
      String sql = "select n_regionkey, count(distinct n_name) dist_name, count(distinct n_nationkey) dist_key from cp.\"tpch/nation.parquet\" group by n_regionkey";
      testPlanMatchingPatterns(sql, new String[]{"NestedLoopJoin"}, "HashJoin");
    }
  }

  @Test
  public void testExecution() throws Exception {
    try (AutoCloseable option = withOption(PlannerSettings.ENABLE_DISTINCT_AGG_WITH_GROUPING_SETS, true)) {
      String sql = "select n_regionkey, count(distinct n_name) dist_name, count(distinct n_nationkey) dist_key from cp.\"tpch/nation.parquet\" group by n_regionkey";
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("n_regionkey", "dist_name", "dist_key")
        .baselineValues(0, 5L, 5L)
        .baselineValues(1, 5L, 5L)
        .baselineValues(2, 5L, 5L)
        .baselineValues(3, 5L, 5L)
        .baselineValues(4, 5L, 5L)
        .go();
    }
  }
}
