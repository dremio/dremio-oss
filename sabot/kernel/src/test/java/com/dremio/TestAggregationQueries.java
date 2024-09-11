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

public class TestAggregationQueries extends PlanTestBase {

  @Test // DRILL-4521
  public void ensureVarianceIsAggregateReduced() throws Exception {
    String query01 = "select variance(salary) from cp.\"employee.json\"";
    testPlanSubstrPatterns(
        query01,
        new String[] {"EXPR$0=[/(-($0, /(*($1, $1), $2)), CASE(=($2, 1), null:BIGINT, -($2, 1)))]"},
        new String[] {"EXPR$0=[VARIANCE($0)]"});
    testBuilder()
        .sqlQuery(query01)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(2.8856749581279494E7)
        .go();

    String query02 = "select var_samp(salary) from cp.\"employee.json\"";
    testBuilder()
        .sqlQuery(query02)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(2.8856749581279494E7)
        .go();

    String query03 = "select var_pop(salary) from cp.\"employee.json\"";
    testBuilder()
        .sqlQuery(query03)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(2.8831765382507823E7)
        .go();
  }

  @Test // DRILL-4521
  public void ensureStddevIsAggregateReduced() throws Exception {
    String query01 = "select stddev(salary) from cp.\"employee.json\"";
    testPlanSubstrPatterns(
        query01,
        new String[] {
          "EXPR$0=[POWER(/(-($0, /(*($1, $1), $2)), CASE(=($2, 1), null:BIGINT, -($2, 1))), 0.5:DECIMAL(2, 1))]"
        },
        new String[] {"EXPR$0=[STDDEV($0)]"});
    testBuilder()
        .sqlQuery(query01)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(5371.84787398894)
        .go();

    String query02 = "select stddev_samp(salary) from cp.\"employee.json\"";
    testBuilder()
        .sqlQuery(query02)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(5371.84787398894)
        .go();

    String query03 = "select stddev_pop(salary) from cp.\"employee.json\"";
    testBuilder()
        .sqlQuery(query03)
        .approximateEquality()
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(5369.521895151171)
        .go();
  }

  @Test
  public void testAggJoinPushdown() throws Exception {
    testNoResult("use cp.tpch");
    String query =
        "select l_orderkey, count(*) cnt from \"lineitem.parquet\" join \"orders.parquet\"\n"
            + "on l_orderkey = o_orderkey\n"
            + "group by l_orderkey";
    testPlanMatchingPatterns(query, new String[] {"(?s)Join.*Agg.*Agg"});
  }

  @Test
  public void testAggJoinPushdownSumDecimal() throws Exception {
    testNoResult("use cp.tpch");
    String queryTemplate =
        "with lineitem as (select l_orderkey, cast(l_extendedprice as DECIMAL(%d,%d)) l_extendedprice\n"
            + "   from cp.tpch.\"lineitem.parquet\")\n"
            + "select l_orderkey, sum(l_extendedprice) rev from lineitem join \"orders.parquet\"\n"
            + "on l_orderkey = o_orderkey\n"
            + "group by l_orderkey";
    testPlanMatchingPatterns(
        String.format(queryTemplate, 10, 5), new String[] {"(?s)Join.*Agg.*Agg"});
    testPlanMatchingPatterns(
        String.format(queryTemplate, 38, 10), new String[] {"(?s)Agg.*Join"}, "(?s)Join.*Agg.*Agg");
  }
}
