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
package com.dremio.exec.planner;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;

public class TestHyperLogLog extends PlanTestBase {

  @Test
  public void testHLL() throws Exception {
    runTestExpected(15000, true);
    test("ALTER TABLE cp.\"tpch/lineitem.parquet\" ENABLE APPROXIMATE STATS");
    runTestExpected(14906, false);
    test("ALTER TABLE cp.\"tpch/lineitem.parquet\" DISABLE APPROXIMATE STATS");
    runTestExpected(15000, true);
  }

  private void runTestExpected(long value, boolean expectExact) throws Exception {
    final String sql = "select count(distinct l_orderkey) as cnt from cp.\"tpch/lineitem.parquet\"";
    final String exactPlan = "StreamAgg(group=[{}], cnt=[COUNT($0)])";
    final String ndvPlan = "StreamAgg(group=[{}], cnt=[HLL($0)])";
    if (expectExact) {
      testPlanSubstrPatterns(sql, new String[]{exactPlan}, new String[]{ndvPlan});
    } else {
      testPlanSubstrPatterns(sql, new String[]{ndvPlan}, new String[]{exactPlan});
    }
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(value)
      .build()
      .run();
  }

  @Test
  public void ndv() throws Exception {
    test("set planner.slice_target = 1");
    test("SELECT l_returnflag, sum(l_extendedprice), ndv(l_partkey)\n" +
      "FROM cp.\"tpch/lineitem.parquet\"\n" +
      "group by l_returnflag");
  }

  @Ignore("DX-10395")
  @Test
  public void ndv2() throws Exception {
    test("set planner.slice_target = 1");
    test("SELECT l_returnflag, sum(l_extendedprice), ndv(l_partkey), ndv(l_suppkey)\n" +
      "FROM cp.\"tpch/lineitem.parquet\"\n" +
      "group by l_returnflag");
  }

  @Test
  public void testNdvSinglePhase() throws Exception {
    testBuilder()
      .sqlQuery("select ndv(l_linenumber) as ndv_linenumber, ndv(l_orderkey) ndv_orderkey, ndv(concat(l_orderkey, l_linenumber)) ndv_key, count(1) as cnt from cp.\"tpch/lineitem.parquet\"")
      .ordered()
      .baselineColumns("ndv_linenumber", "ndv_orderkey", "ndv_key", "cnt")
      .baselineValues(7L,14906L, 60336L, 60175L)
      .go();
  }

  @Test
  public void testNdvAllTypes() throws Exception {
    String sql = "SELECT \n" +
        "    ndv(bool_col) as a, \n" +
        "    ndv(int_col) as b,\n" +
        "    ndv(bigint_col) as c, \n" +
        "    ndv(float4_col) as d, \n" +
        "    ndv(float8_col) as e, \n" +
        "    ndv(date_col) as f, \n" +
        "    ndv(timestamp_col) as g, \n" +
        "    ndv(time_col) as h, \n" +
        "    ndv(varchar_col) as i, \n" +
        "    ndv(varbinary_col) as j,\n" +
        "    ndv(INTERVAL '4 5:12:10' DAY TO SECOND) as k, \n" +
        "    ndv(INTERVAL '2-6' YEAR TO MONTH) as l\n" +
        "FROM cp.parquet.\"all_scalar_types.parquet\"";
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l")
      .baselineValues(1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l)
      .go();
  }

  @Test
  public void testNdvMultiPhase() throws Exception {
    test("set planner.slice_target = 1");
    String sql = "select ndv(l_linenumber) as ndv_linenumber, ndv(l_orderkey) ndv_orderkey, ndv(concat(l_orderkey, l_linenumber)) ndv_key, count(1) as cnt from cp.\"tpch/lineitem.parquet\"";
    test(sql);
    try {
      testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("ndv_linenumber", "ndv_orderkey", "ndv_key", "cnt")
        .baselineValues(7L, 14906L, 60336L, 60175L)
        .go();
    } finally {
      testNoResult("set planner.slice_target = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }
}
