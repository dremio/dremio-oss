/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.ExecConstants;

public class TestTpchExplain extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchExplain.class);

  private static final String EXPLAIN_PREFIX = "EXPLAIN PLAN FOR ";


  private void doExplain(String fileName) throws Exception{
    String query = getFile(fileName);
    test(query);
    query = EXPLAIN_PREFIX + query;
    test(query);
  }

  public static void checkPlan(final String fileName, String[] expected, String[] unexpected) throws Exception{
    final String query = getFile(fileName);
    String[] queries = query.split(";");
    for (String q : queries) {
      final String trimmedQuery = q.trim();
      if (trimmedQuery.isEmpty()) {
        continue;
      }
      testPlanMatchingPatterns(q, expected, unexpected);
    }
  }

  @Test
  public void tpch01() throws Exception{
    try {
      test("alter session set planner.slice_target=1");
      doExplain("queries/tpch/01.sql");
    } finally {
      test("alter session set planner.slice_target=" + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  @Ignore // DRILL-512
  public void tpch02() throws Exception{
    doExplain("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    doExplain("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    doExplain("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    doExplain("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    doExplain("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    doExplain("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    doExplain("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    checkPlan("queries/tpch/09.sql",
        new String[] {
            "HashJoin",
            "Scan.*lineitem.parquet.*",
            "Filter.*LIKE\\(\\$1, \\'\\%yellow\\%\\'\\)",
            "Scan.*part.parquet" },
        null);
  }

  @Test
  public void tpch10() throws Exception{
    doExplain("queries/tpch/10.sql");
  }

  @Test
  @Ignore // cartesion problem
  public void tpch11() throws Exception{
    doExplain("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    checkPlan("queries/tpch/12.sql",
        new String[] {
            "00-05.*HashJoin", "00-07.*Project", "00-09.*Scan.*orders.parquet", "00-10.*Filter", "00-11.*Scan.*lineitem.parquet" },
        new String[] {
            "HashToRandomExchange"
        });

    try {
      test("alter session set `planner.slice_target` = 10");
      test("alter session set `planner.broadcast_factor` = 1000.0");
      test("alter session set `planner.broadcast_min_threshold` = 1");
      checkPlan("queries/tpch/12.sql",
          new String[]{
              "HashJoin",
              "Scan.*tpch/orders.parquet",
              "Scan.*tpch/lineitem.parquet"},
          null);
      } finally {
      test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
      test("alter session set `planner.broadcast_factor` = 2.0");
      test("alter session set `planner.broadcast_min_threshold` = 500000");
    }
  }

  @Test
  public void tpch13() throws Exception{
    doExplain("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    doExplain("queries/tpch/14.sql");
  }

  @Test
  @Ignore // non equality join
  public void tpch15() throws Exception{
    doExplain("queries/tpch/15.sql");
  }

  @Test
  public void tpch16() throws Exception{
    doExplain("queries/tpch/16.sql");
  }

  @Test
  public void tpch17() throws Exception{
    checkPlan("queries/tpch/17.sql",
        new String[] {
            "HashJoin",
            "columns=\\[`l_partkey`, `l_quantity`, `l_extendedprice`\\]",
            "columns=\\[`p_partkey`, `p_brand`, `p_container`\\]",
            "columns=\\[`l_partkey`, `l_quantity`\\]",
            "columns=\\[`l_partkey`\\]"},
        new String[] { "MergeJoin", "columns=\\[`\\*`\\]" });
  }

  @Test
  public void tpch18() throws Exception{
    doExplain("queries/tpch/18.sql");
  }

  @Test
  @Ignore // DRILL-519
  public void tpch19() throws Exception{
    doExplain("queries/tpch/19.sql");
  }

  @Test
  public void tpch19_1() throws Exception{
    doExplain("queries/tpch/19_1.sql");
  }


  @Test
  public void tpch20() throws Exception{
    doExplain("queries/tpch/20.sql");
  }

  @Test
  @Ignore
  public void tpch21() throws Exception{
    doExplain("queries/tpch/21.sql");
  }

  @Test
  @Ignore // DRILL-518
  public void tpch22() throws Exception{
    doExplain("queries/tpch/22.sql");
  }
}
