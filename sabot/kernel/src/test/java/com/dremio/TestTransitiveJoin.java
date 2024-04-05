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

import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestTransitiveJoin extends PlanTestBase {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestTransitiveJoin.class);

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void setup() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test
  public void testTransitiveJoin() throws Exception {
    testPlanOneExpectedPattern(
        ""
            + "select l.l_orderkey as x, c.c_custkey as y \n"
            + "  from cp.\"tpch/lineitem.parquet\" l "
            + "  join cp.\"tpch/customer.parquet\" c "
            + "  on l.l_orderkey = c.c_custkey "
            + "where l_orderkey = 1 limit 1"
            + "",
        Pattern.quote("Filter(condition=[=($0, 1)]) : rowType = RecordType(INTEGER c_custkey)"));
  }

  @Test
  public void testTransitiveJoinThroughCast() throws Exception {
    test("use dfs_test");
    test(
        "create vds dfs_test.l as select cast(l_orderkey as varchar) as l_orderkey from cp.\"tpch/lineitem.parquet\"");
    test(
        "create vds dfs_test.r as select cast(o_orderkey as varchar) as o_orderkey from cp.\"tpch/orders.parquet\"");

    final String onVds =
        "select r.o_orderkey, l.l_orderkey \n"
            + " from dfs_test.l  l "
            + " join dfs_test.r r "
            + " on r.o_orderkey = l.l_orderkey"
            + " where l.l_orderkey = '2' "
            + "";

    testPlanMatchingPatterns(
        onVds,
        new String[] {
          Pattern.quote(
              "Filter(condition=[=(CAST($0):VARCHAR(65536), '2')]) : rowType = RecordType(INTEGER l_orderkey)"),
          Pattern.quote(
              "Filter(condition=[=(CAST($0):VARCHAR(65536), '2')]) : rowType = RecordType(INTEGER o_orderkey):")
        });

    test("drop view dfs_test.l");
    test("drop view dfs_test.r");
  }

  @Test
  public void testTransitiveJoinThroughMathExpression() throws Exception {
    test("use dfs_test");
    test(
        "create vds dfs_test.l as select l_orderkey + 2 as l_orderkey from cp.\"tpch/lineitem.parquet\"");
    test(
        "create vds dfs_test.r as select o_orderkey + 2 as o_orderkey from cp.\"tpch/orders.parquet\"");

    final String onVds =
        "select r.o_orderkey, l.l_orderkey \n"
            + " from dfs_test.l  l "
            + " join dfs_test.r r "
            + " on r.o_orderkey = l.l_orderkey"
            + " where l.l_orderkey = 2 "
            + "";

    testPlanMatchingPatterns(
        onVds,
        new String[] {
          Pattern.quote(
              " Filter(condition=[=(+($0, 2), 2)]) : rowType = RecordType(INTEGER o_orderkey):"),
          Pattern.quote(
              "Filter(condition=[=(+($0, 2), 2)]) : rowType = RecordType(INTEGER l_orderkey)")
        });

    test("drop view dfs_test.l");
    test("drop view dfs_test.r");
  }
}
