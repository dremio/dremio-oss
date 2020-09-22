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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;

public class TestTpchLimit0 extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchLimit0.class);

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private void testLimitZeroQuery(String query) throws Exception {
    query = "ALTER SESSION SET \"planner.slice_target\" = 1; select * from \n(" + query.replace(";", ")xyz limit 0;");
    test(query);
  }

  private void testLimitZero(String fileName) throws Exception {
    String query = getFile(fileName);
    testLimitZeroQuery(query);
  }

  @Test
  public void tpch01() throws Exception{
    testLimitZero("queries/tpch/01.sql");
  }

  @Test
  public void tpch02() throws Exception{
    testLimitZero("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testLimitZero("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testLimitZero("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testLimitZero("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testLimitZero("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testLimitZero("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testLimitZero("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testLimitZero("queries/tpch/09.sql");
  }

  @Test
  public void tpch10() throws Exception{
    testLimitZero("queries/tpch/10.sql");
  }

  @Test
  public void tpch11() throws Exception{
    testLimitZero("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    testLimitZero("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testLimitZero("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    testLimitZero("queries/tpch/14.sql");
  }

  @Test
  public void tpch15() throws Exception{
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");

      // NB: can't use query 15 directly, as it has four distinct parts. testLimitZero(), above, assumes a single query
      test("use dfs_test");
      try {
        test("create view revenue0 (supplier_no, total_revenue) as\n" +
          "  select\n" +
          "    l_suppkey,\n" +
          "    sum(l_extendedprice * (1 - l_discount))\n" +
          "  from\n" +
          "    cp.\"tpch/lineitem.parquet\"\n" +
          "  where\n" +
          "    l_shipdate >= date '1993-05-01'\n" +
          "    and l_shipdate < date '1993-05-01' + interval '3' month\n" +
          "  group by\n" +
          "    l_suppkey;\n");
        testLimitZeroQuery("select\n" +
          "    s.s_suppkey,\n" +
          "      s.s_name,\n" +
          "      s.s_address,\n" +
          "      s.s_phone,\n" +
          "      r.total_revenue\n" +
          "    from\n" +
          "    cp.\"tpch/supplier.parquet\" s,\n" +
          "      revenue0 r\n" +
          "      where\n" +
          "    s.s_suppkey = r.supplier_no\n" +
          "    and r.total_revenue = (\n" +
          "      select\n" +
          "    max(total_revenue)\n" +
          "    from\n" +
          "      revenue0\n" +
          "  )\n" +
          "    order by\n" +
          "    s.s_suppkey;\n"
        );
      } finally {
        test("drop view revenue0");
      }
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void tpch16() throws Exception{
    testLimitZero("queries/tpch/16.sql");
  }

  @Test
  public void tpch17() throws Exception{
    testLimitZero("queries/tpch/17.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testLimitZero("queries/tpch/18.sql");
  }

  @Test
  @Ignore("cartesian") // DRILL-519
  public void tpch19() throws Exception{
    testLimitZero("queries/tpch/19.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testLimitZero("queries/tpch/20.sql");
  }

  @Test
  public void tpch21() throws Exception{
    testLimitZero("queries/tpch/21.sql");
  }

  @Test
  public void tpch22() throws Exception{
    testLimitZero("queries/tpch/22.sql");
  }
}
