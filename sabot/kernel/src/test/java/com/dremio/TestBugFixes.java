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

import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestBugFixes extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBugFixes.class);
  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";


  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  @Test
  public void leak1() throws Exception {
    String select = "select count(*) \n" +
        "    from cp.\"tpch/part.parquet\" p1, cp.\"tpch/part.parquet\" p2 \n" +
        "    where p1.p_name = p2.p_name \n" +
        "  and p1.p_mfgr = p2.p_mfgr";
    test(select);
  }

  @Test
  public void failingSmoke() throws Exception {
    String select = "select count(*) \n" +
        "  from (select l.l_orderkey as x, c.c_custkey as y \n" +
        "  from cp.\"tpch/lineitem.parquet\" l \n" +
        "    left outer join cp.\"tpch/customer.parquet\" c \n" +
        "      on l.l_orderkey = c.c_custkey) as foo\n" +
        "  where x < 10000";
    test(select);
  }

  @Test
  public void testSysNodes() throws Exception {
    test("select * from sys.nodes");
  }

  @Test
  public void testVersionTable() throws Exception {
    test("select * from sys.version");
  }

  @Test
  public void DRILL883() throws Exception {
    test("select n1.n_regionkey from cp.\"tpch/nation.parquet\" n1, (select n_nationkey from cp.\"tpch/nation.parquet\") as n2 where n1.n_nationkey = n2.n_nationkey");
  }

  @Test
  public void TestAlternatePagesFilterMatch() throws Exception {
    test("select col1,col2,col3 from cp.\"parquet/alternate_pages.parquet\"  where col1 = 256");
  }

  @Test
  public void DRILL1061() throws Exception {
    String query = "select foo.mycol.x as COMPLEX_COL from (select convert_from('{ x : [1,2], y : 100 }', 'JSON') as mycol from cp.\"tpch/nation.parquet\") as foo(mycol) limit 1";
    test(query);
  }

  @Test
  public void DRILL1126() throws Exception {
    try {
      test(String.format("alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      String query = "select sum(cast(employee_id as decimal(38, 18))), avg(cast(employee_id as decimal(38, 18))) from cp.\"employee.json\" group by (department_id)";
      test(query);
    } finally {
      test(String.format("alter session set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
    }
  }

  @Test (expected = UserException.class)
  @Ignore
  // Should be "Failure while parsing sql. SabotNode [rel#26:Subset#6.LOGICAL.ANY([]).[]] could not be implemented;".
  // Dremio will hit CanNotPlan, until we add code fix to transform the local LHS filter in left outer join properly.
  public void testDRILL1337_LocalLeftFilterLeftOutJoin() throws Exception {
    try {
      test("select count(*) from cp.\"tpch/nation.parquet\" n left outer join cp.\"tpch/region.parquet\" r on n.n_regionkey = r.r_regionkey and n.n_nationkey > 10;");
    } catch (UserException e) {
      logger.info("***** Test resulted in expected failure: " + e.getMessage());
      throw e;
    }
  }

  @Test
  public void testDRILL1337_LocalRightFilterLeftOutJoin() throws Exception {
    test("select * from cp.\"tpch/nation.parquet\" n left outer join cp.\"tpch/region.parquet\" r on n.n_regionkey = r.r_regionkey and r.r_name not like '%ASIA' order by r.r_name;");
  }

  @Test
  public void testDRILL2361_AggColumnAliasWithDots() throws Exception {
    testBuilder()
      .sqlQuery("select count(*) as \"test.alias\" from cp.\"employee.json\"")
      .unOrdered()
      .baselineColumns("`test.alias`")
      .baselineValues(1155L)
      .build().run();
  }

  @Test
  public void testDRILL2361_SortColumnAliasWithDots() throws Exception {
    testBuilder()
            .sqlQuery("select o_custkey as \"x.y.z\" from cp.\"tpch/orders.parquet\" where o_orderkey < 5 order by \"x.y.z\"")
            .unOrdered()
            .baselineColumns("`x.y.z`")
            .baselineValues(370)
            .baselineValues(781)
            .baselineValues(1234)
            .baselineValues(1369)
            .build().run();
  }

  @Test
  public void testDRILL2361_JoinColumnAliasWithDots() throws Exception {
    testBuilder()
            .sqlQuery("select count(*) as cnt from (select o_custkey as \"x.y\" from cp.\"tpch/orders.parquet\") o inner join cp.\"tpch/customer.parquet\" c on o.\"x.y\" = c.c_custkey")
            .unOrdered()
            .baselineColumns("cnt")
            .baselineValues(15000L)
            .build().run();
  }

  @Test
  public void testDRILL4192() throws Exception {

    String query = (String.format("select dir0, dir1 from dfs.\"%s/bugs/DRILL-4192\" order by dir1", TEST_RES_PATH));
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("dir0", "dir1")
        .baselineValues("single_top_partition", "nested_partition_1")
        .baselineValues("single_top_partition", "nested_partition_2")
        .go();

    query = (String.format("select dir0, dir1 from dfs.\"%s/bugs/DRILL-4192/*/nested_partition_1\" order by dir1", TEST_RES_PATH));

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("dir0", "dir1")
        .baselineValues("single_top_partition", "nested_partition_1")
        .go();
  }

  @Test // DRILL-4971
  public void testVisitBooleanOrWithoutFunctionsEvaluation() throws Exception {
    String query = "SELECT\n" +
        "CASE WHEN employee_id IN (1) THEN 1 ELSE 0 END \"first\"\n" +
        ", CASE WHEN employee_id IN (2) THEN 1 ELSE 0 END \"second\"\n" +
        ", CASE WHEN employee_id IN (1, 2) THEN 1 ELSE 0 END \"any\"\n" +
        "FROM cp.\"employee.json\" ORDER BY employee_id limit 2";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("first", "second", "any")
        .baselineValues(1, 0, 1)
        .baselineValues(0, 1, 1)
        .go();
  }

  @Test // DRILL-4971
  public void testVisitBooleanAndWithoutFunctionsEvaluation() throws Exception {
    String query = "SELECT employee_id FROM cp.\"employee.json\" WHERE\n" +
        "((employee_id > 1 AND employee_id < 3) OR (employee_id > 9 AND employee_id < 11))\n" +
        "AND (employee_id > 1 AND employee_id < 3)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("employee_id")
        .baselineValues((long) 2)
        .go();
}
}
