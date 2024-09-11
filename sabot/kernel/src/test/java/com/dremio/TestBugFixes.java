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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestBugFixes extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestBugFixes.class);
  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  @Test
  public void leak1() throws Exception {
    String select =
        "select count(*) \n"
            + "    from cp.\"tpch/part.parquet\" p1, cp.\"tpch/part.parquet\" p2 \n"
            + "    where p1.p_name = p2.p_name \n"
            + "  and p1.p_mfgr = p2.p_mfgr";
    test(select);
  }

  @Test
  public void failingSmoke() throws Exception {
    String select =
        "select count(*) \n"
            + "  from (select l.l_orderkey as x, c.c_custkey as y \n"
            + "  from cp.\"tpch/lineitem.parquet\" l \n"
            + "    left outer join cp.\"tpch/customer.parquet\" c \n"
            + "      on l.l_orderkey = c.c_custkey) as foo\n"
            + "  where x < 10000";
    test(select);
  }

  @Test
  public void DRILL883() throws Exception {
    test(
        "select n1.n_regionkey from cp.\"tpch/nation.parquet\" n1, (select n_nationkey from cp.\"tpch/nation.parquet\") as n2 where n1.n_nationkey = n2.n_nationkey");
  }

  @Test
  public void TestAlternatePagesFilterMatch() throws Exception {
    test("select col1,col2,col3 from cp.\"parquet/alternate_pages.parquet\"  where col1 = 256");
  }

  @Test
  public void DRILL1061() throws Exception {
    String query =
        "select foo.mycol.x as COMPLEX_COL from (select convert_from('{ x : [1,2], y : 100 }', 'JSON') as mycol from cp.\"tpch/nation.parquet\") as foo(mycol) limit 1";
    test(query);
  }

  @Test
  public void DRILL1126() throws Exception {
    String query =
        "select sum(cast(employee_id as decimal(38, 18))), avg(cast(employee_id as decimal(38, 18))) from cp.\"employee.json\" group by (department_id)";
    test(query);
  }

  @Test // DX-85230
  public void testAmbiguousColumn() throws Exception {
    test(String.format("alter session set \"%s\" = true", "planner.allow_ambiguous_column"));
    String query =
        "select * from (select employee_id, department_id, 2 as employee_id from cp.\"employee.json\") ";
    test(query);

    test(String.format("alter session set \"%s\" = false", "planner.allow_ambiguous_column"));
    assertThatThrownBy(() -> test(query))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("VALIDATION ERROR: Column 'employee_id' is ambiguous");
  }

  @Test // DX-87539
  public void testCheckAmbiguousColumn() throws Exception {
    test(String.format("alter session set \"%s\" = true", "planner.allow_ambiguous_column"));
    String ddl = "create table \"dfs_test\".qwerty(c_int int, c_bigint bigint)";
    test(ddl);
    String query =
        "select is_current_ancestor from TABLE(table_history('dfs_test.qwerty')) order by made_current_at";
    test(query); // Should succeed rather than throw VALIDATION ERROR: Table 'EXPR$0' not found
  }

  @Test(expected = UserException.class)
  @Ignore
  // Should be "Failure while parsing sql. SabotNode [rel#26:Subset#6.LOGICAL.ANY([]).[]] could not
  // be implemented;".
  // Dremio will hit CanNotPlan, until we add code fix to transform the local LHS filter in left
  // outer join properly.
  public void testDRILL1337_LocalLeftFilterLeftOutJoin() throws Exception {
    try {
      test(
          "select count(*) from cp.\"tpch/nation.parquet\" n left outer join cp.\"tpch/region.parquet\" r on n.n_regionkey = r.r_regionkey and n.n_nationkey > 10;");
    } catch (UserException e) {
      logger.info("***** Test resulted in expected failure: " + e.getMessage());
      throw e;
    }
  }

  @Test
  public void testDRILL1337_LocalRightFilterLeftOutJoin() throws Exception {
    test(
        "select * from cp.\"tpch/nation.parquet\" n left outer join cp.\"tpch/region.parquet\" r on n.n_regionkey = r.r_regionkey and r.r_name not like '%ASIA' order by r.r_name;");
  }

  @Test
  public void testDRILL2361_AggColumnAliasWithDots() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) as \"test.alias\" from cp.\"employee.json\"")
        .unOrdered()
        .baselineColumns("`test.alias`")
        .baselineValues(1155L)
        .build()
        .run();
  }

  @Test
  public void testDRILL2361_SortColumnAliasWithDots() throws Exception {
    testBuilder()
        .sqlQuery(
            "select o_custkey as \"x.y.z\" from cp.\"tpch/orders.parquet\" where o_orderkey < 5 order by \"x.y.z\"")
        .unOrdered()
        .baselineColumns("`x.y.z`")
        .baselineValues(370)
        .baselineValues(781)
        .baselineValues(1234)
        .baselineValues(1369)
        .build()
        .run();
  }

  @Test
  public void testDRILL2361_JoinColumnAliasWithDots() throws Exception {
    testBuilder()
        .sqlQuery(
            "select count(*) as cnt from (select o_custkey as \"x.y\" from cp.\"tpch/orders.parquet\") o inner join cp.\"tpch/customer.parquet\" c on o.\"x.y\" = c.c_custkey")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(15000L)
        .build()
        .run();
  }

  @Ignore
  @Test
  public void testDRILL4192() throws Exception {

    String query =
        (String.format(
            "select dir0, dir1 from dfs.\"%s/bugs/DRILL-4192\" order by dir1", TEST_RES_PATH));
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("dir0", "dir1")
        .baselineValues("single_top_partition", "nested_partition_1")
        .baselineValues("single_top_partition", "nested_partition_2")
        .go();

    query =
        (String.format(
            "select dir0, dir1 from dfs.\"%s/bugs/DRILL-4192/*/nested_partition_1\" order by dir1",
            TEST_RES_PATH));

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("dir0", "dir1")
        .baselineValues("single_top_partition", "nested_partition_1")
        .go();
  }

  @Test // DRILL-4971
  public void testVisitBooleanOrWithoutFunctionsEvaluation() throws Exception {
    String query =
        "SELECT\n"
            + "CASE WHEN employee_id IN (1) THEN 1 ELSE 0 END \"first\"\n"
            + ", CASE WHEN employee_id IN (2) THEN 1 ELSE 0 END \"second\"\n"
            + ", CASE WHEN employee_id IN (1, 2) THEN 1 ELSE 0 END \"any\"\n"
            + "FROM cp.\"employee.json\" ORDER BY employee_id limit 2";

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
    String query =
        "SELECT employee_id FROM cp.\"employee.json\" WHERE\n"
            + "((employee_id > 1 AND employee_id < 3) OR (employee_id > 9 AND employee_id < 11))\n"
            + "AND (employee_id > 1 AND employee_id < 3)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("employee_id")
        .baselineValues((long) 2)
        .go();
  }

  @Test // DX-40352
  public void testLiteralMathExpression() throws Exception {
    final String query =
        "SELECT employee_id, (employee_id * (2 + 2)) \n"
            + "FROM cp.\"employee.json\" WHERE employee_id < 10";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("employee_id", "EXPR$1")
        .baselineValues(1L, 4L)
        .baselineValues(2L, 8L)
        .baselineValues(4L, 16L)
        .baselineValues(5L, 20L)
        .baselineValues(6L, 24L)
        .baselineValues(7L, 28L)
        .baselineValues(8L, 32L)
        .baselineValues(9L, 36L)
        .go();
  }

  @Test // DX-62402
  public void testExpressionSplitterNestedGandivaFunction() throws Exception {
    final String query =
        "SELECT COUNT(DISTINCT CASE\n"
            + "           WHEN TRIM(BOTH ' '\n"
            + "                     FROM to_utf8(CASE\n"
            + "           WHEN POSITION('-' IN \"o_orderpriority\") = 0 THEN \"o_orderpriority\"\n"
            + "           ELSE RIGHT(\"o_orderpriority\", LENGTH(\"o_orderpriority\") - POSITION('-' IN \"o_orderpriority\"))\n"
            + "       END,'Windows-1250')) LIKE 'MEDIUM' THEN 'MED'\n"
            + "           WHEN TRIM(BOTH ' '\n"
            + "                     FROM to_utf8(CASE\n"
            + "           WHEN POSITION('-' IN \"o_orderpriority\") = 0 THEN \"o_orderpriority\"\n"
            + "           ELSE RIGHT(\"o_orderpriority\", LENGTH(\"o_orderpriority\") - POSITION('-' IN \"o_orderpriority\"))\n"
            + "       END,'Windows-1250')) LIKE 'URGENT' THEN 'HI'\n"
            + "       END) AS \"Result\" from cp.\"tpch/orders.parquet\"";

    testBuilder().sqlQuery(query).unOrdered().baselineColumns("Result").baselineValues(2L).go();
  }
}
