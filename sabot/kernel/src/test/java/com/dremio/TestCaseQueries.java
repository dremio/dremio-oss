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

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.test.TemporarySystemProperties;

public class TestCaseQueries extends PlanTestBase {

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void setup() throws Exception {
    testNoResult("ALTER SESSION SET \"%s\" = 5", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult("ALTER SESSION SET \"%s\" = true", ExecConstants.ENABLE_VERBOSE_ERRORS.getOptionName());
    testNoResult("ALTER SESSION SET \"%s\" = 500",
      ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testNoResult("ALTER SESSION RESET \"%s\"", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult("ALTER SESSION RESET \"%s\"", ExecConstants.ENABLE_VERBOSE_ERRORS.getOptionName());
    testNoResult("ALTER SESSION RESET \"%s\"", ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
  }

  @Test
  public void testFixedCaseClause() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE n_regionkey \n" +
      "                     WHEN 0 THEN 'AFRICA' \n" +
      "                     WHEN 1 THEN 'AMERICA' \n" +
      "                     WHEN 2 THEN 'ASIA' \n" +
      "                     ELSE NULL \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('ASIA', 'AMERICA')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(10L)
      .go();
  }

  @Test
  public void testFixedCaseEnsureOrder() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE \n" +
      "                     WHEN n_nationkey = 0 THEN 'AFRICA' \n" +
      "                     WHEN n_nationkey >= 0 THEN 'AMERICA' \n" +
      "                     WHEN n_nationkey = 2 THEN 'ASIA' \n" +
      "                     ELSE 'OTHER' \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('AFRICA', 'OTHER', 'ASIA')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(1L)
      .go();
  }

  @Test
  public void testFixedNestedCase() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE n_regionkey \n" +
      "                     WHEN 0 THEN CASE WHEN n_nationkey < 10  THEN 'FIRST AFRICAN' ELSE 'AFRICAN' END \n" +
      "                     WHEN 1 THEN 'AMERICA' \n" +
      "                     WHEN 2 THEN 'ASIA' \n" +
      "                     ELSE CASE WHEN n_nationkey > 5 THEN 'LAST OTHER' " +
      "                               WHEN n_nationkey < 10 THEN 'MIDDLE OTHER' " +
      "                               ELSE 'OTHER' END \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('FIRST AFRICAN', 'LAST OTHER', 'MIDDLE OTHER')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(12L)
      .go();
  }

  @Test
  public void testSingleConditionCase() throws Exception {
    final String sql = "select n_name as name, " +
      "case when n_regionkey = 0 then concat(n_name, '_AFRICA') else concat(n_name, '_OTHER') end as nation " +
      "from cp.\"tpch/nation.parquet\" limit 4";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "nation")
      .baselineValues("ALGERIA", "ALGERIA_AFRICA")
      .baselineValues("ARGENTINA", "ARGENTINA_OTHER")
      .baselineValues("BRAZIL", "BRAZIL_OTHER")
      .baselineValues("CANADA", "CANADA_OTHER")
      .go();
  }

  private static final int MAX_CASE_CONDITIONS = 500;
  @Test
  public void testLargeCaseStatement() throws Exception {
    final String sql = String.format("select %s from cp.\"tpch/nation.parquet\" limit 5",
      projectLargeCase(MAX_CASE_CONDITIONS, 1));
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("nation")
      .baselineValues("ALGERIA_ABC")
      .baselineValues("argentina_xy1")
      .baselineValues("brazil_xy2")
      .baselineValues("canada_xy3")
      .baselineValues("egypt_xy4")
      .go();
  }

  private static final int NUM_NEXT_CASE_CONDITIONS = 300;
  private static final int NUM_FIRST_CASE_CONDITIONS = 4;
  @Test
  public void testLargeNestedCaseWithLike() throws Exception {
    StringBuilder sb = new StringBuilder("CASE ");
    for (int i = 0; i < NUM_FIRST_CASE_CONDITIONS; i++) {
      final char valToCheck = (char) ('a' + i);
      sb.append("WHEN o_comment LIKE '%").append(valToCheck).append("%'")
        .append(" THEN ").append(System.lineSeparator())
        .append(projectOrderCaseRepeatingCondition(NUM_NEXT_CASE_CONDITIONS, i))
        .append(System.lineSeparator());
    }
    sb.append("ELSE 999 END as _final").append(System.lineSeparator());
    final String sql = String.format("select count(*) as _total from " +
        "(select %s, %s from cp.\"tpch/orders.parquet\" ) where _final != 0",
      sb, projectOrderCase(650000, 10, 10));
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_total")
      .baselineValues(1065L)
      .go();
  }

  private static final int LAST_ORDER_KEY = 60000;
  private static final int RANGE_PER_CASE = 100;
  private static final int NUM_CASE_CONDITIONS = 450;
  @Test
  public void testLargeCaseReverse() throws Exception {
    final String sql = String.format("with t1 as (select %s from cp.\"tpch/orders.parquet\" " +
        "ORDER BY o_orderkey ASC) %s",
      projectOrderCase(LAST_ORDER_KEY, RANGE_PER_CASE, NUM_CASE_CONDITIONS),
      "SELECT count(distinct reflect) as uref FROM t1");
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("uref")
      .baselineValues((long) NUM_CASE_CONDITIONS + 1)
      .go();
  }

  private static final int LAST_ORDER_KEY1 = 60000;
  private static final int RANGE_PER_CASE1 = 20;
  private static final int NUM_CASE_CONDITIONS1 = 50;
  @Test
  public void testLargeCaseWithCaseInThen() throws Exception {
    final String sql = String.format("with t1 as (select %s from cp.\"tpch/orders.parquet\" " +
        "ORDER BY o_orderkey DESC) %s",
      projectFunctionArgCase(LAST_ORDER_KEY1, RANGE_PER_CASE1, NUM_CASE_CONDITIONS1, false),
      "SELECT count(distinct arg) as uref FROM t1 where arg like '%XY%'");
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("uref")
      .baselineValues(32L)
      .go();
  }

  @Test
  public void testLargeCaseWithCaseInWhenThen() throws Exception {
    final String sql = String.format("with t1 as (select %s from cp.\"tpch/orders.parquet\" " +
        "ORDER BY o_orderkey DESC) %s",
      projectFunctionArgCase(LAST_ORDER_KEY1, RANGE_PER_CASE1, NUM_CASE_CONDITIONS1, true),
      "SELECT count(distinct arg) as uref FROM t1 where arg like '%XY%'");
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("uref")
      .baselineValues(35L)
      .go();
  }

  @Test
  public void testNestedLargeCaseStatement() throws Exception {
    final String sql = String.format("select %s from cp.\"tpch/nation.parquet\" limit 10",
      projectLargeCase(50, 4));
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("nation")
      .baselineValues("ALGERIA_ABC")
      .baselineValues("argentina_americas")
      .baselineValues("brazil_americas")
      .baselineValues("canada_americas")
      .baselineValues("egypt_me")
      .baselineValues("france_europe")
      .baselineValues("germany_europe")
      .baselineValues("india_asia")
      .baselineValues("indonesia_asia")
      .baselineValues("ethiopia_unknown")
      .go();
  }

  @Test
  public void testMultiProjectCase() throws Exception {
    final String sql = "select n_name as name, " +
      "case when n_regionkey = 0 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 0" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 1" +
      "   else 9 end " +
      "when n_regionkey = 1 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 10" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 11" +
      "   else 99 end " +
      "when n_regionkey = 2 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 100" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 101" +
      "   else 999 end " +
      "else 0 end as _useless1," +
      "case when n_nationkey < 100 then " +
      "   case when n_regionkey >= 0 and n_regionkey < 3 then 1000" +
      "   when n_regionkey >= 3 and n_regionkey < 20 then 1111" +
      "   else 9999 end " +
      "else 0 end as _useless2 " +
      "from cp.\"tpch/nation.parquet\" limit 3";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "_useless1", "_useless2")
      .baselineValues("ALGERIA", 0, 1000)
      .baselineValues("ARGENTINA", 10, 1000)
      .baselineValues("BRAZIL", 10, 1000)
      .go();
  }

  @Test
  public void testComplexCase() throws Exception {
    final String sql = "select case when c.ingredients[2].name = 'Cheese' then 'Scrumptious' " +
      "                             when c.ingredients[0].name = 'Tomatoes' then 'Sweetened' " +
      "                             else 'Unknown' end as _flavor from cp.\"parquet/complex.parquet\" c";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_flavor")
      .baselineValues("Scrumptious")
      .baselineValues("Sweetened")
      .go();
  }

  @Test
  public void testComplexReturn() throws Exception {
    final String sql = "select case when c.ingredients[0].name = 'Beef' then c.ingredients " +
      "                             else NULL end as _new_recipe " +
      "  from cp.\"parquet/complex.parquet\" c";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_new_recipe")
      .baselineValues((Object) null)
      .baselineValues(listOf(mapOf("name", "Beef"),
        mapOf("name", "Lettuce"), mapOf("name", "Cheese")))
      .go();
  }

  @Test
  public void testFilterCase() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE \n" +
      "                     WHEN n_regionkey = 0 THEN 'AFRICA' \n" +
      "                     WHEN n_regionkey = 1 THEN 'AMERICAS' \n" +
      "                     WHEN n_regionkey = 2 THEN 'ASIA' \n" +
      "                     ELSE 'OTHER123456789012' \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  CASE WHEN length(region) > 10 THEN region LIKE '%NOTTHERE%' ELSE region LIKE '%A' END";
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(10L)
      .go();
  }

  private Object projectOrderCase(int start, int range, int conditions) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    int begin = start;
    int end = start - range;
    for (int i = 0; i < conditions; i++) {
      sb.append("WHEN o_orderkey <= ").append(begin).append(" AND o_orderkey > ")
        .append(end).append(" THEN ").append(begin)
        .append(System.lineSeparator());
      begin -= range;
      end = begin - range;
    }
    sb.append("ELSE 99999 END as reflect").append(System.lineSeparator());
    return sb.toString();
  }

  private Object projectOrderCaseRepeatingCondition(int conditions, int startIdx) {
    final int[] outputs = {0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000};
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    int nextOrder = 1000 + startIdx;
    int nextCustomer = 100 + startIdx;
    double beginTotalPrice = 10000.00 + startIdx;
    double nextTotalPrice = beginTotalPrice;
    int nextOutput = outputs[startIdx % outputs.length];
    for (int i = 0; i < conditions; i++) {
      sb.append("WHEN o_orderkey <= ").append(nextOrder)
        .append(" AND o_custkey <= ").append(nextCustomer)
        .append(" AND o_totalprice < ").append(nextTotalPrice)
        .append(" THEN ").append(nextOutput)
        .append(System.lineSeparator());
      nextTotalPrice += 100.0;
      if (i % 2 == 0) {
        nextOrder += 1000;
      }
      if (i % 5 == 0) {
        nextCustomer += 100;
        nextOutput = outputs[startIdx++ % outputs.length];
      }
      if (i % 200 == 0) {
        nextTotalPrice = beginTotalPrice;
      }
    }
    sb.append("ELSE 0 END").append(System.lineSeparator());
    return sb.toString();
  }

  private Object projectFunctionArgCase(int start, int range, int conditions,
                                        boolean when) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    int begin = start;
    int end = start - range;
    for (int i = 0; i < conditions; i++) {
      final String whenClause = (when) ? caseInWhen(begin, end) : Integer.toString(end);
      sb.append("WHEN o_orderkey <= ").append(begin).append(" AND o_orderkey > ")
        .append(whenClause).append(" THEN ").append(" concat(o_comment, ")
        .append(caseInThenFunction(begin, end + 10)).append(")")
        .append(System.lineSeparator());
      begin -= range;
      end = begin - range;
    }
    sb.append("ELSE 'ELS' END as arg").append(System.lineSeparator());
    return sb.toString();
  }

  private String caseInWhen(int begin, int end) {
    final char[] status = {'P', 'F', 'O', 'E', 'Q', 'P', 'O'};
    StringBuilder sb = new StringBuilder("case o_orderstatus ");
    for (int i = 0; i < begin - end - 10; i++) {
      sb.append("when '").append(status[i % status.length]).append("' then ").append(end - i * 100).append(" ");
    }
    sb.append("else ").append(begin).append(" end");
    return sb.toString();
  }

  private String caseInThenFunction(int begin, int end) {
    final String[] retStr = {"AB", "BA", "YX", "XY"};
    StringBuilder sb = new StringBuilder("case ");
    final int inc = (begin - end) * 10;
    int start = 0;
    for (int i = 0; i < begin - end; i++) {
      sb.append("when o_custkey >= ").append(start).append(" and o_custkey < ").append(start + inc)
        .append(" then '").append(retStr[i%4]).append(i).append("' ");
      start += inc;
    }
    sb.append("else 'CD").append(end).append("' end");
    return sb.toString();
  }

  private String projectLargeCase(int level1Size, int level2Size) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE n_nationkey").append(System.lineSeparator());
    for (int i = level1Size; i > 0; i--) {
      sb.append("WHEN ").append(i).append(" THEN ").append(getNextClause(level2Size, "_XY" + i))
        .append(System.lineSeparator());
    }
    sb.append("ELSE concat(n_name, '_ABC') END as nation").append(System.lineSeparator());
    return sb.toString();
  }

  private String getNextClause(int level2Size, String extension) {
    final String[] regions = {"_UNKNOWN", "_AMERICAS", "_ASIA", "_EUROPE", "_ME"};
    if (level2Size <= 1) {
      return "lower(concat(n_name, '" + extension + "'))";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("CASE n_regionkey").append(System.lineSeparator());
      for (int i = level2Size; i > 0; i--) {
        final int region_idx = (i >= regions.length) ? 0 : i;
        sb.append("WHEN ").append(i).append(" THEN ").append((getNextClause(1, regions[region_idx])))
          .append(System.lineSeparator());
      }
      sb.append("ELSE lower(concat(n_name, '_UNKNOWN')) END").append(System.lineSeparator());
      return sb.toString();
    }
  }
}
