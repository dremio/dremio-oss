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

import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestCaseLargeQueriesMixed extends TestAbstractCaseLargeQueries {

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(80, TimeUnit.SECONDS);

  @BeforeClass
  public static void setup() throws Exception {
    testNoResult(
        "ALTER SESSION SET \"%s\" = 4", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult(
        "ALTER SESSION SET \"%s\" = true", ExecConstants.ENABLE_VERBOSE_ERRORS.getOptionName());
    testNoResult(
        "ALTER SESSION SET \"%s\" = 500",
        ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
    testNoResult(
        "ALTER SESSION SET \"%s\" = false", ExecConstants.GANDIVA_OPTIMIZE.getOptionName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testNoResult(
        "ALTER SESSION RESET \"%s\"", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult("ALTER SESSION RESET \"%s\"", ExecConstants.ENABLE_VERBOSE_ERRORS.getOptionName());
    testNoResult(
        "ALTER SESSION RESET \"%s\"",
        ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
    testNoResult("ALTER SESSION RESET \"%s\"", ExecConstants.GANDIVA_OPTIMIZE.getOptionName());
  }

  @Test
  public void testNestedLargeCaseStatement() throws Exception {
    final String sql =
        String.format(
            "select %s from cp.\"tpch/nation.parquet\" limit 10", projectLargeCase(25, 4));
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
  public void testLargeCaseWithPotentialSplits() throws Exception {
    final String sql =
        String.format(
            "with t1 as (select %s from cp.\"tpch/orders.parquet\" "
                + "ORDER BY o_orderkey DESC) %s",
            projectLargeOrderCase(4, 12),
            "SELECT count(distinct c_order) as final FROM t1 where c_order not like '%_X%'");
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("final").baselineValues(1000L).go();
  }

  private static final int NUM_NEXT_CASE_CONDITIONS = 200;
  private static final int NUM_FIRST_CASE_CONDITIONS = 4;

  @Test
  public void testLargeNestedCaseWithLike() throws Exception {
    StringBuilder sb = new StringBuilder("CASE ");
    for (int i = 0; i < NUM_FIRST_CASE_CONDITIONS; i++) {
      final char valToCheck = (char) ('a' + i);
      sb.append("WHEN o_comment LIKE '%")
          .append(valToCheck)
          .append("%'")
          .append(" THEN ")
          .append(System.lineSeparator())
          .append(projectOrderCaseRepeatingCondition(NUM_NEXT_CASE_CONDITIONS, i))
          .append(System.lineSeparator());
    }
    sb.append("ELSE 999 END as _final").append(System.lineSeparator());
    final String sql =
        String.format(
            "select count(*) as _total from "
                + "(select %s, %s from cp.\"tpch/orders.parquet\" ) where _final != 0",
            sb, projectOrderCase(650000, 10, 10));
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("_total").baselineValues(1059L).go();
  }

  private static final int LAST_ORDER_KEY = 60000;
  private static final int RANGE_PER_CASE = 100;
  private static final int NUM_CASE_CONDITIONS = 450;

  @Test
  public void testLargeCaseReverse() throws Exception {
    final String sql =
        String.format(
            "with t1 as (select %s from cp.\"tpch/orders.parquet\" "
                + "ORDER BY o_orderkey ASC) %s",
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
    final String sql =
        String.format(
            "with t1 as (select %s from cp.\"tpch/orders.parquet\" "
                + "ORDER BY o_orderkey DESC) %s",
            projectFunctionArgCase(LAST_ORDER_KEY1, RANGE_PER_CASE1, NUM_CASE_CONDITIONS1, false),
            "SELECT count(distinct arg) as uref FROM t1 where arg like '%XY%'");
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("uref").baselineValues(32L).go();
  }

  @Test
  public void testLargeCaseWithCaseInWhenThen() throws Exception {
    final String sql =
        String.format(
            "with t1 as (select %s from cp.\"tpch/orders.parquet\" "
                + "ORDER BY o_orderkey DESC) %s",
            projectFunctionArgCase(LAST_ORDER_KEY1, RANGE_PER_CASE1, NUM_CASE_CONDITIONS1, true),
            "SELECT count(distinct arg) as uref FROM t1 where arg like '%XY%'");
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("uref").baselineValues(35L).go();
  }

  private Object projectOrderCase(int start, int range, int conditions) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    int begin = start;
    int end = start - range;
    for (int i = 0; i < conditions; i++) {
      sb.append("WHEN o_orderkey <= ")
          .append(begin)
          .append(" AND o_orderkey > ")
          .append(end)
          .append(" THEN ")
          .append(begin)
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
      sb.append("WHEN o_orderkey <= ")
          .append(nextOrder)
          .append(" AND o_custkey <= ")
          .append(nextCustomer)
          .append(" AND o_totalprice < ")
          .append(nextTotalPrice)
          .append(" THEN ")
          .append(nextOutput)
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

  private Object projectFunctionArgCase(int start, int range, int conditions, boolean when) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    int begin = start;
    int end = start - range;
    for (int i = 0; i < conditions; i++) {
      final String whenClause = (when) ? caseInWhen(begin, end) : Integer.toString(end);
      sb.append("WHEN o_orderkey <= ")
          .append(begin)
          .append(" AND o_orderkey > ")
          .append(whenClause)
          .append(" THEN ")
          .append(" concat(o_comment, ")
          .append(caseInThenFunction(begin, end + 10))
          .append(")")
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
      sb.append("when '")
          .append(status[i % status.length])
          .append("' then ")
          .append(end - i * 100)
          .append(" ");
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
      sb.append("when o_custkey >= ")
          .append(start)
          .append(" and o_custkey < ")
          .append(start + inc)
          .append(" then '")
          .append(retStr[i % 4])
          .append(i)
          .append("' ");
      start += inc;
    }
    sb.append("else 'CD").append(end).append("' end");
    return sb.toString();
  }

  private String projectLargeOrderCase(int level1Size, int level2Size) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE ").append(System.lineSeparator());
    for (int i = level1Size; i > 0; i--) {
      final int limit = i * 100;
      final String whenExpr =
          (i % 3 == 0) ? "sqrt(o_totalprice) >= " : "castINT(o_totalprice / 100.0) >= ";
      sb.append("WHEN ")
          .append(whenExpr)
          .append(limit)
          .append(" THEN ")
          .append(getNextOrderClause(level2Size, i))
          .append(System.lineSeparator());
    }
    sb.append("ELSE concat(o_orderstatus, '_X') END as c_order").append(System.lineSeparator());
    return sb.toString();
  }

  private String getNextOrderClause(int level2Size, int level1Idx) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE (castINT(sqrt(o_totalprice))) ").append(System.lineSeparator());
    for (int i = level2Size; i > 0; i--) {
      final String whenExpr =
          (i % 10 == 0)
              ? "(castINT(sqrt(o_totalprice))) > " + i
              : (i % 5 == 0)
                  ? "regexp_matches(o_comment, '.*(quick|haggle|foxes).*') "
                      + "OR regexp_matches(LOWER(o_comment), '.*(deposit).*') "
                      + "OR regexp_matches(LOWER(o_comment), '.*(ideas).*')"
                  : (i % 2 == 0)
                      ? "o_orderstatus IN ('1-URGENT', '2-HIGH')"
                      : "o_totalprice / 1000 > " + i;
      sb.append("WHEN ")
          .append(whenExpr)
          .append(" THEN ")
          .append("lower(concat(o_clerk,'AA")
          .append(level1Idx * i)
          .append("'))")
          .append(System.lineSeparator());
    }
    sb.append("ELSE lower(concat(o_clerk, '_UNKNOWN')) END").append(System.lineSeparator());
    return sb.toString();
  }
}
