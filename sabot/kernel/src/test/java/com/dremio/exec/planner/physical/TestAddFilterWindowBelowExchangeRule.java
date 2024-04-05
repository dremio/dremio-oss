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
package com.dremio.exec.planner.physical;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAddFilterWindowBelowExchangeRule extends PlanTestBase {

  @Before
  public void setup() throws Exception {
    test("alter session set planner.slice_target=1");
    test("alter session set planner.enable_sort_round_robin=true");
  }

  @After
  public void reset() throws Exception {
    test("alter session set planner.slice_target=" + ExecConstants.SLICE_TARGET_DEFAULT);
    test(
        "alter session set planner.enable_sort_round_robin="
            + PlannerSettings.ENABLE_SORT_ROUND_ROBIN.getDefault().getBoolVal());
  }

  @Test
  public void testInvalidFilter() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, Dense_rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank "
            + "FROM cp.\"customer.json\") WHERE c_rank > 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*EasyScan"
        });
  }

  @Test
  public void testInvalidWindowFunction() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, PERCENT_RANK() OVER (PARTITION BY total_children ORDER by postal_code) AS p_rank "
            + "FROM cp.\"customer.json\") WHERE p_rank < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*EasyScan"
        });
  }

  @Test
  public void testInvalidPartition() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, Dense_rank() OVER (ORDER by postal_code) AS c_rank "
            + "FROM cp.\"customer.json\") WHERE c_rank < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*SingleMergeExchange.*Sort.*RoundRobinExchange.*EasyScan"
        });
  }

  @Test
  public void testInvalidOption() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank "
            + "FROM cp.\"customer.json\") WHERE c_rank <= 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*EasyScan"
        });
  }

  @Test
  public void testDenseRank() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, Dense_rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank "
            + "FROM cp.\"customer.json\") WHERE c_rank < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*Project.*SelectionVectorRemover.*Filter\\(condition=\\[<\\(\\$2, 2\\).*Window.*Sort.*RoundRobinExchange"
        });
  }

  @Test
  public void testRank() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank "
            + "FROM cp.\"customer.json\") WHERE c_rank <= 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*Project.*SelectionVectorRemover.*Filter\\(condition=\\[<\\=\\(\\$2, 2\\).*Window.*Sort.*RoundRobinExchange"
        });
  }

  @Test
  public void testRowNumber() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, row_number() OVER (PARTITION BY total_children ORDER by postal_code) AS c_row_number "
            + "FROM cp.\"customer.json\") WHERE c_row_number < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*Project.*SelectionVectorRemover.*Filter\\(condition=\\[<\\(\\$2, 2\\).*Window.*Sort.*RoundRobinExchange"
        });
  }

  @Test
  public void testCount1() throws Exception {
    test("alter session set planner.slice_target=1");
    test("alter session set planner.enable_sort_round_robin=true");
    String sql =
        "SELECT * FROM (SELECT total_children, count(*) OVER (PARTITION BY total_children ORDER by postal_code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c_row_number "
            + "FROM cp.\"customer.json\") WHERE c_row_number < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*Project.*SelectionVectorRemover.*Filter\\(condition=\\[<\\(\\$2, 2\\).*Window.*Sort.*RoundRobinExchange"
        });
  }

  @Test
  public void testCount2() throws Exception {
    String sql =
        "SELECT * FROM (SELECT total_children, count(*) OVER (PARTITION BY total_children ORDER by postal_code RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c_row_number "
            + "FROM cp.\"customer.json\") WHERE c_row_number < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*Project.*SelectionVectorRemover.*Filter\\(condition=\\[<\\(\\$2, 2\\).*Window.*Sort.*RoundRobinExchange"
        });
  }

  @Test
  public void testCountInvalid() throws Exception {
    test("alter session set planner.slice_target=1");
    test("alter session set planner.enable_sort_round_robin=true");
    String sql =
        "SELECT * FROM (SELECT total_children, count(*) OVER (PARTITION BY total_children ORDER by postal_code RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS c_row_number "
            + "FROM cp.\"customer.json\") WHERE c_row_number < 2";
    testPlanMatchingPatterns(
        sql,
        new String[] {
          "(?s)Filter.*Window.*Sort.*Project.*HashToRandomExchange.*Project.*EasyScan"
        });
  }
}
