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
package com.dremio.exec.physical.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;

/**
 * Test RoundRobin
 */
public class TestRoundRobin extends PlanTestBase {

  private final String useRoundRobinSort = "alter session set \"planner.enable_sort_round_robin\" = true";
  private final String useHashSort = "alter session set \"planner.enable_sort_round_robin\" = false";

  private final String query = "select state, review_count, business_id from dfs.\"${WORKING_PATH}/src/test/resources/yelp_business.json\" order by state, review_count, business_id limit 1";

  @Before
  public void setup() throws Exception{
    testNoResult("alter session set \"planner.slice_target\" = 1");
  }

  @After
  public void done() throws Exception{
    testNoResult("alter session set \"planner.slice_target\" = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }

  @Test
  public void testSmallExamplePlan() throws Exception {
    testPlanMatchingPatterns(query,
      new String[] {
        "SingleMergeExchange\\(sort0=\\[0\\], sort1=\\[1\\], sort2=\\[2\\]\\)",
        "TopN\\(limit=\\[1\\]\\)",
        "RoundRobinExchange" },
      null);
  }

  @Test
  public void testSmallExamplePlanWithoutRoundRobin() throws Exception {
    // Should revert back to old behavior with hash to random exchange
    try {
      testNoResult(useHashSort);
      testPlanMatchingPatterns(query,
        new String[] {
          "SingleMergeExchange\\(sort0=\\[0\\], sort1=\\[1\\], sort2=\\[2\\]\\)",
          "TopN\\(limit=\\[1\\]\\)",
          "HashToRandomExchange\\(dist0=\\[\\[\\$0\\]\\], dist1=\\[\\[\\$1\\]\\], dist2=\\[\\[\\$2\\]\\]\\)"
        },
        null);
    } finally {
      testNoResult(useRoundRobinSort);
    }
  }

  @Test
  public void testSmallExampleResult() throws Exception {
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("state", "review_count", "business_id")
      .baselineValues("IN", 22L, "Lp32rSc3OATt1P0Upa4Jxg")
      .go();
  }

  @Test
  public void testRoundRobinBiggerData() throws Exception {
    final String pmulti = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    final String queryPmulti = "SELECT * FROM dfs_test.\"" + pmulti + "\" ORDER BY o_orderkey, o_custkey desc, o_orderdate";

    try {
      testNoResult(useHashSort);
      testPlanMatchingPatterns(queryPmulti,
        new String[] {
          "SingleMergeExchange\\(sort0=\\[0\\], sort1=\\[1 DESC\\], sort2=\\[4\\]\\)",
          "Sort\\(sort0=\\[\\$0\\], sort1=\\[\\$1\\], sort2=\\[\\$4\\], dir0=\\[ASC\\], dir1=\\[DESC\\], dir2=\\[ASC\\]\\)",
          "HashToRandomExchange\\(dist0=\\[\\[\\$0\\]\\], dist1=\\[\\[\\$1\\]\\], dist2=\\[\\[\\$4\\]\\]\\)"},
        null );
    } finally {
      testNoResult(useRoundRobinSort);
    }

    testPlanMatchingPatterns(queryPmulti,
      new String[] {
        "SingleMergeExchange\\(sort0=\\[0\\], sort1=\\[1 DESC\\], sort2=\\[4\\]\\)",
        "Sort\\(sort0=\\[\\$0\\], sort1=\\[\\$1\\], sort2=\\[\\$4\\], dir0=\\[ASC\\], dir1=\\[DESC\\], dir2=\\[ASC\\]\\)",
        "RoundRobinExchange"},
      null );

    // Compare results for the two queries with and without round robin distribution for sorting.
    testBuilder()
      .optionSettingQueriesForTestQuery(useRoundRobinSort)
      .optionSettingQueriesForBaseline(useHashSort)
      .ordered()
      .sqlQuery(queryPmulti)
      .sqlBaselineQuery(queryPmulti)
      .go();
  }
}
