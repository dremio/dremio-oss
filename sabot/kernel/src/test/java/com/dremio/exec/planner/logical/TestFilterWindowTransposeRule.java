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
package com.dremio.exec.planner.logical;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestFilterWindowTransposeRule extends PlanTestBase {

  @Test
  public void testFilterPartialPushdown() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children, Dense_rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank " +
      "FROM cp.\"customer.json\") WHERE total_children > 3 AND c_rank > 2";

    testPlanMatchingPatterns(
      sql,
      new String[] { "Filter\\(condition=\\[>\\(\\$2, 2\\)\\]\\).*\n.*Window.*\n.*\n.*Filter\\(condition=\\[>\\(\\$1, 3\\)\\]\\)" },
      new String[] {});
  }

  @Test
  public void testFilterAllPushdown() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children, Dense_rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank " +
      "FROM cp.\"customer.json\") WHERE total_children > 3 AND total_children < 5";

    testPlanMatchingPatterns(
      sql,
      new String[] { "Window.*\n.*\n.*Filter\\(condition=\\[AND\\(>\\(\\$1, 3\\), <\\(\\$1, 5\\)\\)\\]\\)" },
      new String[] {});
  }

  @Test
  public void testFilterNonePushdown() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children, Dense_rank() OVER (PARTITION BY total_children ORDER by postal_code) AS c_rank " +
      "FROM cp.\"customer.json\") WHERE c_rank > 2";

    testPlanMatchingPatterns(
      sql,
      new String[] { "Filter\\(condition=\\[>\\(\\$2, 2\\)\\]\\).*\n.*Window" },
      new String[] {});
  }

}
