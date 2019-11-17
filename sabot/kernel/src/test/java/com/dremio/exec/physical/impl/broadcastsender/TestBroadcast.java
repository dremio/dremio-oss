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
package com.dremio.exec.physical.impl.broadcastsender;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestBroadcast extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBroadcast.class);

  String broadcastQuery = "select * from "
      + "dfs.\"${WORKING_PATH}/src/test/resources/broadcast/sales\" s "
      + "INNER JOIN "
      + "dfs.\"${WORKING_PATH}/src/test/resources/broadcast/customer\" c "
      + "ON s.id = c.id";

  @Test
  public void plansWithBroadcast() throws Exception {
    //TODO: actually verify that this plan has a broadcast exchange in it once plan tools are enabled.
    setup();
    test("explain plan for " + broadcastQuery);
  }

  @Test
  public void broadcastExecuteWorks() throws Exception {
    setup();
    test(broadcastQuery);
  }

  @Test // DX-16800
  public void testRightJoinWithBroadcast() throws Exception {
    // test to ensure that broadcastExchange is not created on the right side if joinType is right
    final String query = "select * from dfs.\"${WORKING_PATH}/src/test/resources/broadcast/left\" le "
            + "left outer join dfs.\"${WORKING_PATH}/src/test/resources/broadcast/right\" ri "
            + "on (ri.join_col_a = le.join_col_a and ri.join_col_b = le.join_col_b) "
            + "where le.join_col_b='someVal' and le.join_col_a='Some Value'";
    setup();
    testPlanMatchingPatterns(query, new String[] {"joinType=\\[right\\]"},
            "BroadcastExchange.*left_col_a");
  }


  private void setup() throws Exception{
    testNoResult("alter session set \"planner.slice_target\" = 1");
    testNoResult("alter session set \"planner.enable_broadcast_join\" = true");
    testNoResult("alter session set \"planner.filter.transitive_pushdown\" = false");
  }
}
