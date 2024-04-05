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
package com.dremio.exec.physical.impl.join;

import com.dremio.PlanTestBase;
import org.junit.Test;

public class TestJoinPlanning extends PlanTestBase {
  @Test
  public void testJoinProjectPushdown() throws Exception {
    testNoResult("use cp.tpch");
    String sql =
        "select n_name, sum(l_extendedprice) from \"lineitem.parquet\", \"orders.parquet\", \"customer.parquet\", \"nation.parquet\"\n"
            + "where l_orderkey = o_orderkey\n"
            + "AND o_custkey = c_custkey\n"
            + "AND c_nationkey = n_nationkey\n"
            + "group by n_name";
    // without join project pushdown in join optimization phase, we wouldn't have the intermediate
    // Projects to drop unneeded
    // join keys
    testPlanMatchingPatterns(
        sql, new String[] {"(?s)HashJoin.*Project.*Project.*HashJoin.*Project.*Project.*HashJoin"});
  }
}
