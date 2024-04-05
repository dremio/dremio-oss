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

import com.dremio.exec.planner.physical.PlannerSettings;
import java.util.regex.Pattern;
import org.junit.Test;

public class TestNonEquiJoinRewriteWithProject extends PlanTestBase {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestNonEquiJoinRewriteWithProject.class);

  @Test
  public void conditionsOnLeft() throws Exception {
    String sql =
        "select n_name, r_name from cp.\"tpch/region.parquet\" left join cp.\"tpch/nation.parquet\"\n"
            + "on n_regionkey = r_regionkey and r_name like '%ASIA%'";
    testPlanMatchingPatterns(sql, new String[] {"HashJoin"});
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("n_name", "r_name")
        .baselineValues("INDIA", "ASIA")
        .baselineValues("INDONESIA", "ASIA")
        .baselineValues("JAPAN", "ASIA")
        .baselineValues("CHINA", "ASIA")
        .baselineValues("VIETNAM", "ASIA")
        .baselineValues(null, "AFRICA")
        .baselineValues(null, "AMERICA")
        .baselineValues(null, "EUROPE")
        .baselineValues(null, "MIDDLE EAST")
        .go();
  }

  @Test
  public void conditionsOnRight() throws Exception {
    String sql =
        "select r_name from cp.\"tpch/region.parquet\" right join cp.\"tpch/nation.parquet\"\n"
            + "on n_regionkey = r_regionkey and n_name like '%ALGERIA%'";
    testPlanMatchingPatterns(sql, new String[] {"HashJoin"});
    TestBuilder builder =
        testBuilder().sqlQuery(sql).unOrdered().baselineColumns("r_name").baselineValues("AFRICA");
    for (int i = 0; i < 24; i++) {
      builder.baselineValues(null);
    }
    builder.go();
  }

  @Test
  public void negativeCase() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.EXTRA_CONDITIONS_HASHJOIN, true)) {
      String sql =
          "select r_name from cp.\"tpch/region.parquet\" right join cp.\"tpch/nation.parquet\"\n"
              + "on n_regionkey = r_regionkey and n_name <> r_name and r_name like '%ASIA%'";
      testPlanMatchingPatterns(
          sql,
          new String[] {
            Pattern.quote(
                "HashJoin(condition=[=($1, $2)], joinType=[left], extraCondition=[<>($0, $3)])")
          },
          "NestedLoopJoin");
    }
  }
}
