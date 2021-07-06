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

import org.junit.Before;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestHashJoinWithRuntimeFilter extends PlanTestBase {

  private static final String NATION ="dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/nation\"";
  private static final String REGION ="dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/region\"";

  @Test
  public void testHashJoin() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + "group by nations.N_NAME", NATION, REGION);
    String excludedColNames1 =  "runtimeFilter";
    testPlanMatchingPatterns(sql, new String[]{excludedColNames1});
  }

  @Test
  public void testLeftHashJoin() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "LEFT JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + "group by nations.N_NAME", NATION, REGION);
    String excludedColNames1 =  "runtimeFilterInfo";
    String excludedColNames2 =  "runtimeFilter";
    testPlanMatchingPatterns(sql, null, excludedColNames1, excludedColNames2);
  }

  @Test
  public void testFullOuterHashJoin() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "FULL OUTER JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + "group by nations.N_NAME", NATION, REGION);
    String excludedColNames1 =  "runtimeFilterInfo";
    String excludedColNames2 =  "runtimeFilter";
    testPlanMatchingPatterns(sql, null, excludedColNames1, excludedColNames2);
  }


  @Test
  public void testHashJoinWithFuncJoinCondition() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on (nations.N_REGIONKEY +1) = regions.R_REGIONKEY \n"
      + "group by nations.N_NAME", NATION, REGION);
    String excludedColNames1 =  "runtimeFilterInfo";
    String excludedColNames2 =  "runtimeFilter";
    testPlanMatchingPatterns(sql, null, excludedColNames1, excludedColNames2);
  }

  @Test
  public void testHashJoinWithCast() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on CAST (nations.N_REGIONKEY as INT) = regions.R_REGIONKEY\n"
      + "group by nations.N_NAME", NATION, REGION);
    String excludedColNames1 =  "runtimeFilterInfo";
    String excludedColNames2 =  "runtimeFilter";
    testPlanMatchingPatterns(sql, null, excludedColNames1, excludedColNames2);
  }

  @Test
  public void testWithAlias() throws Exception {
    String sql = String.format("select * from\n" +
      "  (select n_regionkey as key, convert_from(n_comment, 'utf8') as comment from %s)\n" +
      "where key = (select max(r_regionkey) from %s)", NATION, REGION);
    testPlanMatchingPatterns(sql, new String[] {"runtimeFilter.*N_REGIONKEY"});
  }

  @Test
  public void testWithFilter() throws Exception {
    String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
      + "%s nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + " where nations.n_NATIONKEY > 1 \n"
      + "group by nations.N_NAME", NATION, REGION);
    String include =  "runtimeFilter";
    testPlanMatchingPatterns(sql, new String[]{include});
  }

  @Test
  public void testWithUnion() throws Exception {
    String unionCTE = String.format("select * from %s union all select * from %s", NATION, NATION);
    String sql = String.format("with nations as (%s)\n"
      + "SELECT nations.N_NAME, count(*) FROM\n"
      + "nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + " where nations.n_NATIONKEY > 1 \n"
      + "group by nations.N_NAME", unionCTE, REGION);
    String include =  "runtimeFilter.*03.*04";
    testPlanMatchingPatterns(sql, new String[]{include});
  }

  @Test
  public void testWithUnionDifferentTables() throws Exception {
    String unionCTE = String.format("select n_name, n_nationkey, n_regionkey from %s union all select r_name, r_regionkey, r_regionkey as n_regionkey from %s", NATION, REGION);
    String sql = String.format("with nations as (%s)\n"
      + "SELECT nations.N_NAME, count(*) FROM\n"
      + "nations \n"
      + "JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + " where nations.n_NATIONKEY > 1 \n"
      + "group by nations.N_NAME", unionCTE, REGION);
    String include =  "runtimeFilter.*03.*04";
    testPlanMatchingPatterns(sql, new String[]{include});
  }
  @Test
  public void testWithAggregate() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String aggCTE = String.format("select distinct n_nationkey, n_regionkey, n_name from %s", NATION);
      String sql = String.format("with nations as (%s)\n"
        + "SELECT nations.N_NAME, count(*) FROM\n"
        + "nations \n"
        + "JOIN\n"
        + "%s regions \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + "group by nations.N_NAME", aggCTE, REGION);
      String include = "runtimeFilter.*03\\-04";
      testPlanMatchingPatterns(sql, new String[]{include});
    }
  }

  @Test
  public void testWithNLJ() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String nljCTE = String.format("select r_name, n_name, n_regionkey from %s left join %s on n_regionkey = r_regionkey and n_name > r_name ", NATION, REGION);
      String sql = String.format("with nations as (%s)\n"
        + "SELECT nations.N_NAME, count(*) FROM\n"
        + "nations \n"
        + "JOIN\n"
        + "%s regions \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + "group by nations.N_NAME", nljCTE, REGION);
      String include = "runtimeFilter.*02\\-09";
      testPlanMatchingPatterns(sql, new String[]{include});
    }
  }

  @Test
  public void test3wayJoinWithLeftJoin() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
        + "%s nations \n"
        + "LEFT JOIN\n"
        + "%s regions \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + " JOIN\n"
        + "%s nations2\n"
        + " on nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
        + "group by nations.N_NAME", NATION, REGION, NATION);
      String includedString = "runtimeFilter";
      testPlanMatchingPatterns(sql, new String[]{includedString});
    }
  }

  @Test
  public void test3wayJoinWithRightJoinNegative() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
        + "%s nations \n"
        + "RIGHT JOIN\n"
        + "%s regions \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + " JOIN\n"
        + "%s nations2\n"
        + " on nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
        + "group by nations.N_NAME", NATION, REGION, NATION);
      String excluded = "runtimeFilter";
      testPlanMatchingPatterns(sql, null, excluded);
    }
  }

  @Test
  public void test3wayJoinWithRightJoin() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
        + "%s regions \n"
        + "RIGHT JOIN\n"
        + "%s nations \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + " JOIN\n"
        + "%s nations2\n"
        + " on nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
        + "group by nations.N_NAME", REGION, NATION, NATION);
      String includedString = "runtimeFilter";
      testPlanMatchingPatterns(sql, new String[]{includedString});
    }
  }

  @Test
  public void test3wayJoinWithStreamingAgg() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql = String.format("SELECT nations.N_NAME, r_regionkey, count(*) FROM\n"
        + "(select sum(r_regionkey) r_regionkey from %s) regions \n"
        + "RIGHT JOIN\n"
        + "%s nations \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + " JOIN\n"
        + "%s nations2\n"
        + " on nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
        + "group by nations.N_NAME, r_regionkey", REGION, NATION, NATION);
      String includedString = "runtimeFilter";
      testPlanMatchingPatterns(sql, new String[]{includedString});
    }
  }

  @Test
  public void test3wayJoinWithLeftJoinNegative() throws Exception {
    try (AutoCloseable with1 = withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false); AutoCloseable with2 = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql = String.format("SELECT nations.N_NAME, count(*) FROM\n"
        + "%s regions \n"
        + "LEFT JOIN\n"
        + "%s nations \n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
        + " JOIN\n"
        + "%s nations2\n"
        + " on nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
        + "group by nations.N_NAME", REGION, NATION, NATION);
      String includedString = "runtimeFilter";
      testPlanMatchingPatterns(sql, null, includedString);
    }
  }

  @Before
  public void setup() throws Exception{
    testNoResult("alter session set \"planner.slice_target\" = 1");
    testNoResult("alter session set \"planner.enable_broadcast_join\" = true");
    testNoResult("alter session set \"planner.filter.runtime_filter\" = true");
  }
}
