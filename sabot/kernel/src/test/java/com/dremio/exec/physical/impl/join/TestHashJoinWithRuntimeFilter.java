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

import static com.dremio.exec.ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET;

import com.dremio.PlanTestBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHashJoinWithRuntimeFilter extends PlanTestBase {

  private static final String NATION_ORIGINAL =
      "dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/nation\"";
  private static final String REGION_ORIGINAL =
      "dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/region\"";

  private static final String NATION = "dfs_test.NATION";
  private static final String REGION = "dfs_test.REGION";

  @BeforeClass
  public static void beforeClass() throws Exception {
    test("use dfs_test");
    test(
        String.format(
            "CREATE TABLE %s(N_NATIONKEY bigint, N_NAME VARBINARY, N_REGIONKEY bigint, N_COMMENT VARBINARY) partition by (N_REGIONKEY, N_NATIONKEY)",
            NATION));
    test(
        String.format(
            "CREATE TABLE %s(R_REGIONKEY bigint, R_NAME VARBINARY, R_COMMENT VARBINARY) partition by (R_REGIONKEY)",
            REGION));
    test(String.format("INSERT INTO %s select * from %s", NATION, NATION_ORIGINAL));
    test(String.format("INSERT INTO %s select * from %s", REGION, REGION_ORIGINAL));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    test(String.format("DROP TABLE %s", REGION));
    test(String.format("DROP TABLE %s", NATION));
  }

  @Before
  public void setup() throws Exception {
    testNoResult("alter session set \"planner.slice_target\" = 1");
    testNoResult("alter session set \"planner.enable_broadcast_join\" = true");
    testNoResult("alter session set \"planner.filter.runtime_filter\" = true");
  }

  @Test
  public void testHashJoin() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s nations \n"
                + "JOIN %s regions \n"
                + "  ON nations.N_REGIONKEY = regions.R_REGIONKEY",
            NATION, REGION);
    testPlanMatchingPatterns(sql, new String[] {"runtimeFilter"});
    test(sql);
  }

  @Test
  public void testHashJoinWithFuncJoinCondition() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s nations \n"
                + "JOIN %s regions \n"
                + "  ON (nations.N_REGIONKEY +1) = regions.R_REGIONKEY",
            NATION, REGION);
    // We are unable to do runtime filters a non-trivial references
    testPlanMatchingPatterns(sql, null, "runtimeFilter");
    test(sql);
  }

  @Test
  public void testHashJoinWithCast() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s nations \n"
                + "JOIN %s regions \n"
                + "  ON CAST (nations.N_REGIONKEY as INT) = regions.R_REGIONKEY",
            NATION, REGION);
    testPlanMatchingPatterns(sql, null, "runtimeFilter");
    test(sql);
  }

  @Test
  public void testLeftHashJoinWithLeftBuildSide() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s nations \n"
                + "LEFT JOIN %s regions \n"
                + "  ON nations.N_REGIONKEY = regions.R_REGIONKEY",
            NATION, REGION);
    // Runtime filters should not occur
    testPlanMatchingPatterns(sql, null, "runtimeFilter");
    test(sql);
  }

  @Test
  public void testLeftHashJoinWithRightBuildSide() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s  regions\n"
                + "LEFT JOIN %s nations \n"
                + "  ON nations.N_REGIONKEY = regions.R_REGIONKEY",
            REGION, NATION);
    testPlanMatchingPatterns(sql, new String[] {"runtimeFilter"});
    test(sql);
  }

  @Test
  public void testFullOuterHashJoin() throws Exception {
    String sql =
        String.format(
            "SELECT *\n"
                + "FROM %s nations \n"
                + "FULL OUTER JOIN %s regions\n"
                + "  ON nations.N_REGIONKEY = regions.R_REGIONKEY",
            NATION, REGION);
    testPlanMatchingPatterns(sql, null, "runtimeFilter");
    test(sql);
  }

  @Test
  public void testNonPartitionJoinColumnsDisabled() throws Exception {
    try (AutoCloseable with1 =
        withSystemOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET, false)) {
      String sql =
          String.format(
              "SELECT nations.N_NAME, count(*) FROM\n"
                  + "%s nations \n"
                  + "JOIN\n"
                  + "%s regions \n"
                  + "  on nations.N_NAME = regions.R_NAME \n"
                  + "group by nations.N_NAME",
              NATION, REGION);
      String excluded = "runtimeFilter";
      testPlanMatchingPatterns(sql, null, new String[] {excluded});
    }
  }

  @Test
  public void testWithAlias() throws Exception {
    String sql =
        String.format(
            ""
                + "WITH aliased_nation AS (\n"
                + "  SELECT  n_regionkey AS key, convert_from(n_comment, 'utf8') AS comment\n"
                + "  FROM %s\n"
                + ")\n"
                + "SELECT *\n"
                + "FROM aliased_nation\n"
                + "WHERE key = (SELECT max(r_regionkey) FROM %s)",
            NATION, REGION);
    testPlanMatchingPatterns(sql, new String[] {"runtimeFilter.*N_REGIONKEY"});
    test(sql);
  }

  @Test
  public void testWithFilter() throws Exception {
    String sql =
        String.format(
            ""
                + "SELECT *\n"
                + "FROM %s nations \n"
                + "JOIN %s regions ON nations.N_REGIONKEY = regions.R_REGIONKEY \n"
                + "WHERE nations.n_NATIONKEY > 1",
            NATION, REGION);
    testPlanMatchingPatterns(sql, new String[] {"runtimeFilter"});
    test(sql);
  }

  @Test
  public void testWithUnion() throws Exception {
    String sql =
        String.format(
            ""
                + "WITH\n"
                + "nations AS (SELECT * FROM %s),\n"
                + "regions AS (SELECT * FROM %s),\n"
                + "nations_union AS (SELECT * FROM nations UNION ALL SELECT * FROM nations)\n"
                + "SELECT *\n"
                + "FROM nations_union\n"
                + "JOIN regions ON nations_union.N_REGIONKEY = regions.R_REGIONKEY \n"
                + "WHERE nations_union.n_NATIONKEY > 1 ",
            NATION, REGION);
    String include = "runtimeFilter.*N_REGIONKEY.*N_REGIONKEY";
    testPlanMatchingPatterns(sql, new String[] {include});
    test(sql);
  }

  @Test
  public void testWithUnionDifferentTables() throws Exception {
    String sql =
        String.format(
            ""
                + "WITH\n"
                + "nations AS (SELECT * FROM %s),\n"
                + "regions AS (SELECT * FROM %s),\n"
                + "nations_region_union AS (\n"
                + "  SELECT n_name, n_nationkey, n_regionkey FROM nations\n"
                + "  UNION ALL SELECT r_name, r_regionkey, r_regionkey as n_regionkey FROM regions)\n"
                + "SELECT *\n"
                + "FROM nations_region_union \n"
                + "JOIN regions ON nations_region_union.N_REGIONKEY = regions.R_REGIONKEY",
            NATION, REGION);
    String[] includes = {"runtimeFilter.*R_REGIONKEY.*N_REGIONKEY"};
    testPlanMatchingPatterns(sql, includes);
    test(sql);
  }

  @Test
  public void testWithAggregate() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s),\n"
                  + "distinct_nation AS (SELECT DISTINCT n_nationkey, n_regionkey, n_name FROM nations)\n"
                  + "SELECT distinct_nation.N_NAME, count(*)\n"
                  + "FROM distinct_nation\n"
                  + "JOIN regions\n"
                  + "  ON distinct_nation.N_REGIONKEY = regions.R_REGIONKEY \n"
                  + "GROUP BY distinct_nation.N_NAME",
              NATION, REGION);
      String[] includes = {
        "HashJoin.*runtimeFilterId=\\[[0-9a-f]+\\]",
        "TableFunction.*runtimeFilters=\\[p_[0-9a-f]+\\:R_REGIONKEY->N_REGIONKEY\\]"
      };
      testPlanMatchingPatterns(sql, includes);
      test(sql);
    }
  }

  @Test
  public void testWithNLJ() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s),\n"
                  + "nations_regions_nlj AS (\n"
                  + "  SELECT r_name, n_name, n_regionkey"
                  + "  FROM nations\n"
                  + "  LEFT JOIN regions ON n_regionkey = r_regionkey and n_name > r_name)"
                  + "SELECT *\n"
                  + "FROM nations_regions_nlj \n"
                  + "JOIN regions ON nations_regions_nlj.N_REGIONKEY = regions.R_REGIONKEY",
              NATION, REGION);
      String[] includes = {"runtimeFilters=.*:R_REGIONKEY->N_REGIONKEY"};
      testPlanMatchingPatterns(sql, includes);
      test(sql);
    }
  }

  @Test
  public void test3wayJoinWithLeftJoin() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s)\n"
                  + "SELECT *"
                  + "FROM nations \n"
                  + "LEFT JOIN regions\n"
                  + "  ON nations.N_REGIONKEY = regions.R_REGIONKEY \n"
                  + "JOIN nations AS nations2\n"
                  + " ON nations.N_NATIONKEY = nations2.N_NATIONKEY\n",
              NATION, REGION);
      String[] includedStrings = {"runtimeFilter"};
      testPlanMatchingPatterns(sql, includedStrings);
      test(sql);
    }
  }

  @Test
  public void test3wayJoinWithRightJoinNegative() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s)\n"
                  + "SELECT *"
                  + "FROM nations \n"
                  + "RIGHT JOIN regions ON nations.N_REGIONKEY = regions.R_REGIONKEY \n"
                  + "JOIN nations AS nations2 ON nations.N_NATIONKEY = nations2.N_NATIONKEY\n",
              NATION, REGION);
      String excluded = "runtimeFilter";
      testPlanMatchingPatterns(sql, null, excluded);
      test(sql);
    }
  }

  @Test
  public void test3wayJoinWithRightJoin() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s)\n"
                  + "SELECT *"
                  + "FROM regions \n"
                  + "RIGHT JOIN nations ON nations.N_REGIONKEY = regions.R_REGIONKEY \n"
                  + "JOIN nations nations2 ON nations.N_NATIONKEY = nations2.N_NATIONKEY",
              NATION, REGION);
      String[] includedStrings = {"runtimeFilter"};
      testPlanMatchingPatterns(sql, includedStrings);
      test(sql);
    }
  }

  @Test
  public void test3wayJoinWithStreamingAgg() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s),\n"
                  + "max_regions AS (SELECT MAX(r_regionkey) AS r_regionkey FROM regions)"
                  + "SELECT nations.N_NAME, r_regionkey, count(*)\n"
                  + "FROM max_regions\n"
                  + "RIGHT JOIN nations ON nations.N_REGIONKEY = max_regions.R_REGIONKEY \n"
                  + "JOIN nations nations2 ON nations.N_NATIONKEY = nations2.N_NATIONKEY\n"
                  + "group by nations.N_NAME, r_regionkey",
              NATION, REGION);
      String[] includedStrings = {"runtimeFilter"};
      testPlanMatchingPatterns(sql, includedStrings);
      test(sql);
    }
  }

  @Test
  public void test3wayJoinWithLeftJoinNegative() throws Exception {
    try (AutoCloseable withDisableJoinOpt =
            withOption(PlannerSettings.ENABLE_JOIN_OPTIMIZATION, false);
        AutoCloseable withHashJoinSwap = withOption(PlannerSettings.HASH_JOIN_SWAP, false)) {
      String sql =
          String.format(
              ""
                  + "WITH\n"
                  + "nations AS (SELECT * FROM %s),\n"
                  + "regions AS (SELECT * FROM %s)\n"
                  + "SELECT *\n"
                  + "FROM regions\n"
                  + "LEFT JOIN nations ON nations.N_REGIONKEY = regions.R_REGIONKEY \n"
                  + "JOIN nations nations2 ON nations.N_NATIONKEY = nations2.N_NATIONKEY",
              NATION, REGION);
      String includedString = "runtimeFilter";
      testPlanMatchingPatterns(sql, null, includedString);
      test(sql);
    }
  }
}
