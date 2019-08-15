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

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.TestTools;

public class TestJoinNullable extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJoinNullable.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void testHelper(String query, int expectedRecordCount) throws Exception {
    final int actualRecordCount = testSql(query);
    assertEquals("Number of output rows", expectedRecordCount, actualRecordCount);
  }

  /** InnerJoin on nullable cols, HashJoin */
  @Test
  public void testHashInnerJoinOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_root.\"%s/jsoninput/nullable1.json\" t1, " +
                   " dfs_root.\"%s/jsoninput/nullable2.json\" t2 where t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);
    testHelper(query, 1);
  }

  /** LeftOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashLOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_root.\"%s/jsoninput/nullable1.json\" t1 " +
                      " left outer join dfs_root.\"%s/jsoninput/nullable2.json\" t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 2);
  }

  /** RightOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashROJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_root.\"%s/jsoninput/nullable1.json\" t1 " +
                      " right outer join dfs_root.\"%s/jsoninput/nullable2.json\" t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 4);
  }

  /** FullOuterJoin on nullable cols, HashJoin */
  @Test
  public void testHashFOJOnNullableColumns() throws Exception {
    String query = String.format("select t1.a1, t1.b1, t2.a2, t2.b2 from dfs_root.\"%s/jsoninput/nullable1.json\" t1 " +
                      " full outer join dfs_root.\"%s/jsoninput/nullable2.json\" t2 " +
                      " on t1.b1 = t2.b2", TEST_RES_PATH, TEST_RES_PATH);

    testHelper(query, 5);
  }

  @Test
  public void withDistinctFromJoinConditionHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
            "cp.\"jsoninput/nullableOrdered1.json\" t1 JOIN " +
            "cp.\"jsoninput/nullableOrdered2.json\" t2 " +
            "ON t1.key IS NOT DISTINCT FROM t2.key AND t1.data is NOT null";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Ignore("DX-11205")
  @Test
  public void withNullEqualHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
            "cp.\"jsoninput/nullableOrdered1.json\" t1 JOIN " +
            "cp.\"jsoninput/nullableOrdered2.json\" t2 " +
            "ON t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Ignore("DX-11205")
  @Test
  public void withNullEqualInWhereConditionHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullableOrdered1.json\" t1, " +
        "cp.\"jsoninput/nullableOrdered2.json\" t2 " +
        "WHERE t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
    nullEqualJoinHelper(query);
  }

  @Test
  public void withNullEqualInWhereConditionNegative() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullableOrdered1.json\" t1, " +
        "cp.\"jsoninput/nullableOrdered2.json\" t2, " +
        "cp.\"jsoninput/nullableOrdered3.json\" t3 " +
        "WHERE t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)";
    errorMsgTestHelper(query,
        "This query cannot be planned");
  }

  @Ignore("DX-11205")
  @Test
  public void withNullEqualInWhereConditionThreeTableHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullableOrdered1.json\" t1, " +
        "cp.\"jsoninput/nullableOrdered2.json\" t2, " +
        "cp.\"jsoninput/nullableOrdered3.json\" t3 " +
        "WHERE (t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)) AND" +
        "(t2.key = t3.key OR (t2.key IS NULL AND t3.key IS NULL))";
    testPlanSubstrPatterns(query, new String[] { "HashJoin", "IS NOT DISTINCT FROM" }, null);
  }

  public void nullEqualJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues(null, "L_null_1", "R_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null)
        .baselineValues(null, "L_null_2", "R_null_2", null)
        .baselineValues(null, "L_null_1", "R_null_3", null)
        .baselineValues(null, "L_null_2", "R_null_3", null)
        .go();
  }

  public void nullEqual3WayJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0", "data1", "key1")
        .baselineValues(null, "L_null_1", "R_null_1", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_1", null, "RR_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A", "RR_A_1", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A", "RR_A_1", "A")
        .baselineValues("A", "L_A_1", "R_A_1", "A", "RR_A_2", "A")
        .baselineValues("A", "L_A_2", "R_A_1", "A", "RR_A_2", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_2", null, "RR_null_1", null)
        .baselineValues(null, "L_null_1", "R_null_3", null, "RR_null_1", null)
        .baselineValues(null, "L_null_2", "R_null_3", null, "RR_null_1", null)
        .go();
  }
  @Ignore("DX-11205")
  @Test
  public void withNullEqualAdditionFilter() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullableOrdered1.json\" t1 JOIN " +
        "cp.\"jsoninput/nullableOrdered2.json\" t2 " +
        "ON (t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)) AND t1.data LIKE '%1%'";

    testPlanSubstrPatterns(query,
        new String[] {
            "HashJoin(condition=[IS NOT DISTINCT FROM($0, $4)], joinType=[inner])",
            "Filter(condition=[$2])", // 'like' is pushed into project
            "[LIKE($1, '%1%')]"
        },
        null);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues(null, "L_null_1", "R_null_1", null)
        .baselineValues("A", "L_A_1", "R_A_1", "A")
        .baselineValues(null, "L_null_1", "R_null_2", null)
        .baselineValues(null, "L_null_1", "R_null_3", null)
        .go();
  }

  @Ignore("DX-12609: need to update test case")
  @Test
  public void withMixedEqualAndIsNotDistinctHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullEqualJoin1.json\" t1 JOIN " +
        "cp.\"jsoninput/nullEqualJoin2.json\" t2 " +
        "ON t1.key = t2.key AND t1.data is not distinct from t2.data";
    testPlanOneExpectedPattern(query, "HashJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
    nullMixedComparatorEqualJoinHelper(query);
  }

  @Ignore("DX-9864")
  @Test
  public void withMixedEqualAndIsNotDistinctFilterHashJoin() throws Exception {
    final String query = "SELECT * FROM " +
        "cp.\"jsoninput/nullEqualJoin1.json\" t1 JOIN " +
        "cp.\"jsoninput/nullEqualJoin2.json\" t2 " +
        "ON t1.key = t2.key " +
        "WHERE t1.data is not distinct from t2.data";
    // Expected the filter to be pushed into the join
    testPlanOneExpectedPattern(query, "HashJoin.*condition.*AND\\(=\\(.*IS NOT DISTINCT FROM*");
    nullMixedComparatorEqualJoinHelper(query);
  }

  public void nullMixedComparatorEqualJoinHelper(final String query) throws Exception {
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("key", "data", "data0", "key0")
        .baselineValues("A", "L_A_1", "L_A_1", "A")
        .baselineValues("A", null, null, "A")
        .baselineValues("B", null, null, "B")
        .baselineValues("B", "L_B_1", "L_B_1", "B")
        .go();
  }
}
