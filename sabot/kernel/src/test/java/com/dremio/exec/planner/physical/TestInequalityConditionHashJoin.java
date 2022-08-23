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

import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;

/**
 * Tests for supporting inequality expressions with hash join
 */
public class TestInequalityConditionHashJoin extends PlanTestBase {
  private static AutoCloseable autoCloseable;

  @BeforeClass
  public static void setup() throws Exception {
    autoCloseable = withOption(PlannerSettings.EXTRA_CONDITIONS_HASHJOIN, true);
  }

  @AfterClass
  public static void clean() throws Exception {
    autoCloseable.close();
  }

  @Test
  public void testSimpleInequalityConditionLeftOuterJoin() throws Exception {
    final String query = "SELECT * FROM cp.\"tpch/orders.parquet\" o \n" +
      "LEFT OUTER JOIN cp.\"tpch/lineitem.parquet\" l \n" +
      "ON o.o_orderkey = l.l_orderkey\n" +
      "AND o.o_totalprice / l.l_quantity > 100.0";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashJoin(condition=[=($0, $16)], joinType=[right], extraCondition=[>(/($19, $4), 100.0)])")
      },
      "NestedLoopJoin");
  }

  @Test
  public void testSimpleInequalityConditionRightOuterJoin() throws Exception {
    final String query = "SELECT * FROM cp.\"tpch/orders.parquet\" o \n" +
      "RIGHT OUTER JOIN cp.\"tpch/lineitem.parquet\" l \n" +
      "ON o.o_orderkey = l.l_orderkey\n" +
      "AND o.o_totalprice / l.l_quantity > 100.0";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashJoin(condition=[=($0, $16)], joinType=[left], extraCondition=[>(/($19, $4), 100.0)])")
      },
      "NestedLoopJoin");
  }

  @Test
  public void testComplexInequalityCondition1() throws Exception {
    final String query = "SELECT\n" +
      "l.l_orderkey, o.o_orderdate, o.o_shippriority\n" +
      "FROM cp.\"tpch/orders.parquet\" o\n" +
      "LEFT OUTER JOIN cp.\"tpch/lineitem.parquet\" l ON l.l_orderkey = o.o_orderkey AND o.o_orderdate < l.l_shipdate\n" +
      "RIGHT OUTER JOIN cp.\"tpch/customer.parquet\" c ON c.c_custkey = o.o_custkey AND o.o_orderkey / c.c_nationkey > 100.0\n" +
      "INNER JOIN cp.\"tpch/nation.parquet\" n ON c.c_nationkey = n.n_nationkey";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashJoin(condition=[=($3, $4)], joinType=[inner])"),
        Pattern.quote("HashJoin(condition=[=($1, $5)], joinType=[right], extraCondition=[>(/($0, $6), 100.0)])"),
        Pattern.quote("HashJoin(condition=[=($0, $2)], joinType=[right], extraCondition=[<($4, $1)])")
      },
      "NestedLoopJoin");
  }

  @Test
  public void testComplexInequalityCondition2() throws Exception {
    final String query = "SELECT DISTINCT l.l_partkey\n" +
      "FROM cp.\"tpch/orders.parquet\" o\n" +
      "LEFT JOIN cp.\"tpch/lineitem.parquet\" l \n" +
      "ON (\n" +
      "(((o.o_custkey IN (63190) AND l.l_suppkey IN (73573)) OR \n" +
      "(o.o_custkey IN (21878) AND l.l_suppkey IN (97916)) OR \n" +
      "(o.o_custkey IN (187442) AND l.l_suppkey IN (31359)))) AND \n" +
      "(l.l_orderkey = o.o_orderkey))\n" +
      "WHERE ((o.o_custkey IN (63190,21878,187442)))";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashJoin(condition=[=($0, $5)], joinType=[right], extraCondition=[OR(AND($6, $2), AND($7, $3), AND($8, $4))])")
      },
      "NestedLoopJoin");
  }

  @Test
  public void testComplexInequalityCondition3() throws Exception {
    // Same as com.dremio.exec.physical.impl.join.TestNestedLoopJoin.testNlJoinWithLeftOuterJoin
    // but with INEQUALITY_HASHJOIN enabled
    String query = "SELECT * FROM cp.\"tpch/orders.parquet\" o \n" +
      "LEFT OUTER JOIN cp.\"tpch/lineitem.parquet\" l \n" +
      "ON o.o_orderkey = l.l_orderkey\n" +
      "AND o.o_custkey = 63190\n" +
      "AND o.o_totalprice / l.l_quantity > 100.0";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashJoin(condition=[=($0, $16)], joinType=[right], extraCondition=[AND($25, >(/($19, $4), 100.0))])")
      },
      "NestedLoopJoin");
  }
}
