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

import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;

/**
 * Plan tests for list_agg aggregate call
 */
public class TestListAggPlan extends PlanTestBase {

  @Test
  public void testSimpleListAgg() throws Exception {
    final String query = "SELECT LISTAGG(o_comment) \"comment_list\",\n" +
      "       MIN(o_orderkey) \"Earliest\"\n" +
      "  FROM cp.\"tpch/orders.parquet\"\n" +
      "  WHERE o_custkey > 1";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("HashAgg(group=[{}], comment_list=[LISTAGG($0)], Earliest=[MIN($1)])")
      },
      "StreamAgg");
  }

  @Test
  public void testSimpleListAggWithDistinct() throws Exception {
    final String query = "SELECT LISTAGG(distinct o_comment) \"comment_list\",\n" +
      "       MIN(o_orderkey) \"Earliest\"\n" +
      "  FROM cp.\"tpch/orders.parquet\"\n" +
      "  WHERE o_custkey > 1";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("00-02        HashAgg(group=[{}], comment_list=[LISTAGG(DISTINCT $0)], Earliest=[MIN($1)])"),
        Pattern.quote("00-03          Project(o_comment=[$2], o_orderkey=[$0])")
      },
      "StreamAgg");
  }

  @Test
  public void testListAggWithDelimiter() throws Exception {
    final String query = "SELECT LISTAGG(o_comment, '; ') \"comment_list\",\n" +
      "       MIN(o_orderkey) \"Earliest\"\n" +
      "  FROM cp.\"tpch/orders.parquet\"\n" +
      "  WHERE o_custkey > 1";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("00-02        HashAgg(group=[{}], comment_list=[LISTAGG($0, $1)], Earliest=[MIN($2)])"),
        Pattern.quote("00-03          Project(o_comment=[$2], $f1=['; ':VARCHAR(2)], o_orderkey=[$0])")
      },
      "StreamAgg");
  }

  @Test
  public void testListAggWithWithinGroupOrderBy() throws Exception {
    final String query = "SELECT LISTAGG(o_comment, '; ') WITHIN GROUP (ORDER BY o_comment) \"comment_list\",\n" +
      "       MIN(o_orderkey) \"Earliest\"\n" +
      "  FROM cp.\"tpch/orders.parquet\"\n" +
      "  WHERE o_custkey > 1";
    testPlanMatchingPatterns(query, new String[]{
        Pattern.quote("00-02        HashAgg(group=[{}], comment_list=[LISTAGG($0, $1) WITHIN GROUP ([0])], Earliest=[MIN($2)])"),
        Pattern.quote("00-03          Project(o_comment=[$2], $f1=['; ':VARCHAR(2)], o_orderkey=[$0])")
      },
      "StreamAgg");
  }

  @Test
  public void test2PhaseListAggWithoutDelimiter() throws Exception {
    try (AutoCloseable autoCloseable = withOption(ExecConstants.SLICE_TARGET_OPTION, 1)){
      final String query = "SELECT LISTAGG(o_comment) \"comment_list\",\n" +
        "       MIN(o_orderkey) \"Earliest\"\n" +
        "  FROM cp.\"tpch/orders.parquet\"\n" +
        "  group by o_custkey";
      testPlanMatchingPatterns(query, new String[]{
          Pattern.quote("01-02            HashAgg(group=[{0}], comment_list=[listagg_merge($1)], Earliest=[MIN($2)])"),
          Pattern.quote("01-03              Project(o_custkey=[$0], comment_list=[$1], Earliest=[$2])"),
          Pattern.quote("01-04                HashToRandomExchange(dist0=[[$0]])"),
          Pattern.quote("02-01                  Project(o_custkey=[$0]"),
          Pattern.quote("02-02                    HashAgg(group=[{0}], comment_list=[local_listagg($1)], Earliest=[MIN($2)])"),
          Pattern.quote("02-03                      Project(o_custkey=[$1], o_comment=[$2], o_orderkey=[$0])")
        },
        "StreamAgg");
    }
  }

  @Test
  public void test2PhaseListAggWithDelimiter() throws Exception {
    try (AutoCloseable autoCloseable = withOption(ExecConstants.SLICE_TARGET_OPTION, 1)){
      final String query = "SELECT LISTAGG(o_comment, '; ') \"comment_list\",\n" +
        "       MIN(o_orderkey) \"Earliest\"\n" +
        "  FROM cp.\"tpch/orders.parquet\"\n" +
        "  group by o_custkey";
      testPlanMatchingPatterns(query, new String[]{
          Pattern.quote("01-02            HashAgg(group=[{0}], comment_list=[listagg_merge($1, $3)], Earliest=[MIN($2)])"),
          Pattern.quote("01-03              Project(o_custkey=[$0], comment_list=[$1], Earliest=[$2], EXPR$0=['; ':VARCHAR(2)])"),
          Pattern.quote("01-05                  HashToRandomExchange(dist0=[[$0]])"),
          Pattern.quote("02-01                    Project(o_custkey=[$0]"),
          Pattern.quote("02-02                      HashAgg(group=[{0}], comment_list=[local_listagg($1, $2)], Earliest=[MIN($3)])"),
          Pattern.quote("02-03                        Project(o_custkey=[$1], o_comment=[$2], $f2=['; ':VARCHAR(2)], o_orderkey=[$0])")
        },
        "StreamAgg");
    }
  }

  @Test
  public void test2PhaseListAggWithOrderByAndDelimiter() throws Exception {
    try (AutoCloseable autoCloseable = withOption(ExecConstants.SLICE_TARGET_OPTION, 1)) {
      final String query = "SELECT LISTAGG(o_comment, '; ') WITHIN GROUP (ORDER BY o_comment) \"comment_list\",\n" +
        "       MIN(o_orderkey) \"Earliest\"\n" +
        "  FROM cp.\"tpch/orders.parquet\"\n" +
        "  group by o_custkey";
      testPlanMatchingPatterns(query, new String[]{
          Pattern.quote("01-02            HashAgg(group=[{0}], comment_list=[listagg_merge($1, $3) WITHIN GROUP ([1])], Earliest=[MIN($2)])"),
          Pattern.quote("01-03              Project(o_custkey=[$0], comment_list=[$1], Earliest=[$2], EXPR$0=['; ':VARCHAR(2)])"),
          Pattern.quote("01-05                  HashToRandomExchange(dist0=[[$0]])"),
          Pattern.quote("02-01                    Project(o_custkey=[$0]"),
          Pattern.quote("02-02                      HashAgg(group=[{0}], comment_list=[local_listagg($1, $2) WITHIN GROUP ([1])], Earliest=[MIN($3)])"),
          Pattern.quote("02-03                        Project(o_custkey=[$1], o_comment=[$2], $f2=['; ':VARCHAR(2)], o_orderkey=[$0])")
        },
        "StreamAgg");
    }
  }

  @Test
  public void test2PhaseListAggWithOrderByAndWithoutDelimiter() throws Exception {
    try (AutoCloseable autoCloseable = withOption(ExecConstants.SLICE_TARGET_OPTION, 1)) {
      final String query = "SELECT LISTAGG(o_comment) WITHIN GROUP (ORDER BY o_comment) \"comment_list\",\n" +
        "       MIN(o_orderkey) \"Earliest\"\n" +
        "  FROM cp.\"tpch/orders.parquet\"\n" +
        "  group by o_custkey";
      testPlanMatchingPatterns(query, new String[]{
          Pattern.quote("01-02            HashAgg(group=[{0}], comment_list=[listagg_merge($1) WITHIN GROUP ([1])], Earliest=[MIN($2)])"),
          Pattern.quote("01-03              Project(o_custkey=[$0], comment_list=[$1], Earliest=[$2])"),
          Pattern.quote("01-04                HashToRandomExchange(dist0=[[$0]])"),
          Pattern.quote("02-01                  Project(o_custkey=[$0]"),
          Pattern.quote("02-02                    HashAgg(group=[{0}], comment_list=[local_listagg($1) WITHIN GROUP ([1])], Earliest=[MIN($2)])"),
          Pattern.quote("02-03                      Project(o_custkey=[$1], o_comment=[$2], o_orderkey=[$0])")
        },
        "StreamAgg");
    }
  }

  @Test (expected = UserRemoteException.class)
  public void testVariableDelimiter() throws Exception {
    final String query = "SELECT LISTAGG(o_comment, o_clerk) FROM cp.\"tpch/orders.parquet\"";
    test(query);
  }
}
