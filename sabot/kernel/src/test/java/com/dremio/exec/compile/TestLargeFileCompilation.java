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
package com.dremio.exec.compile;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.google.common.base.Joiner;

public class TestLargeFileCompilation extends BaseTestQuery {
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(150, TimeUnit.SECONDS); // 150secs

  private static final String LARGE_QUERY_GROUP_BY;

  private static final String LARGE_QUERY_ORDER_BY;

  private static final String LARGE_QUERY_ORDER_BY_WITH_LIMIT;

  private static final String LARGE_QUERY_FILTER;

  private static final String LARGE_QUERY_WRITER;

  private static final String LARGE_QUERY_SELECT_LIST;

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestLargeFileCompilation.iteration", "1"));

  private static final int NUM_PROJECT_COULMNS = 2000;

  private static final int NUM_ORDERBY_COULMNS = 500;

  private static final int NUM_GROUPBY_COULMNS = 225;

  private static final int NUM_FILTER_COULMNS = 150;

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    sb.append("full_name\nfrom (select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as c").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.\"employee.json\")\ngroup by\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    LARGE_QUERY_GROUP_BY = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.\"employee.json\"\n\n\t");
    LARGE_QUERY_SELECT_LIST = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.\"employee.json\"\norder by\n\t");
    for (int i = 0; i < NUM_ORDERBY_COULMNS; i++) {
      sb.append(" col").append(i).append(", ");
    }
    LARGE_QUERY_ORDER_BY = sb.append("full_name").toString();
    LARGE_QUERY_ORDER_BY_WITH_LIMIT = sb.append("\nlimit 1").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select *\n")
      .append("from cp.\"employee.json\"\n")
      .append("where");
    for (int i = 0; i < NUM_FILTER_COULMNS; i++) {
      sb.append(" employee_id+").append(i).append(" < employee_id ").append(i%2==0?"OR":"AND");
    }
    LARGE_QUERY_FILTER = sb.append(" true") .toString();
  }

  static {
    StringBuilder sb = new StringBuilder("create table %s as (select \n");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    LARGE_QUERY_WRITER = sb.append("full_name\nfrom cp.\"employee.json\" limit 1)").toString();
  }

  @Test
  public void testTEXT_WRITER() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult("use dfs_test");
    testNoResult("alter session set \"%s\"='csv'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(LARGE_QUERY_WRITER, "wide_table_csv");
  }

  @Test
  public void testPARQUET_WRITER() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult("use dfs_test");
    testNoResult("alter session set \"%s\"='parquet'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_WRITER, "wide_table_parquet");
  }

  @Test
  public void testGROUP_BY() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_GROUP_BY);
  }

  @Test
  public void testEXTERNAL_SORT() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY);
  }

  @Test
  public void testTOP_N_SORT() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY_WITH_LIMIT);
  }

  @Test
  public void testFILTER() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_FILTER);
  }

  @Test
  public void testProject() throws Exception {
    testNoResult("alter session set \"%s\"='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_SELECT_LIST);
  }

  @Test
  public void testFilterWithMultipleCaseStatements() throws Exception {
    String sql = "SELECT CASE \n" +
      "         WHEN ( l.l_orderkey = 1 ) THEN 'one' \n" +
      "         WHEN ( l.l_orderkey = 2 ) THEN 'two' \n" +
      "         WHEN ( l.l_orderkey = 3 ) THEN 'three' \n" +
      "         WHEN ( l.l_orderkey = 4 ) THEN 'four' \n" +
      "         WHEN ( l.l_orderkey = 5 ) THEN 'five' \n" +
      "         WHEN ( l.l_orderkey = 6 ) THEN 'six' \n" +
      "         WHEN ( l.l_orderkey = 7 ) THEN 'seven' \n" +
      "         WHEN ( l.l_orderkey = 8 ) THEN 'eight' \n" +
      "         WHEN ( l.l_orderkey = 9 ) THEN 'nine' \n" +
      "         WHEN ( l.l_orderkey = 10 ) THEN 'ten' \n" +
      "         WHEN ( l.l_orderkey = 11 ) THEN 'eleven' \n" +
      "         WHEN ( l.l_orderkey = 12 ) THEN 'twelve' \n" +
      "         WHEN ( l.l_orderkey = 13 ) THEN 'thirteen' \n" +
      "         WHEN ( l.l_orderkey = 14 ) THEN 'fourteen' \n" +
      "         WHEN ( l.l_orderkey = 15 ) THEN 'fifteen' \n" +
      "         WHEN ( l.l_orderkey = 16 ) THEN 'sixteen' \n" +
      "         WHEN ( l.l_orderkey = 17 ) THEN 'seventeen' \n" +
      "         WHEN ( l.l_orderkey = 18 ) THEN 'eighteen' \n" +
      "         WHEN ( l.l_orderkey = 19 ) THEN 'nineteen' \n" +
      "         WHEN ( l.l_orderkey = 20 ) THEN 'twenty' \n" +
      "         WHEN ( l.l_orderkey = 21 ) THEN 'twenty one' \n" +
      "         ELSE NULL \n" +
      "       end AS calculation \n" +
      "FROM   cp.\"tpch/lineitem.parquet\" l\n" +
      "WHERE  ( ( ( CASE \n" +
      "               WHEN ( l.l_orderkey = 1 ) THEN  'one' \n" +
      "               WHEN ( l.l_orderkey = 2 ) THEN  'two' \n" +
      "               WHEN ( l.l_orderkey = 3 ) THEN  'three' \n" +
      "               WHEN ( l.l_orderkey = 4 ) THEN  'four' \n" +
      "               WHEN ( l.l_orderkey = 5 ) THEN  'five' \n" +
      "               WHEN ( l.l_orderkey = 6 ) THEN  'six' \n" +
      "               WHEN ( l.l_orderkey = 7 ) THEN  'seven' \n" +
      "               WHEN ( l.l_orderkey = 8 ) THEN  'eight' \n" +
      "               WHEN ( l.l_orderkey = 9 ) THEN  'nine' \n" +
      "               WHEN ( l.l_orderkey = 10 ) THEN 'ten' \n" +
      "               WHEN ( l.l_orderkey = 11 ) THEN 'eleven' \n" +
      "               WHEN ( l.l_orderkey = 12 ) THEN 'twelve' \n" +
      "               WHEN ( l.l_orderkey = 13 ) THEN 'thirteen' \n" +
      "               WHEN ( l.l_orderkey = 14 ) THEN 'fourteen' \n" +
      "               WHEN ( l.l_orderkey = 15 ) THEN 'fifteen' \n" +
      "               WHEN ( l.l_orderkey = 16 ) THEN 'sixteen' \n" +
      "               WHEN ( l.l_orderkey = 17 ) THEN 'seventeen' \n" +
      "               WHEN ( l.l_orderkey = 18 ) THEN 'eighteen' \n" +
      "               WHEN ( l.l_orderkey = 19 ) THEN 'nineteen' \n" +
      "               WHEN ( l.l_orderkey = 20 ) THEN 'twenty' \n" +
      "               WHEN ( l.l_orderkey = 21 ) THEN 'twenty one' \n" +
      "               ELSE NULL \n" +
      "             end ) IN (\n" +
      "                'one',\n" +
      "                'two',\n" +
      "                'three',\n" +
      "                'four',\n" +
      "                'five',\n" +
      "                'six',\n" +
      "                'seven',\n" +
      "                'eight',\n" +
      "                'nine',\n" +
      "                'ten',\n" +
      "                'eleven',\n" +
      "                'twelve',\n" +
      "                'thirteen',\n" +
      "                'fourteen',\n" +
      "                'fifteen',\n" +
      "                'sixteen',\n" +
      "                'seventeen',\n" +
      "                'eighteen',\n" +
      "                'nineteen',\n" +
      "                'twenty' \n" +
      "                'twenty one' \n" +
      "\t\t) ) \n" +
      "         AND ( ( CASE \n" +
      "                   WHEN ( ( CASE \n" +
      "                              WHEN ( l.l_orderkey = 1 ) THEN 'one' \n" +
      "                              WHEN ( l.l_orderkey = 2 ) THEN 'two' \n" +
      "                              WHEN ( l.l_orderkey = 3 ) THEN 'three' \n" +
      "                              WHEN ( l.l_orderkey = 4 ) THEN 'four' \n" +
      "                              WHEN ( l.l_orderkey = 5 ) THEN 'five' \n" +
      "                              WHEN ( l.l_orderkey = 6 ) THEN 'six' \n" +
      "                              WHEN ( l.l_orderkey = 7 ) THEN 'seven' \n" +
      "                              WHEN ( l.l_orderkey = 8 ) THEN 'eight' \n" +
      "                              WHEN ( l.l_orderkey = 9 ) THEN 'nine' \n" +
      "                              WHEN ( l.l_orderkey = 10 ) THEN 'ten' \n" +
      "                              WHEN ( l.l_orderkey = 11 ) THEN 'eleven' \n" +
      "                              WHEN ( l.l_orderkey = 12 ) THEN 'twelve' \n" +
      "                              WHEN ( l.l_orderkey = 13 ) THEN 'thirteen' \n" +
      "                              WHEN ( l.l_orderkey = 14 ) THEN 'fourteen' \n" +
      "                              WHEN ( l.l_orderkey = 15 ) THEN 'fifteen' \n" +
      "                              WHEN ( l.l_orderkey = 16 ) THEN 'sixteen' \n" +
      "                              WHEN ( l.l_orderkey = 17 ) THEN 'seventeen' \n" +
      "                              WHEN ( l.l_orderkey = 18 ) THEN 'eighteen' \n" +
      "                              WHEN ( l.l_orderkey = 19 ) THEN 'nineteen' \n" +
      "                              WHEN ( l.l_orderkey = 20 ) THEN 'twenty' \n" +
      "                              WHEN ( l.l_orderkey = 21 ) THEN 'twenty one' \n" +
      "                              ELSE NULL \n" +
      "                            end ) IS NULL ) THEN 0 \n" +
      "                   ELSE 1 \n" +
      "                 end ) = 1 ) )\n";
    test(sql);
  }

  @Test
  public void manyRegExp() throws Exception {
    List<String> s = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      s.add(String.format("regexp_matches(n_comment, '%d')", i));
    }

    String where = Joiner.on(" and\n").join(s);

    String sql = String.format("select * from cp.\"tpch/nation.parquet\" where %s", where);

    test(sql);
  }
}
