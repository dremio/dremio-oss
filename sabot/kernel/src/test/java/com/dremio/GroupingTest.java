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

import com.dremio.common.util.TestTools;
import com.dremio.test.TemporarySystemProperties;
import org.junit.Rule;
import org.junit.Test;

public class GroupingTest extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(GroupingTest.class);
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test // Simple grouping with rollup
  public void testGroupingWithRollup() throws Exception {
    String query =
        "select n_regionkey, count(*), GROUPING(n_regionkey) from cp.\"tpch/nation.parquet\" group by rollup (n_regionkey);";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "EXPR$1", "EXPR$2")
        .baselineValues(null, 25L, 1L)
        .baselineValues(1, 5L, 0L)
        .baselineValues(4, 5L, 0L)
        .baselineValues(3, 5L, 0L)
        .baselineValues(2, 5L, 0L)
        .baselineValues(0, 5L, 0L)
        .go();
  }

  @Test // Simple grouping with rollup
  public void testGroupingWithRollupWithGroupingFirst() throws Exception {
    String query =
        "select n_regionkey, GROUPING(n_regionkey), avg(n_nationkey) from cp.\"tpch/nation.parquet\" group by rollup (n_regionkey);";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "EXPR$1", "EXPR$2")
        .baselineValues(null, 1L, 12.0)
        .baselineValues(1, 0L, 9.4)
        .baselineValues(4, 0L, 11.6)
        .baselineValues(3, 0L, 15.4)
        .baselineValues(2, 0L, 13.6)
        .baselineValues(0, 0L, 10.0)
        .go();
  }

  @Test // Simple grouping without rollup
  public void testGroupingWithoutRollup() throws Exception {
    String query =
        "select n_regionkey, count(*) as cnt , GROUPING(n_regionkey) from cp.\"tpch/nation.parquet\" group by (n_regionkey);";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt", "EXPR$2")
        .baselineValues(1, 5L, 0L)
        .baselineValues(4, 5L, 0L)
        .baselineValues(3, 5L, 0L)
        .baselineValues(2, 5L, 0L)
        .baselineValues(0, 5L, 0L)
        .go();
  }

  @Test // Simple grouping using values
  public void testGroupingValues() throws Exception {
    String query =
        "SELECT department, gender, sum(salary) as salary_sum, "
            + "GROUPING(department) as GP_Department, "
            + "GROUPING(gender) as GP_Gender "
            + "FROM (VALUES (1, 'David', 'Male', 5000, 'Sales'),"
            + "(2, 'Jim', 'Female', 6000, 'HR'),"
            + "(3, 'Kate', 'Female', 7500, 'IT'),"
            + "(4, 'Will', 'Male', 6500, 'Marketing'),"
            + "(5, 'Shane', 'Female', 5500, 'Finance'),"
            + "(6, 'Shed', 'Male', 8000, 'Sales'),"
            + "(7, 'Vik', 'Male', 7200, 'HR'), "
            + "(8, 'Vince', 'Female', 6600, 'IT'), "
            + "(9, 'Jane', 'Female', 5400, 'Marketing'), "
            + "(10, 'Laura', 'Female', 6300, 'Finance'), "
            + "(11, 'Mac', 'Male', 5700, 'Sales'), "
            + "(12, 'Pat', 'Male', 7000, 'HR'), "
            + "(13, 'Julie', 'Female', 7100, 'IT'), "
            + "(14, 'Elice', 'Female', 6800, 'Marketing'), "
            + "(15, 'Wayne', 'Male', 5000, 'Finance')) "
            + "AS t(id, name, gender, salary, department) "
            + "GROUP BY ROLLUP (department, gender);";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department", "gender", "salary_sum", "GP_Department", "GP_Gender")
        .baselineValues("HR", "Male", 14200L, 0L, 0L)
        .baselineValues("HR", "Female", 6000L, 0L, 0L)
        .baselineValues("HR", null, 20200L, 0L, 1L)
        .baselineValues("Finance", "Male", 5000L, 0L, 0L)
        .baselineValues("Finance", "Female", 11800L, 0L, 0L)
        .baselineValues("Finance", null, 16800L, 0L, 1L)
        .baselineValues("Sales", "Male", 18700L, 0L, 0L)
        .baselineValues("Sales", null, 18700L, 0L, 1L)
        .baselineValues("IT", "Female", 21200L, 0L, 0L)
        .baselineValues("IT", null, 21200L, 0L, 1L)
        .baselineValues("Marketing", "Male", 6500L, 0L, 0L)
        .baselineValues("Marketing", "Female", 12200L, 0L, 0L)
        .baselineValues("Marketing", null, 18700L, 0L, 1L)
        .baselineValues(null, null, 95600L, 1L, 1L)
        .go();
  }

  @Test // Grouping filter with rollup
  public void testGroupingInFilterWithRollup() throws Exception {
    String query =
        "select n_regionkey, count(*) from cp.\"tpch/nation.parquet\" group by rollup(n_regionkey) HAVING GROUPING(n_regionkey) = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "EXPR$1")
        .baselineValues(null, 25L)
        .go();
  }

  @Test // Grouping filter without rollup
  public void testGroupingInFilterWithoutRollup() throws Exception {
    String query =
        "select n_regionkey, count(*) from cp.\"tpch/nation.parquet\" group by n_regionkey HAVING GROUPING(n_regionkey) = 1";

    testBuilder().sqlQuery(query).unOrdered().expectsEmptyResultSet().go();
  }

  @Test // Simple grouping_ID
  public void testGroupingID1() throws Exception {
    String query =
        "select n_regionkey, count(*), GROUPING_ID(n_regionkey) as GP_Region from cp.\"tpch/nation.parquet\" group by rollup(n_regionkey);";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "EXPR$1", "GP_Region")
        .baselineValues(null, 25L, 1L)
        .baselineValues(1, 5L, 0L)
        .baselineValues(4, 5L, 0L)
        .baselineValues(3, 5L, 0L)
        .baselineValues(2, 5L, 0L)
        .baselineValues(0, 5L, 0L)
        .go();
  }

  @Test
  public void testGroupingOnMultipleColumns() throws Exception {
    String inner =
        "select n_name, n_regionkey, count(*), GROUPING_ID(n_name, n_regionkey) as gp from cp.\"tpch/nation.parquet\" group by cube(n_name, n_regionkey)";
    String query = String.format("select gp, count(*) as cnt from (%s) group by gp", inner);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("gp", "cnt")
        .baselineValues(3L, 1L)
        .baselineValues(1L, 25L)
        .baselineValues(2L, 5L)
        .baselineValues(0L, 25L)
        .go();
  }

  @Test
  public void testDistinctGroupingAgg() throws Exception {
    String query =
        "SELECT l_returnflag, l_linestatus, count(distinct l_shipdate) cnt from cp.\"tpch/lineitem.parquet\"\n"
            + "group by rollup(l_returnflag, l_linestatus) order by 1 nulls first,2 nulls first";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("l_returnflag", "l_linestatus", "cnt")
        .baselineValues(null, null, 2518L)
        .baselineValues("A", null, 1253L)
        .baselineValues("A", "F", 1253L)
        .baselineValues("N", null, 1288L)
        .baselineValues("N", "F", 28L)
        .baselineValues("N", "O", 1260L)
        .baselineValues("R", null, 1249L)
        .baselineValues("R", "F", 1249L)
        .go();
  }
}
