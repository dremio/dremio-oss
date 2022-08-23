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

import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.util.TestTools;
import com.dremio.test.TemporarySystemProperties;

public class GroupSetTest extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GroupSetTest.class);
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test  // Simple grouping sets
  public void testGroupSet1() throws Exception {
    String query = "select n_regionkey, count(*) as cnt from "
        + "cp.\"tpch/nation.parquet\" group by grouping sets(n_regionkey, n_name)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n_regionkey", "cnt")
        .baselineValues(   1, 5L)
        .baselineValues(   4, 5L)
        .baselineValues(   2, 5L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(   3, 5L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(   0, 5L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .baselineValues(null, 1L)
        .go();
  }

  @Test  // Simple grouping sets using values
  public void testGroupSet2() throws Exception {
    String query = "SELECT c1, c2, count(*) AS my_count\n"
      + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS INTEGER), 1)) AS t(c1, c2)\n"
      + "GROUP BY GROUPING SETS (\n"
      + "        (c1, c2),\n"
      + "        (c1),\n"
      + "        (c2),\n"
      + "        ()\n"
      + ") ORDER BY 1, 2";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("c1", "c2", "my_count")
      .baselineValues(   1,    1, 1L)
      .baselineValues(   1,    2, 1L)
      .baselineValues(   1, null, 2L)
      .baselineValues(null,    1, 1L)
      .baselineValues(null,    1, 2L)
      .baselineValues(null,    2, 1L)
      .baselineValues(null, null, 1L)
      .baselineValues(null, null, 3L)
      .go();
  }

  @Test  // Simple grouping sets using values
  public void testGroupSetValues() throws Exception {
    String query = "SELECT department, gender, sum(salary) as salary_sum " +
      "FROM (VALUES (1, 'David', 'Male', 5000, 'Sales')," +
      "(2, 'Jim', 'Female', 6000, 'HR')," +
      "(3, 'Kate', 'Female', 7500, 'IT')," +
      "(4, 'Will', 'Male', 6500, 'Marketing')," +
      "(5, 'Shane', 'Female', 5500, 'Finance')," +
      "(6, 'Shed', 'Male', 8000, 'Sales')," +
      "(7, 'Vik', 'Male', 7200, 'HR'), " +
      "(8, 'Vince', 'Female', 6600, 'IT'), " +
      "(9, 'Jane', 'Female', 5400, 'Marketing'), " +
      "(10, 'Laura', 'Female', 6300, 'Finance'), " +
      "(11, 'Mac', 'Male', 5700, 'Sales'), " +
      "(12, 'Pat', 'Male', 7000, 'HR'), " +
      "(13, 'Julie', 'Female', 7100, 'IT'), " +
      "(14, 'Elice', 'Female', 6800, 'Marketing'), " +
      "(15, 'Wayne', 'Male', 5000, 'Finance')) " +
      "AS t(id, name, gender, salary, department) " +
      "GROUP BY ROLLUP (department, gender);";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("department", "gender", "salary_sum")
      .baselineValues("HR", "Male", 14200L)
      .baselineValues("HR", "Female", 6000L)
      .baselineValues("HR", null, 20200L)
      .baselineValues("Finance", "Male", 5000L)
      .baselineValues("Finance", "Female", 11800L)
      .baselineValues("Finance", null, 16800L)
      .baselineValues("Sales", "Male", 18700L)
      .baselineValues("Sales", null, 18700L)
      .baselineValues("IT", "Female", 21200L)
      .baselineValues("IT", null, 21200L)
      .baselineValues("Marketing", "Male", 6500L)
      .baselineValues("Marketing", "Female", 12200L)
      .baselineValues("Marketing", null, 18700L)
      .baselineValues(null, null, 95600L)
      .go();
  }

  @Test  // Simple ROLLUP test
  public void testRollup1() throws Exception {
    String query = "select n_regionkey, n_name, count(*) as cnt from "
      + "cp.\"tpch/nation.parquet\" group by rollup(n_regionkey, n_name)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "n_name", "cnt")
      .baselineValues(null, null, 25L)
      .baselineValues(   1, "ARGENTINA", 1L)
      .baselineValues(   1, "CANADA", 1L)
      .baselineValues(   4, "IRAN", 1L)
      .baselineValues(   1, "PERU", 1L)
      .baselineValues(   2, "JAPAN", 1L)
      .baselineValues(   2, "CHINA", 1L)
      .baselineValues(   2, "VIETNAM", 1L)
      .baselineValues(   0, "ALGERIA", 1L)
      .baselineValues(   0, null, 5L)
      .baselineValues(   4, null, 5L)
      .baselineValues(   3, null, 5L)
      .baselineValues(   2, null, 5L)
      .baselineValues(   2, "INDONESIA", 1L)
      .baselineValues(   4, "JORDAN", 1L)
      .baselineValues(   3, "GERMANY", 1L)
      .baselineValues(   4, "IRAQ", 1L)
      .baselineValues(   0, "MOROCCO", 1L)
      .baselineValues(   4, "SAUDI ARABIA", 1L)
      .baselineValues(   4, "EGYPT", 1L)
      .baselineValues(   1, "UNITED STATES", 1L)
      .baselineValues(   1, null, 5L)
      .baselineValues(   2, "INDIA", 1L)
      .baselineValues(   3, "RUSSIA", 1L)
      .baselineValues(   1, "BRAZIL", 1L)
      .baselineValues(   0, "ETHIOPIA", 1L)
      .baselineValues(   3, "ROMANIA", 1L)
      .baselineValues(   0, "KENYA", 1L)
      .baselineValues(   0, "MOZAMBIQUE", 1L)
      .baselineValues(   3, "UNITED KINGDOM", 1L)
      .baselineValues(   3, "FRANCE", 1L)
      .go();
  }

  @Test  // Simple ROLLUP test
  public void testRollup2() throws Exception {
    String query = String.format(
      "SELECT warehouse, SUM(quantity) FROM dfs.\"%s/phone_inventory.parquet\" GROUP BY ROLLUP(warehouse)",
      TEST_RES_PATH);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("warehouse", "EXPR$1")
      .baselineValues(null, 1210L)
      .baselineValues("San Jose", 650L)
      .baselineValues("San Francisco", 560L)
      .go();
  }

  @Test  // Simple ROLLUP test
  public void testCube1() throws Exception {
    String query = "select n_regionkey, count(*) as cnt from "
      + "cp.\"tpch/nation.parquet\" group by cube(n_regionkey, n_name)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "cnt")
      .baselineValues(   1, 1L)
      .baselineValues(   1, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   4, 1L)
      .baselineValues(   1, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   2, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   2, 1L)
      .baselineValues(   2, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   0, 1L)
      .baselineValues(   0, 5L)
      .baselineValues(   4, 5L)
      .baselineValues(   3, 5L)
      .baselineValues(   2, 5L)
      .baselineValues(   2, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   4, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   3, 1L)
      .baselineValues(   4, 1L)
      .baselineValues(   0, 1L)
      .baselineValues(   4, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   4, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   1, 1L)
      .baselineValues(   1, 5L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   3, 1L)
      .baselineValues(   2, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   3, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 25L)
      .baselineValues(   1, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   0, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   3, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   0, 1L)
      .baselineValues(   0, 1L)
      .baselineValues(null, 1L)
      .baselineValues(   3, 1L)
      .baselineValues(null, 1L)
      .go();
  }

  @Test  // multicolumn CUBE test
  public void testCube2() throws Exception {
    String query = String.format(
      "SELECT\n" +
        "  warehouse, product, SUM(quantity)\n" +
        "  FROM dfs.\"%s/phone_inventory.parquet\"" +
        " GROUP BY\n" +
        "  CUBE(warehouse,product)\n" +
        "  ORDER BY\n" +
        "  warehouse,\n" +
        "  product",
      TEST_RES_PATH);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("warehouse", "product", "EXPR$2")
      .baselineValues("San Francisco", "Samsung", 300L)
      .baselineValues("San Francisco", "iPhone", 260L)
      .baselineValues("San Francisco", null, 560L)
      .baselineValues("San Jose", null, 650L)
      .baselineValues(null, "Samsung", 650L)
      .baselineValues(null, "iPhone", 560L)
      .baselineValues(null, null, 1210L)
      .baselineValues("San Jose", "Samsung", 350L)
      .baselineValues("San Jose", "iPhone", 300L)
      .go();
  }

  @Test
  public void testGroupingSetWithAggFunctionOnGroupColumn() throws Exception {
    String query = "select n_regionkey, count(n_regionkey) cnt from cp.\"tpch/nation.parquet\" group by rollup (n_regionkey);";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "cnt")
      .baselineValues(null, 25L)
      .baselineValues(0, 5L)
      .baselineValues(1, 5L)
      .baselineValues(2, 5L)
      .baselineValues(3, 5L)
      .baselineValues(4, 5L)
      .go();
  }

  @Test
  public void testGroupingSetWithAvgFunctionOnGroupColumn() throws Exception {
    String query = "select n_regionkey, avg(n_regionkey) \"avg\" from cp.\"tpch/nation.parquet\" group by rollup (n_regionkey);";
    test("explain plan for " + query);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "avg")
      .baselineValues(null, 2.0)
      .baselineValues(0, 0.0)
      .baselineValues(1, 1.0)
      .baselineValues(2, 2.0)
      .baselineValues(3, 3.0)
      .baselineValues(4, 4.0)
      .go();
  }

  @Test
  public void testGroupingSetWithCaseStmt() throws Exception {
    String query = "SELECT n_regionkey,\n"
      + "CASE WHEN n_name IN ('CANADA', 'RUSSIA') THEN 'COOL' ELSE 'Other' END\n"
      + "FROM cp.\"tpch/nation.parquet\"\n"
      + "GROUP BY GROUPING SETS (\n"
      + "(n_regionkey, CASE WHEN n_name IN ('CANADA', 'RUSSIA') THEN 'COOL' ELSE 'Other' END),\n"
      + "(n_regionkey),\n"
      + "()\n"
      + ")";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "EXPR$1")
      .baselineValues(null, null)
      .baselineValues(1, "Other")
      .baselineValues(1, "COOL")
      .baselineValues(0, "Other")
      .baselineValues(2, null)
      .baselineValues(4, null)
      .baselineValues(3, null)
      .baselineValues(0, null)
      .baselineValues(3, "Other")
      .baselineValues(2, "Other")
      .baselineValues(3, "COOL")
      .baselineValues(1, null)
      .baselineValues(4, "Other")
      .go();
  }

  @Test
  public void testGroupingSetWithCaseStmtAlias() throws Exception {
    String query = "SELECT n_regionkey,\n"
      + "CASE WHEN n_name IN ('CANADA', 'RUSSIA') THEN 'COOL' ELSE 'Other' END AS x\n"
      + "FROM cp.\"tpch/nation.parquet\"\n"
      + "GROUP BY GROUPING SETS (\n"
      + "(n_regionkey, x),\n"
      + "(n_regionkey),\n"
      + "()\n"
      + ")";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("n_regionkey", "x")
      .baselineValues(null, null)
      .baselineValues(1, "Other")
      .baselineValues(1, "COOL")
      .baselineValues(0, "Other")
      .baselineValues(2, null)
      .baselineValues(4, null)
      .baselineValues(3, null)
      .baselineValues(0, null)
      .baselineValues(3, "Other")
      .baselineValues(2, "Other")
      .baselineValues(3, "COOL")
      .baselineValues(1, null)
      .baselineValues(4, "Other")
      .go();
  }
}
