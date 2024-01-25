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

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;

public class TestCorrelation extends PlanTestBase {

  @Test  // DRILL-2962
  public void testScalarAggCorrelatedSubquery() throws Exception {
    String query = ""
        + "SELECT count(*) AS cnt\n"
        + "FROM cp.\"tpch/nation.parquet\" n1\n"
        + "WHERE n1.n_nationkey  > (\n"
        + "  SELECT avg(n2.n_regionkey) * 4\n"
        + "  FROM cp.\"tpch/nation.parquet\" n2\n"
        + "  WHERE n1.n_regionkey = n2.n_nationkey\n"
        + ")";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(17L)
      .build()
      .run();
  }

  @Test  // DRILL-2949
  public void testScalarAggAndFilterCorrelatedSubquery() throws Exception {
    String query = ""
        + "SELECT count(*) as cnt\n"
        + "FROM cp.\"tpch/nation.parquet\" n1, cp.\"tpch/region.parquet\" r1\n"
        + "WHERE n1.n_regionkey = r1.r_regionkey\n"
        + "  AND r1.r_regionkey < 3\n"
        + "  AND n1.n_nationkey  > ("
        + "    SELECT avg(n2.n_regionkey) * 4\n"
        + "    FROM cp.\"tpch/nation.parquet\" n2\n"
        + "    WHERE n1.n_regionkey = n2.n_nationkey\n"
        + ")\n";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(11L)
      .build()
      .run();
  }

  @Test
  public void testCorrelatedQuery() throws Exception {
    String query = ""
        + "SELECT full_name, employee_id\n"
        + "FROM cp.\"employee.json\" e1\n"
        + "WHERE EXISTS (\n"
        + "  SELECT *\n"
        + "  FROM cp.\"employee.json\" e2\n"
        + "  WHERE e1.employee_id=e2.employee_id\n"
        + ")";
    String baselineQuery = ""
        + "SELECT full_name, employee_id\n"
        + "FROM cp.\"employee.json\" e1\n";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery(baselineQuery)
        .build()
        .run();
  }

  @Test // DX-20910
  public void testCorrelatedQueryWithLimit() throws Exception {
    String query = ""
        + "SELECT full_name, employee_id\n"
        + "FROM cp.\"employee.json\" e1\n"
        + "WHERE EXISTS (\n"
        + "  SELECT *\n"
        + "  FROM cp.\"employee.json\" e2\n"
        + "  WHERE e1.employee_id=e2.employee_id\n"
        + ")\n"
        + "ORDER BY employee_id\n"
        + "LIMIT 1";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("full_name", "employee_id")
        .baselineValues("Sheri Nowmer", 1L)
        .build()
        .run();
  }

  @Test
  public void testNotIn() throws Exception {
    String query = ""
      + "SELECT full_name\n"
      + "FROM cp.\"employee.json\" e1\n"
      + "WHERE e1.employee_id NOT IN (\n"
      + "  SELECT e2.employee_id\n"
      + "  FROM cp.\"employee.json\" e2\n"
      + "  WHERE e1.employee_id=e2.employee_id\n"
      + ")\n"
      + "ORDER BY employee_id\n"
      + "LIMIT 1";
    DremioTestWrapper testWrapper = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("full_name")
      .expectsEmptyResultSet()
      .build();
    try(AutoCloseable c= withOption(PlannerSettings.USE_SQL_TO_REL_SUB_QUERY_EXPANSION, true)) {
      testWrapper.run();
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains("(java.lang.AssertionError) contains $cor0"));
    }
    testWrapper.run();
  }

  @Test
  public void testAny() throws Exception {
    String query = ""
      + "SELECT c\n"
      + "FROM (VALUES (1), (2), (3), (4)) AS t(c)\n"
      + "WHERE c = ANY(1, 2)";
    DremioTestWrapper testWrapper = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("c")
      .baselineValues(1L)
      .baselineValues(2L)
      .build();
    try(AutoCloseable c= withOption(PlannerSettings.USE_SQL_TO_REL_SUB_QUERY_EXPANSION, true)) {
      testWrapper.run();
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains("SOME is only supported if expand = false"));
    }
    testWrapper.run();
  }

  @Test
  public void testWinMagicFail() throws Exception{
    String query = ""
      + "WITH \n"
      + "  t1 AS (\n"
      + "      SELECT *\n"
      + "      FROM (VALUES ('a', 2), ('b', 4), ('c', 6)) AS t1(t1_id, m1)),\n"
      + "  t2 AS (\n"
      + "      SELECT *\n"
      + "      FROM (VALUES (1), (2), (3)) AS t2(m2)),\n"
      + "  t3 AS (\n"
      + "      SELECT *\n"
      + "      FROM (VALUES (2, 1, 'a'), (2, 3, 'a'), (1, 3, 'b')) AS t3(m1, m2, t1_id))\n"
      + "SELECT t1.*, t2.*\n"
      + "FROM t1\n"
      + "LEFT JOIN t2 ON t1.m1 = (\n"
      + "    SELECT MAX(t3.m1)\n"
      + "    FROM t3\n"
      + "    WHERE t1.t1_id = t3.t1_id AND t2.m2 < t3.m2)";


    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("t1_id", "m1", "m2")
      .baselineValues("a", 2L, 1L)
      .baselineValues("a", 2L, 2L)
      .baselineValues("b", 4L, null)
      .baselineValues("c", 6L, null)
      .build()
      .run();
  }

  @Test
  public void testInnerJoinWithMultipleConditionsOnSameCorrelatedVariable() throws Exception{
    String query = ""
      + "SELECT t1.t1_id, t2_id, t3_id\n"
      + "FROM (VALUES ('t1.a'), ('t1.b'), ('t1.c')) AS t1(t1_id)\n"
      + "LEFT JOIN LATERAL (\n"
      + "    SELECT *\n"
      + "    FROM (\n"
      + "      SELECT *\n"
      + "      FROM (VALUES ('t2.a', 't1.a'), ('t2.b', 't1.a'), ('t2.c', 't1.b')) AS t2(t2_id, t1_id)\n"
      + "      WHERE t1.t1_id = t2.t1_id),\n"
      + "    ("
      + "      SELECT *\n"
      + "      FROM (VALUES ('t3.a', 't1.a'), ('t3.b', 't1.a'), ('t3.c', 't1.b')) AS t3(t3_id, t1_id)\n"
      + "      WHERE t1.t1_id = t3.t1_id))\n"
      + "  ON TRUE\n"
      + "ORDER BY t1.t1_id, t2_id, t3_id";


    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("t1_id", "t2_id", "t3_id")
      .baselineValues("t1.a", "t2.a", "t3.a")
      .baselineValues("t1.a", "t2.a", "t3.b")
      .baselineValues("t1.a", "t2.b", "t3.a")
      .baselineValues("t1.a", "t2.b", "t3.b")
      .baselineValues("t1.b", "t2.c", "t3.c")
      .baselineValues("t1.c", null, null)
      .build()
      .run();
  }

}
