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

import org.junit.Test;

public class TestCorrelation extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCorrelation.class);

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
}
