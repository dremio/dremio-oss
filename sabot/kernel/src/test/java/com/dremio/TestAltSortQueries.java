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

import java.nio.file.Paths;

import org.junit.Test;

import com.dremio.common.util.TestTools;

public class TestAltSortQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestAltSortQueries.class);

  private static final String SAMPLE_DATA_PATH;
  static {
    SAMPLE_DATA_PATH = Paths.get(TestTools.getWorkingPath()).getParent().getParent().resolve("sample-data").toAbsolutePath().toString();
  }

  @Test
  public void testOrderBy() throws Exception{
    test("select R_REGIONKEY " +
         String.format("from dfs.\"%s/region.parquet\" ", SAMPLE_DATA_PATH) +
         "order by R_REGIONKEY");
  }

  @Test
  public void testOrderBySingleFile() throws Exception{
    test("select R_REGIONKEY " +
         String.format("from dfs.\"%s/regionsSF/\" ", SAMPLE_DATA_PATH) +
         "order by R_REGIONKEY");
  }

  @Test
  public void testSelectWithLimit() throws Exception{
    test("select employee_id,  first_name, last_name from cp.\"employee.json\" order by employee_id limit 5 ");
  }

  // TODO - This is currently passing but I think that it is still in error,
  // the new verification for this test was written against the output that was previously not being checked
  // It looks like there is an off by one error in the results, see the baseline file for the current results
  @Test
  public void testSelectWithLimitOffset() throws Exception{
    testBuilder()
        .sqlQuery("select employee_id,  first_name, last_name from cp.\"employee.json\" order by employee_id limit 5 offset 10 ")
        .ordered()
        .csvBaselineFile("sort/testSelectWithLimitOffset.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        .build().run();
  }

  @Test
  public void testJoinWithLimit() throws Exception{
    test("SELECT\n" +
        "  nations.N_NAME,\n" +
        "  regions.R_NAME\n" +
        "FROM\n" +
        String.format("  dfs.\"%s/nation.parquet\" nations\n", SAMPLE_DATA_PATH) +
        "JOIN\n" +
        String.format("  dfs.\"%s/region.parquet\" regions\n", SAMPLE_DATA_PATH) +
        "  on nations.N_REGIONKEY = regions.R_REGIONKEY" +
        " order by regions.R_NAME, nations.N_NAME " +
        " limit 5");
  }

  /**
   * Test for DX-48015 a column aliased as the same name
   * as the original column in a group by expression
   * returns an error that it's not being grouped.
   * @throws Exception
   */
  @Test
  public void testColumnAliasWithGroupBy() throws Exception{
    test("SELECT\n" +
        "  DATE_TRUNC('day', hire_date) AS hire_date,\n" +
        "       position_id AS position_id,\n" +
        "       sum(salary) AS salary\n" +
        "FROM  cp.\"employee.json\"\n" +
        "WHERE hire_date >= TO_DATE('1987-06-17', 'YYYY-MM-DD')\n" +
        "  AND hire_date < TO_DATE('1989-09-21', 'YYYY-MM-DD')\n" +
        "GROUP BY DATE_TRUNC('day', hire_date),\n" +
        "         position_id\n" +
        "ORDER BY salary");
  }

}
