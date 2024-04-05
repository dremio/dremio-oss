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
package com.dremio.exec.sql;

import com.dremio.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;

public class TestWithClause extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestWithClause.class);

  @Test
  public void withClause() throws Exception {
    test(
        "with alpha as (SELECT * FROM INFORMATION_SCHEMA.CATALOGS where CATALOG_NAME = 'DREMIO') \n"
            + "\n"
            + "select * from alpha");
  }

  @Test
  @Ignore
  public void withClauseWithAliases() throws Exception {
    test(
        "with alpha (x,y) as (SELECT CATALOG_NAME, CATALOG_DESCRIPTION FROM INFORMATION_SCHEMA.CATALOGS where CATALOG_NAME = 'DREMIO') \n"
            + "\n"
            + "select x, y from alpha");
  }

  @Test // DRILL-2318
  public void withClauseOrderBy() throws Exception {
    String query =
        "WITH x \n"
            + "AS (SELECT n_nationkey a1 \n"
            + "FROM  cp.\"tpch/nation.parquet\") \n"
            + "SELECT  x.a1 \n"
            + "FROM  x \n"
            + "ORDER BY x.a1 \n"
            + "limit 5";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a1")
        .baselineValues(0)
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .build()
        .run();
  }
}
