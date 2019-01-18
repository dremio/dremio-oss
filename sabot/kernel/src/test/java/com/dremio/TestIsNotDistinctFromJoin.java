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
package com.dremio;

import org.junit.Test;

public class TestIsNotDistinctFromJoin extends PlanTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestIsNotDistinctFromJoin.class);

  @Test
  public void testINDF() throws Exception {
    testPlanMatchingPatterns("SELECT \"t0\".\"l_returnflag\" AS \"returnflag\",\n" +
      "  SUM( \"t1\".\"X_measure__2\") AS \"sum\"\n" +
      "FROM ( \n" +
      "  SELECT l_orderkey,\n" +
      "    l_returnflag\n" +
      "  FROM cp.\"tpch/lineitem.parquet\" l\n" +
      "  WHERE ( l_linestatus = 'F')\n" +
      "  GROUP BY l_orderkey, l_returnflag\n" +
      ") \"t0\"\n" +
      "  INNER JOIN ( \n" +
      "  SELECT l_orderkey,\n" +
      "    SUM( ( CASE WHEN ( ( {fn LCASE( l_comment )} = 'all') ) THEN l_extendedprice ELSE CAST( NULL AS INTEGER) END)) AS \"X_measure__2\"\n" +
      "  FROM cp.\"tpch/lineitem.parquet\" l\n" +
      "  GROUP BY l_orderkey\n" +
      ") \"t1\" ON ( ( \"t0\".\"l_orderkey\" = \"t1\".\"l_orderkey\") OR ( ( \"t0\".\"l_orderkey\" IS NULL) AND ( \"t1\".\"l_orderkey\" IS NULL)))\n" +
      "GROUP BY \"t0\".\"l_returnflag\"\n", new String[] {"IS NOT DISTINCT FROM"});
  }
}
