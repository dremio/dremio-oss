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

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.test.TemporarySystemProperties;
import org.junit.Rule;
import org.junit.Test;

public class IntersectTest extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IntersectTest.class);

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test // Simple Intersect over two scans
  public void testIntersectDistinct1() throws Exception {
    String query =
        "(select n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey > 1) intersect "
            + "(select r_regionkey from cp.\"tpch/region.parquet\" where r_regionkey < 4)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("")
        .baselineTypes(MinorType.INT)
        .baselineColumns("n_regionkey")
        .baselineValues(2)
        .baselineValues(3)
        .go();
  }

  @Test // More Intersect over two scans
  public void testIntersectDistinct2() throws Exception {
    String query =
        "(select n_name from cp.\"tpch/nation.parquet\" where n_regionkey = 2) union "
            + "(select n_name from cp.\"tpch/nation.parquet\" where n_regionkey in (1,2)) intersect "
            + "(select n_name from cp.\"tpch/nation.parquet\" where n_regionkey < 4)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("")
        .baselineTypes(MinorType.VARCHAR)
        .baselineColumns("n_name")
        .baselineValues("PERU")
        .baselineValues("INDIA")
        .baselineValues("VIETNAM")
        .baselineValues("ARGENTINA")
        .baselineValues("CANADA")
        .baselineValues("UNITED STATES")
        .baselineValues("BRAZIL")
        .baselineValues("INDONESIA")
        .baselineValues("JAPAN")
        .baselineValues("CHINA")
        .go();
  }
}
