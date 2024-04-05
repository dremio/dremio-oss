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

import com.dremio.test.TemporarySystemProperties;
import org.junit.Rule;
import org.junit.Test;

public abstract class TestAbstractCaseLargeQueries extends PlanTestBase {

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  private static final int MAX_CASE_CONDITIONS = 500;

  @Test
  public void testLargeCaseStatement() throws Exception {
    final String sql =
        String.format(
            "select %s from cp.\"tpch/nation.parquet\" limit 5",
            projectLargeCase(MAX_CASE_CONDITIONS, 1));
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("nation")
        .baselineValues("ALGERIA_ABC")
        .baselineValues("argentina_xy1")
        .baselineValues("brazil_xy2")
        .baselineValues("canada_xy3")
        .baselineValues("egypt_xy4")
        .go();
  }

  protected String projectLargeCase(int level1Size, int level2Size) {
    StringBuilder sb = new StringBuilder();
    sb.append("CASE n_nationkey").append(System.lineSeparator());
    for (int i = level1Size; i > 0; i--) {
      sb.append("WHEN ")
          .append(i)
          .append(" THEN ")
          .append(getNextClause(level2Size, "_XY" + i))
          .append(System.lineSeparator());
    }
    sb.append("ELSE concat(n_name, '_ABC') END as nation").append(System.lineSeparator());
    return sb.toString();
  }

  protected String getNextClause(int level2Size, String extension) {
    final String[] regions = {"_UNKNOWN", "_AMERICAS", "_ASIA", "_EUROPE", "_ME"};
    if (level2Size <= 1) {
      return "lower(concat(n_name, '" + extension + "'))";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("CASE n_regionkey").append(System.lineSeparator());
      for (int i = level2Size; i > 0; i--) {
        final int regionIdx = (i >= regions.length) ? 0 : i;
        sb.append("WHEN ")
            .append(i)
            .append(" THEN ")
            .append((getNextClause(1, regions[regionIdx])))
            .append(System.lineSeparator());
      }
      sb.append("ELSE lower(concat(n_name, '_UNKNOWN')) END").append(System.lineSeparator());
      return sb.toString();
    }
  }
}
