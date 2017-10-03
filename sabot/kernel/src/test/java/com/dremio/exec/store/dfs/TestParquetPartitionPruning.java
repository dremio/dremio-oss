/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import org.apache.arrow.vector.util.DateUtility;
import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestParquetPartitionPruning extends PlanTestBase {

  @Test
  public void testPartitionPruningOnTimestamp() throws Exception {
    String sql = "select ts from cp.`parquet/singlets.parquet` where ts = TIMESTAMP '2008-10-05 05:13:14.000'";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("ts")
      .baselineValues(DateUtility.formatTimeStampMilli.parseLocalDateTime("2008-10-05 05:13:14.000"))
      .go();
  }

  @Test // DX-9034
  public void pruningEverythingAcrossUnion() throws Exception {
    String sql = "select ts from cp.`parquet/singlets.parquet` where ts = TIMESTAMP '1908-10-05 05:13:14.000'" +
        "UNION ALL select ts from cp.`parquet/singlets.parquet` where ts = TIMESTAMP '1908-10-05 05:13:14.000'";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("ts")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DX-9179
  public void pruningNullPartitionValues() throws Exception {
    // Create a parquet file containing just null and non-null values for parititon column
    test("CREATE TABLE dfs_test.pruningNullParts HASH PARTITION BY (b) AS " +
        "SELECT * FROM (VALUES(cast(1 as INT), cast(null as INT))) as T(a, b) UNION " +
        "SELECT * FROM (VALUES(cast(2 as INT), cast(1 as INT))) as T(a, b) UNION " +
        "SELECT * FROM (VALUES(cast(3 as INT), cast(2 as INT))) as T(a, b) UNION " +
        "SELECT * FROM (VALUES(cast(4 as INT), cast(null as INT))) as T(a, b) UNION " +
        "SELECT * FROM (VALUES(cast(5 as INT), cast(2 as INT))) as T(a, b) UNION " +
        "SELECT * FROM (VALUES(cast(6 as INT), cast(1 as INT))) as T(a, b)");

    final String q1 = "SELECT a, b FROM dfs_test.pruningNullParts WHERE b != 2";
    testPlanMatchingPatterns(q1, new String[]{"columns=\\[`a`, `b`\\]", "splits=\\[1\\]"}, "Filter");
    testBuilder()
        .sqlQuery(q1)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(2, 1)
        .baselineValues(6, 1)
        .go();

    final String q2 = "SELECT a, b FROM dfs_test.pruningNullParts WHERE b IS NULL";
    testPlanMatchingPatterns(q2, new String[]{"columns=\\[`a`, `b`\\]", "splits=\\[1\\]"}, "Filter");
    testBuilder()
        .sqlQuery(q2)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(4, null)
        .baselineValues(1, null)
        .go();

    final String q3 = "SELECT a, b FROM dfs_test.pruningNullParts WHERE b IS NOT NULL";
    testPlanMatchingPatterns(q3, new String[]{"columns=\\[`a`, `b`\\]", "splits=\\[2\\]"}, "Filter");
    testBuilder()
        .sqlQuery(q3)
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(2, 1)
        .baselineValues(3, 2)
        .baselineValues(5, 2)
        .baselineValues(6, 1)
        .go();
  }
}
