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
package com.dremio.exec.store.dfs;

import java.math.BigDecimal;

import org.joda.time.LocalDateTime;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestParquetPartitionPruning extends PlanTestBase {

  private static final String DATE_PARTITION_TABLE =
          "[WORKING_PATH]/src/test/resources/parquet/parquet_datepartition";

  @Test
  public void testPartitionPruningOnTimestamp() throws Exception {
    String sql = "select ts from cp.\"parquet/singlets.parquet\" where ts = TIMESTAMP '2008-10-05 05:13:14.000'";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("ts")
      .baselineValues(JodaDateUtility.formatTimeStampMilli.parseLocalDateTime("2008-10-05 05:13:14.000"))
      .go();
  }

  @Test // DX-9408
  public void pruningBasedOnCurrentTimeStamp() throws Exception {
    final String query = "SELECT ts FROM cp.\"parquet/singlets.parquet\" WHERE ts > CURRENT_TIMESTAMP";

    testPlanMatchingPatterns(query, new String[]{"Empty"}, "Filter");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ts")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DX-9034
  public void pruningEverythingAcrossUnion() throws Exception {
    String sql = "select ts from cp.\"parquet/singlets.parquet\" where ts = TIMESTAMP '1908-10-05 05:13:14.000'" +
        "UNION ALL select ts from cp.\"parquet/singlets.parquet\" where ts = TIMESTAMP '1908-10-05 05:13:14.000'";

    testPlanMatchingPatterns(sql, new String[]{"Empty"}, "Filter");

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
    testPlanMatchingPatterns(q2, new String[]{"columns=\\[`a`\\]", "splits=\\[1\\]"}, "Filter");
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

  @Test
  public void pruningDecimalPartitionValues() throws Exception {
    try(AutoCloseable ac = withSystemOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE, true)) {
      // Create a parquet file containing just null and non-null values for decimal parititon column
      test("CREATE TABLE dfs_test.decimalPartitions HASH PARTITION BY (b) AS " +
          "SELECT * FROM cp.\"parquet/parquet_with_decimals.parquet\"");

      final String q1 = "SELECT a, b FROM dfs_test.decimalPartitions WHERE b != 7564.23423";
      testPlanMatchingPatterns(q1, new String[]{"columns=\\[`a`, `b`\\]", "splits=\\[1\\]"},
        "Filter");
      testBuilder()
          .sqlQuery(q1)
          .unOrdered()
          .baselineColumns("a", "b")
          .baselineValues(new BigDecimal("0.023423000000000"), new BigDecimal("34523.456233234000000"))
          .baselineValues(new BigDecimal("7564.234230000000000"), new BigDecimal("34523.456233234000000"))
          .go();

      final String q2 = "SELECT a, b FROM dfs_test.decimalPartitions WHERE b IS NULL";
      testPlanMatchingPatterns(q2, new String[]{"columns=\\[`a`\\]", "splits=\\[1\\]"}, "Filter");
      testBuilder()
          .sqlQuery(q2)
          .unOrdered()
          .baselineColumns("a", "b")
          .baselineValues(new BigDecimal("564.343400000000000"), null)
          .baselineValues(new BigDecimal("34523.456233234000000"), null)
          .go();

      final String q3 = "SELECT a, b FROM dfs_test.decimalPartitions WHERE b IS NOT NULL";
      testPlanMatchingPatterns(q3, new String[]{"columns=\\[`a`, `b`\\]", "splits=\\[2\\]"}, "Filter");
      testBuilder()
          .sqlQuery(q3)
          .unOrdered()
          .baselineColumns("a", "b")
          .baselineValues(null, new BigDecimal("7564.234230000000000"))
          .baselineValues(new BigDecimal("0.023423000000000"), new BigDecimal("34523.456233234000000"))
          .baselineValues(new BigDecimal("7564.234230000000000"), new BigDecimal("34523.456233234000000"))
          .baselineValues(new BigDecimal("9823.634000000000000"), new BigDecimal("7564.234230000000000"))
          .go();
    }
  }

  @Test
  public void testPartitionPruningOnDate() throws Exception {
    String q1 = "select date_col from dfs.\"" + DATE_PARTITION_TABLE + "\" where date_col = '1996-02-28'";
    testPlanMatchingPatterns(q1, new String[]{"splits=\\[1"}, "Filter");
    testBuilder()
            .sqlQuery(q1)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new LocalDateTime(1996, 2, 28, 0, 0))
            .go();

    String q2 = "select date_col from dfs.\"" + DATE_PARTITION_TABLE + "\" where date_col > '1996-01-29'";
    testPlanMatchingPatterns(q2, new String[]{"splits=\\[2"}, "Filter");
    testBuilder()
            .sqlQuery(q2)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new LocalDateTime(1996, 2, 28, 0, 0))
            .baselineValues(new LocalDateTime(1996, 3, 1, 0, 0))
            .go();

    String q3 = "select date_col from dfs.\"" + DATE_PARTITION_TABLE + "\" where date_col < '1996-03-01'";
    testPlanMatchingPatterns(q3, new String[]{"splits=\\[4"}, "Filter");
    testBuilder()
            .sqlQuery(q3)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new LocalDateTime(1957, 4, 9, 0, 0))
            .baselineValues(new LocalDateTime(1957, 6, 13, 0, 0))
            .baselineValues(new LocalDateTime(1996, 1, 29, 0, 0))
            .baselineValues(new LocalDateTime(1996, 2, 28, 0, 0))
            .go();
  }
}
