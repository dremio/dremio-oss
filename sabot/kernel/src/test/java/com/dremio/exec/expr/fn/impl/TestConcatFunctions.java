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
package com.dremio.exec.expr.fn.impl;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.DremioGetObject;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.util.List;
import org.junit.Test;

/** Test the CONCAT function. */
public class TestConcatFunctions extends BaseTestQuery {
  /**
   * Test concat with different numbers of arguments of type VARCHAR.
   *
   * @throws Exception
   */
  @Test
  public void testConcat() throws Exception {
    concatVarcharHelper("s1s2", "s1", "s2");
    concatVarcharHelper("s1", "s1", null);
    concatVarcharHelper("s1s2s3s4", "s1", "s2", "s3", "s4");
    concatVarcharHelper(
        "s1s2s3s4s5s6s7s8s9s10", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10");
    concatVarcharHelper(
        "s1s2s4s5s6s8s9s10", "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10");
    concatVarcharHelper(
        "s1s2s4s5s6s8s9s10s11", "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10", "s11");
    concatVarcharHelper(
        "s1s2s4s5s6s8s9s10s11s12s13s14s15s16s17s18s19",
        "s1",
        "s2",
        null,
        "s4",
        "s5",
        "s6",
        null,
        "s8",
        "s9",
        "s10",
        "s11",
        "s12",
        "s13",
        "s14",
        "s15",
        "s16",
        "s17",
        "s18",
        "s19");
    concatVarcharHelper(
        "s1s2s4s5s6s8s9s10s11s12s13s14s15s16s17s18s19s20s21",
        "s1",
        "s2",
        null,
        "s4",
        "s5",
        "s6",
        null,
        "s8",
        "s9",
        "s10",
        "s11",
        "s12",
        "s13",
        "s14",
        "s15",
        "s16",
        "s17",
        "s18",
        "s19",
        "s20",
        "s21");
    concatVarcharHelper("", null, null, null, null, null, null, null, null, null, null);
    concatVarcharHelper(
        "", null, null, null, null, null, null, null, null, null, null, null, null, null);
  }

  @Test
  public void testConcatWithInts() throws Exception {
    concatHelper("-1000000000", "-1000000000");
    concatHelper("hello-1000000000world", "'hello'", "-1000000000", "'world'");
  }

  @Test
  public void testConcatWithDate() throws Exception {
    concatHelper("Date: 1995-02-20", "'Date: '", "CAST('1995-02-20' AS DATE)");
  }

  @Test
  public void testConcatWithDecimal() throws Exception {
    concatHelper("Decimal: 123456.789", "'Decimal: '", "CAST(123456.789 AS DECIMAL(9,3))");
  }

  @Test
  public void testConcatWithDouble() throws Exception {
    concatHelper("Double: 1.1234567890123457", "'Double: '", "CAST(1.1234567890123457 AS DOUBLE)");
    concatHelper(
        "Double: {1.1234567890123457}", "'Double: {'", "CAST(1.1234567890123457 AS DOUBLE)", "'}'");
  }

  /**
   * Test case using the parquet/alltypes.json data set to give a little more variation in our
   * testing. We do not query the json data directly, because in that path we do not enforce the
   * result of our type inference, and instead just default to a max-width VARCHAR.
   *
   * <p>We instead read the test data, and generate queries that concatenate the literals. For the
   * baseline query we cast to varchar, and for the test query we concatenate using the proper type.
   *
   * @throws Exception
   */
  @Test
  public void testConcatAllTypes() throws Exception {

    // These two arrays need to be kept aligned with each other, and must match the column in
    // alltypes.json.
    final String[] colNames = {
      "timestamp_col",
      "intervalday_col",
      "time_col",
      "bigint_col",
      "varbinary_col",
      "int_col",
      "bit_col",
      "float8_col",
      "date_col",
      "float4_col",
      "varchar_col"
    };
    final String[] typeNames = {
      "TIMESTAMP",
      "INTERVAL DAY",
      "TIME",
      "BIGINT",
      "VARBINARY",
      "INT",
      "BOOLEAN",
      "DOUBLE",
      "DATE",
      "FLOAT",
      "VARCHAR"
    };

    // First, read the test data.
    List<QueryDataBatch> allTypesStringData =
        testSqlWithResults(
            String.format(
                "SELECT %s FROM cp.\"/parquet/alltypes.json\"", String.join(", ", colNames)));
    RecordBatchLoader loader = new RecordBatchLoader(getTestAllocator());
    for (QueryDataBatch batch : allTypesStringData) {
      if (batch.getData() != null) {
        loader.load(batch.getHeader().getDef(), batch.getData());
        for (int i = 0; i < loader.getRecordCount(); ++i) {
          // For each row in the file, we generate a query that will return a single row with all
          // the columns in
          // alltypes.json.
          int colIdx = 0;
          StringBuilder baselineConcatQuery = new StringBuilder("SELECT ");
          StringBuilder testConcatQuery = new StringBuilder("SELECT ");
          for (VectorWrapper<?> w : loader) {
            Object value = DremioGetObject.getObject(w.getValueVector(), i);
            String valueAsStringLiteral = value != null ? "'" + value.toString() + "'" : "null";

            if (colIdx > 0) {
              baselineConcatQuery.append(',');
              testConcatQuery.append(',');
            }

            baselineConcatQuery.append(
                String.format(
                    "CONCAT('{', CAST(CAST(%s AS %s) AS VARCHAR), '}') %s",
                    valueAsStringLiteral, typeNames[colIdx], colNames[colIdx]));
            testConcatQuery.append(
                String.format(
                    "CONCAT('{', CAST(%s AS %s), '}') %s",
                    valueAsStringLiteral, typeNames[colIdx], colNames[colIdx]));
            ++colIdx;
          }

          testBuilder()
              .sqlQuery(testConcatQuery.toString())
              .unOrdered()
              .sqlBaselineQuery(baselineConcatQuery.toString())
              .build()
              .run();
        }
      }
      loader.clear();
      batch.release();
    }
  }

  /**
   * Execute a SELECT CONCAT query using the passed in arguments and compare it to the expected
   * string.
   *
   * @param expected The expected result of the query.
   * @param args A list of strings that can be used directly in a CONCAT call. Must be a valid SQL
   *     literal/expression.
   * @throws Exception
   */
  private void concatHelper(String expected, String... args) throws Exception {
    String query = "SELECT concat(" + String.join(", ", args) + ") c1";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("c1").baselineValues(expected).go();
  }

  /**
   * Execute a SELECT CONCAT query using the passed in arguments as string literals and compare it
   * to the expected string.
   *
   * @param expected The expected result of the query.
   * @param args A list of strings that will be represented as string literals in the CONCAT query.
   *     Nulls allowed.
   * @throws Exception
   */
  private void concatVarcharHelper(String expected, String... args) throws Exception {
    final StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT concat(");

    boolean first = true;
    for (String arg : args) {
      if (first) {
        first = false;
      } else {
        queryBuilder.append(", ");
      }
      if (arg == null) {
        queryBuilder.append("cast(null as varchar(200))");
      } else {
        queryBuilder.append("'" + arg + "'");
      }
    }

    queryBuilder.append(") c1 FROM (VALUES(1))");

    testBuilder()
        .sqlQuery(queryBuilder.toString())
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues(expected)
        .go();
  }
}
