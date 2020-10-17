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

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestCTASWithOptions extends PlanTestBase {

  @Test
  public void csv() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testCsv " +
          "STORE AS (type => 'text', fieldDelimiter => ',') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM TABLE(\"dfs_test\".\"testCsv\"" +
              "(type => 'text', fieldDelimiter => ',', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues("0", "None")
          .baselineValues("1", "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testCsv");
    }
  }

  @Test
  public void csvWithCustomExtension() throws Exception {
    try {
      test("CREATE TABLE dfs_test.csvWithCustomExtension " +
          "STORE AS (type => 'text', fieldDelimiter => ',', outputExtension => 'myparquet') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM TABLE(\"dfs_test\".\"csvWithCustomExtension\"" +
              "(type => 'text', fieldDelimiter => ',', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues("0", "None")
          .baselineValues("1", "San Francisco")
          .go();
    } finally {
      // DROP TABLE doesn't support custom extensions
      //test("DROP TABLE dfs_test.csvWithCustomExtension");
    }
  }

  @Test
  public void csvUnordered() throws Exception {
    try {
      // order the options differently
      test("CREATE TABLE dfs_test.testCsvUnordered " +
          "STORE AS (fieldDelimiter => ',', type => 'text') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM TABLE(\"dfs_test\".\"testCsvUnordered\"" +
              "(type => 'text', fieldDelimiter => ',', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues("0", "None")
          .baselineValues("1", "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testCsvUnordered");
    }
  }

  @Test
  public void csvTabRecordDelimiter() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testCsvTabRecordDelimiter " +
          "STORE AS (type => 'text', fieldDelimiter => ',', lineDelimiter => '\t') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"testCsvTabRecordDelimiter\"" +
              "(type => 'text', fieldDelimiter => ',', lineDelimiter => '\t', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues("0", "None")
          .baselineValues("1", "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testCsvTabRecordDelimiter");
    }
  }

  @Test
  public void tsv() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testTsv STORE AS (type => 'teXt', fieldDelimiter => '\t') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"testTsv\"(type => 'text', fieldDelimiter => '\t', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues("0", "None")
          .baselineValues("1", "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testTsv");
    }
  }

  @Test
  public void json() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testJson " +
          "STORE AS (type => 'json') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"testJson\"(type => 'json'))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testJson");
    }
  }

  @Test
  public void jsonWithCustomExtension() throws Exception {
    try {
      test("CREATE TABLE dfs_test.jsonWithCustomExtension " +
          "STORE AS (type => 'json', outputExtension => 'myjson') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"jsonWithCustomExtension\"(type => 'json'))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .go();
    } finally {
      // DROP TABLE doesn't support custom extensions
      //test("DROP TABLE dfs_test.jsonWithCustomExtension");
    }
  }

  @Test
  public void parquet() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testParquet " +
          "STORE AS (type => 'parquet') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"testParquet\"(type => 'parquet'))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testParquet");
    }
  }

  @Test
  public void parquetWithCustomExtension() throws Exception {
    try {
      test("CREATE TABLE dfs_test.parquetWithCustomExtension " +
          "STORE AS (type => 'parquet', outputExtension => 'myparquet') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT * FROM " +
              "TABLE(\"dfs_test\".\"parquetWithCustomExtension\"(type => 'parquet'))")
          .unOrdered()
          .baselineColumns("region_id", "sales_city")
          .baselineValues(0L, "None")
          .baselineValues(1L, "San Francisco")
          .go();
    } finally {
      // DROP TABLE doesn't support custom extensions
      //test("DROP TABLE dfs_test.parquetWithCustomExtension");
    }
  }

  @Test
  public void parquetWithPartition() throws Exception {
    try {
      test("CREATE TABLE dfs_test.testParquetWithPartition " +
          "PARTITION BY (region_id) " +
          "STORE AS (type => 'parquet') " +
          "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2");

      testBuilder()
          .sqlQuery("SELECT dir0, region_id, sales_city FROM TABLE(\"dfs_test\".\"testParquetWithPartition\"(type => 'parquet'))")
          .unOrdered()
          .baselineColumns("dir0", "region_id", "sales_city")
          .baselineValues("0_0", 0L, "None")
          .baselineValues("1_1", 1L, "San Francisco")
          .go();
    } finally {
      test("DROP TABLE dfs_test.testParquetWithPartition");
    }
  }

  @Test
  public void negativeCaseUnsupportedType() throws Exception {
    final String query = "CREATE TABLE dfs_test.negativeCaseUnsupportedType " +
        "STORE AS (type => 'unknownFormat') " +
        "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2";
    errorMsgTestHelper(query, "unknown type unknownFormat, expected one of");
  }

  @Test
  public void negativeCaseUnknownOption() throws Exception {
    final String query = "CREATE TABLE dfs_test.negativeCaseUnknownOptions " +
        "STORE AS (type => 'json', unknownOption => 'sd') " +
        "AS SELECT region_id, sales_city FROM cp.\"region.json\" ORDER BY region_id LIMIT 2";
    errorMsgTestHelper(query, "Unknown storage option(s): {unknownOption=sd}");
  }

  @Test
  public void csvWithSingleWriter() throws Exception {
    try {
      final String query = "CREATE TABLE dfs_test.csvWithSingleWriter " +
          "STORE AS (type => 'text', fieldDelimiter => ',') " +
          "WITH SINGLE WRITER " +
          "AS SELECT region_id, count(*) cnt FROM cp.\"region.json\" GROUP BY region_id ORDER BY region_id LIMIT 2";

      test(query);

      testBuilder()
          .sqlQuery("SELECT * FROM TABLE(\"dfs_test\".\"csvWithSingleWriter\"" +
              "(type => 'text', fieldDelimiter => ',', extractHeader => true))")
          .unOrdered()
          .baselineColumns("region_id", "cnt")
          .baselineValues("0", "1")
          .baselineValues("1", "1")
          .go();
    } finally {
      test("DROP TABLE dfs_test.csvWithSingleWriter");
    }
  }

}
