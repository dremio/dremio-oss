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
package com.dremio.exec.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test connection to external Deltalake Table
 */
public class ITHiveDeltalakeTable extends LazyDataGeneratingHiveTestBase {

  static FileSystem fs;
  static String testRootPath = "/tmp/deltalake_hive/";
  static Configuration conf;

  @BeforeClass
  public static void initFs() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");
    fs = FileSystem.get(conf);
    Path p = new Path(testRootPath);

    if (fs.exists(p)) {
      fs.delete(p, true);
    }

    fs.mkdirs(p);
    copyFromJar("deltalake/delta_extra", java.nio.file.Paths.get(testRootPath + "/delta_extra"));
    copyFromJar("deltalake/delta_parts", java.nio.file.Paths.get(testRootPath + "/delta_parts"));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
  }

  private static String resolveResource(String path) {
    return java.nio.file.Paths.get(testRootPath).resolve(path).toAbsolutePath().toString();
  }

  private static AutoCloseable withDeltaTable(String name, String schema, String path) throws Exception {
    String tablePath = resolveResource(path);
    String createQuery = String.format(
      "CREATE EXTERNAL TABLE %s %s STORED BY 'io.delta.hive.DeltaStorageHandler' LOCATION 'file://%s'",
      name, schema, tablePath);
    dataGenerator.executeDDL(createQuery);

    return () -> {
      String dropQuery = String.format("DROP TABLE IF EXISTS %s", name);
      dataGenerator.executeDDL(dropQuery);
    };
  }

  @Test
  public void testSelectQuery() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_extra", "(col1 INT, col2 STRING, extraCol INT)", "delta_extra")) {
      String query = "SELECT * FROM hive.delta_extra";
      testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "extraCol")
        .baselineValues(1, "abc", 2)
        .baselineValues(3, "xyz", 4)
        .baselineValues(5, "lmn", 6)
        .go();
    }
  }

  @Test
  public void testSelectSnapshotQuery() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_extra", "(col1 INT, col2 STRING, extraCol INT)", "delta_extra")) {
      test("SELECT * FROM hive.delta_extra");
      testBuilder()
        .sqlQuery("SELECT * FROM table(table_snapshot('hive.delta_extra'))")
        .unOrdered()
        .baselineColumns("committed_at", "snapshot_id", "parent_id", "operation", "manifest_list", "summary")
        .baselineValues(new LocalDateTime(1669702109914L, DateTimeZone.UTC), 0L, null, "CREATE OR REPLACE TABLE", null, null)
        .baselineValues(new LocalDateTime(1669702183904L, DateTimeZone.UTC), 1L, null, "WRITE", null, null)
        .go();
    }
  }

  @Test
  public void testSelectQueryWithFilter() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_extra", "(col1 INT, col2 STRING, extraCol INT)", "delta_extra")) {
      String query = "SELECT * FROM hive.delta_extra WHERE col1 = 1";
      testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "extraCol")
        .baselineValues(1, "abc", 2)
        .go();
    }
  }

  @Test
  public void testSelectPartitionsQuery() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_parts", "(number INT, partitionKey INT)", "delta_parts")) {
      String query = "SELECT * FROM hive.delta_parts";
      testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("number", "partitionKey")
        .baselineValues(1, 10)
        .baselineValues(2, 20)
        .go();
    }
  }
}
