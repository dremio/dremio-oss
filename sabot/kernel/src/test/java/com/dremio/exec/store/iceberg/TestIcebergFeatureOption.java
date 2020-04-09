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
package com.dremio.exec.store.iceberg;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;

// Tests that iceberg feature is disabled by default, and dis-allows iceberg operations.
public class TestIcebergFeatureOption extends BaseTestQuery {
  private static String TEST_SCHEMA = TEMP_SCHEMA;
  private static String ERROR_MESSAGE_SUBSTRING = "steps to enable the iceberg tables feature.";

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
  }

  @Test
  public void Insert() throws Exception {
    final String tableName = "nation_insert_auto_refresh";

    try (AutoCloseable ac = enableIcebergTables()) {
      final String ctasQuery =
        String.format(
          "CREATE TABLE %s.%s  "
            + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 2",
          TEST_SCHEMA,
          tableName);

      test(ctasQuery);
    }

    expectedEx.expectMessage(ERROR_MESSAGE_SUBSTRING);
    try {
      // mac stores mtime in units of sec. so, atleast this much time should elapse to detect the
      // change.
      Thread.sleep(1001);
      final String insertQuery =
        String.format(
          "INSERT INTO %s.%s "
            + "SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 3",
          TEST_SCHEMA, tableName);

      test(insertQuery);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void createEmpty() throws Exception {
    final String newTblName = "create_empty_auto_refresh";

    expectedEx.expectMessage(ERROR_MESSAGE_SUBSTRING);
    try {
      final String ctasQuery =
        String.format("CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))", TEST_SCHEMA, newTblName);

      test(ctasQuery);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }
}
