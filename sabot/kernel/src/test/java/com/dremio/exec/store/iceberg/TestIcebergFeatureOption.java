
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;

// Tests that iceberg feature is disabled by default, and dis-allows iceberg operations.
public class TestIcebergFeatureOption extends BaseTestQuery {
  private static String TEST_SCHEMA = TEMP_SCHEMA;

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
  }

  @Test
  public void Insert() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      final String tableName = "nation_insert_auto_refresh";

      try (AutoCloseable ac = enableIcebergTables()) {
        final String ctasQuery =
          String.format(
            "CREATE TABLE %s.%s  "
              + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 2",
            testSchema,
            tableName);

        test(ctasQuery);
      }

      try {
        final String insertQuery =
          String.format(
            "INSERT INTO %s.%s "
              + "SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 3",
            testSchema, tableName);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThatThrownBy(() -> test(insertQuery))
            .satisfiesAnyOf(t -> assertThat(t.getMessage()).contains(
                "UNSUPPORTED_OPERATION ERROR: Source [dfs_test_hadoop] does not support DML operations"),
              t -> assertThat(t.getMessage()).contains(
                "UNSUPPORTED_OPERATION ERROR: Source [dfs_test] does not support DML operations")));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void createEmpty() throws Exception {
    final String newTblName = "create_empty_auto_refresh";

    try {
      final String ctasQuery =
        String.format("CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))", TEST_SCHEMA, newTblName);

      assertThatThrownBy(() -> test(ctasQuery))
        .hasMessageContaining("UNSUPPORTED_OPERATION ERROR: Source [dfs_test] does not support CREATE TABLE. Please use correct catalog");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }
}
