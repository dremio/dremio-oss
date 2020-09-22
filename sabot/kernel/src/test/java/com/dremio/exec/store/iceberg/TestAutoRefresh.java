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
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;

public class TestAutoRefresh extends BaseTestQuery {
  private static String TEST_SCHEMA = "testRefresh";

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    addSubPathDfsPlugin();
  }

  // Create a sub-path with auto-promotion disabled.
  private static void addSubPathDfsPlugin() throws Exception {
    File storageBase = testFolder.newFolder("base");

    final CatalogServiceImpl pluginRegistry = (CatalogServiceImpl) getSabotContext().getCatalogService();
    final ManagedStoragePlugin msp = pluginRegistry.getManagedSource("dfs_test");
    StoragePluginId pluginId = msp.getId();
    InternalFileConf nasConf = pluginId.getConnectionConf();
    nasConf.path = storageBase.getPath();
    nasConf.mutability = SchemaMutability.ALL;

    // Add one configuration for testing when internal is true
    nasConf.isInternal = true;

    SourceConfig config = pluginId.getConfig();
    config.setId(null);
    config.setTag(null);
    config.setConfigOrdinal(null);
    config.setName("testRefresh");
    config.setMetadataPolicy(
      new MetadataPolicy()
        .setAuthTtlMs(0l)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setNamesRefreshMs(0l)
        .setDatasetDefinitionExpireAfterMs(Long.MAX_VALUE)
    );
    config.setConfig(nasConf.toBytesString());
    pluginRegistry.getSystemUserCatalog().createSource(config);
  }

  @Test
  public void CTAS() throws Exception {
    final String tableName = "nation_ctas_auto_refresh";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 2",
              TEST_SCHEMA,
              tableName);

      test(ctasQuery);

      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", TEST_SCHEMA, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(2L)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void Insert() throws Exception {
    final String tableName = "nation_insert_auto_refresh";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 2",
              TEST_SCHEMA,
              tableName);

      test(ctasQuery);

      // mac stores mtime in units of sec. so, atleast this much time should elapse to detect the
      // change.
      Thread.sleep(1001);
      final String insertQuery =
          String.format(
              "INSERT INTO %s.%s "
                  + "SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 3",
              TEST_SCHEMA, tableName);

      test(insertQuery);
      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", TEST_SCHEMA, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(5L)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void createEmpty() throws Exception {
    final String newTblName = "create_empty_auto_refresh";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
        String.format("CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))", TEST_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format("select count(*) as cnt from %s.%s", TEST_SCHEMA, newTblName);
      testBuilder()
        .sqlQuery(selectFromCreatedTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }
}
