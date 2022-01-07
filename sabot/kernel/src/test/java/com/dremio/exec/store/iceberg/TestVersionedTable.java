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
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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

public class TestVersionedTable extends BaseTestQuery {
  private static final String TEST_SCHEMA = "testVersioned";

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
    config.setName("testVersioned");
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

  public void resetClient() throws Exception {
    updateClient((Properties) null);
  }

  @Ignore
  @Test
  public void createIcebergEmptyAndSelectNoVersionSet() throws Exception {
    final String emptyTblName = "create_iceberg_empty_table1" + System.currentTimeMillis();
    resetClient();

    final String createEmptyTableQuery =
      String.format("CREATE TABLE %s.%s.%s (id int, name varchar, distance Decimal(38, 3))", "DDP", TEST_SCHEMA, emptyTblName);

    test(createEmptyTableQuery);


    try (AutoCloseable u = enableUseSyntax()) {
      // Select from the created Iceberg table by reading directly from Nessie/Iceberg
      final String selectFromEmptyTable = String.format("select count(*) as cnt from %s.%s.%s", "DDP", TEST_SCHEMA, emptyTblName);
      test(selectFromEmptyTable);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), emptyTblName));
    }
  }

  @Ignore
  @Test
  public void createIcebergEmptyNonexistentBranchSet() throws Exception {
    final String emptyTblName = "create_iceberg_empty_table1";
    resetClient();
    try (AutoCloseable u = enableUseSyntax()) {
      final String useBranchMain = "use branch xyz";
      test(useBranchMain);
      final String createEmptyTableQuery =
        String.format("CREATE TABLE %s.%s.%s (id int, name varchar, distance Decimal(38, 3))", "DDP", TEST_SCHEMA, emptyTblName);

      errorMsgTestHelper(createEmptyTableQuery, "SYSTEM ERROR: StatusRuntimeException: NOT_FOUND: Named reference 'xyz' not found");
    }
  }

  @Ignore
  @Test
  public void createIcebergEmptyAndSelect() throws Exception {
    final String emptyTblName = "create_iceberg_empty_table1" + System.currentTimeMillis();
    final String ctasTblName = "nation_ctas_iceberg";

    final String createEmptyTableQuery =
      String.format("CREATE TABLE %s.%s.%s (id int, name varchar, distance Decimal(38, 3))", "DDP", TEST_SCHEMA, emptyTblName);

    test(createEmptyTableQuery);


    try (AutoCloseable u = enableUseSyntax()) {
      final String useBranchMain = "use branch main";
      test(useBranchMain);
      // Select from the created Iceberg table by reading directly from Nessie/Iceberg
      final String selectFromEmptyTable = String.format("select count(*) as cnt from %s.%s.%s", "DDP", TEST_SCHEMA, emptyTblName);
      testBuilder()
        .sqlQuery(selectFromEmptyTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), emptyTblName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), ctasTblName));
    }
  }

  @Ignore
  @Test
  public void CTASFromIcebergTable() throws Exception {
    final String tableName = "nation_plain_iceberg";
    final String versionedTableName = "nation_versioned_iceberg";

    final String ctasPlainIcebergCreate =
      String.format(
        "CREATE TABLE %s.%s.%s  "
          + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 2",
        "DDP",
        TEST_SCHEMA,
        tableName);

    test(ctasPlainIcebergCreate);


    final String ctasVersionedIcebergCreate =
      String.format(
        "CREATE TABLE %s.%s.%s  "
          + " AS SELECT n_nationkey, n_regionkey from %s.%s.%s limit 2",
        "DDP",
        TEST_SCHEMA,
        versionedTableName,
        "DDP",
        TEST_SCHEMA,
        tableName);

    test(ctasVersionedIcebergCreate);

    try (AutoCloseable u = enableUseSyntax()) {
      final String useBranchMain = "use branch main";
      test(useBranchMain);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s.%s", "DDP", TEST_SCHEMA, versionedTableName))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), versionedTableName));
    }
  }

  @Ignore
  @Test
  public void createIcebergInsertAndSelect() throws Exception {
    final String emptyTblName = "create_iceberg_insert_table";

    final String createEmptyTableQuery =
      String.format("CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))", TEST_SCHEMA, emptyTblName);

    test(createEmptyTableQuery);


    try (AutoCloseable u = enableUseSyntax()) {
      final String useBranchMain = "use branch main";
      test(useBranchMain);
      // Select from the created Iceberg table by reading directly from Nessie/Iceberg
      final String selectFromEmptyTable = String.format("select count(*) as cnt from %s.%s.%s", "DDP", TEST_SCHEMA, emptyTblName);
      testBuilder()
        .sqlQuery(selectFromEmptyTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .build()
        .run();

      final String insertQuery =
        String.format(
          "INSERT INTO %s.%s values (1, 'street1', 20), (2, 'street2', 21), (3, 'street3', 22) ",
          TEST_SCHEMA, emptyTblName);

      test(insertQuery);

      testBuilder()
        .sqlQuery(selectFromEmptyTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(3L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), emptyTblName));
    }
  }

}

