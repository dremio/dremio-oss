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
package com.dremio.exec.catalog;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.users.SystemUser;

import io.grpc.StatusRuntimeException;


/**
 * This test tests the {@link VersionedDatasetAdapter#getTable(String)} interface
 */

public class TestVersionedDatasetAdapter extends BaseTestQuery {
  private static final String TEST_ICEBERG_TABLE = "iceberg_orders_table";
  private static final String TEST_SCHEMA = "dfs_test";
  private static final String dfsSchema = "dfs";
  private static final String testWorkingPath = TestTools.getWorkingPath();
  private static final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
  private static final String createEmptyTableQuery =
    String.format("CREATE TABLE %s.%s.%s (id int, name varchar, distance Decimal(38, 3))", "DDP",TEST_SCHEMA, TEST_ICEBERG_TABLE);
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    addSubPathDfsPlugin();
    createNonPartitionTable(createEmptyTableQuery);
  }

  @AfterClass
  public static void teardown() {
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), TEST_ICEBERG_TABLE));
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
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setNamesRefreshMs(0L)
        .setDatasetDefinitionExpireAfterMs(Long.MAX_VALUE)
    );
    config.setConfig(nasConf.toBytesString());
    pluginRegistry.getSystemUserCatalog().createSource(config);
  }

  @Ignore
  @Test
  public void TestBasicGetTable() {
    String nessieKey = "DDP." + TEST_SCHEMA + "." + TEST_ICEBERG_TABLE;
    VersionContext defaultVersionContext = VersionContext.ofBranch("main");
    NamespaceTable dremioTable = (NamespaceTable) getTableMetadataFromIceberg(nessieKey, defaultVersionContext);
    //Verify the TableMetadata members are populated.
    Assert.assertNotNull(dremioTable.getSchema());
    DatasetConfig tableMetadataConfig = dremioTable.getDatasetConfig();
    Assert.assertNotNull(tableMetadataConfig);
    Assert.assertEquals(tableMetadataConfig.getName(), TEST_SCHEMA + "." + TEST_ICEBERG_TABLE);
    Assert.assertNotNull(tableMetadataConfig.getReadDefinition());
    Assert.assertNotNull(dremioTable.getDataset().getSplitsKey());
    Assert.assertEquals(dremioTable.getDataset().getApproximateRecordCount(), 0);
    Assert.assertEquals(dremioTable.getDataset().getSplitCount(), 1);
  }

  @Ignore
  @Test
  public void TestBasicGetTableBadKeys() {
    String badKey1 = "DDP." + TEST_SCHEMA + TEST_ICEBERG_TABLE; //Invalid source key after DDP prefix
    VersionContext defaultVersionContext = VersionContext.ofBranch("main");
    try {
      getTableMetadataFromIceberg(badKey1, defaultVersionContext);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Failure while getting handle to iceberg table from source"));
    }

    String badKey2 = "DDX." + TEST_SCHEMA + TEST_ICEBERG_TABLE; //Invalid prefix
    try {
      getTableMetadataFromIceberg(badKey2, defaultVersionContext);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid DDP key"));
    }

    String badKey3 = "DDP" ; // Only DDP and no source or table name
    try {
      getTableMetadataFromIceberg(badKey3, defaultVersionContext);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid DDP key"));
    }

  }

  @Ignore
  @Test
  public void TestBasicGetTableVersionContextWithRegularKey() {
    String key = TEST_SCHEMA + TEST_ICEBERG_TABLE; //Invalid source key after DDP prefix
    VersionContext defaultVersionContext = VersionContext.ofBranch("main");
    try {
      getTableMetadataFromIceberg(key, defaultVersionContext);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid DDP key"));
    }
  }

  @Ignore
  @Test
  public void TestBasicGetTableEmptyVersion() {
    //Should work - and default to main branch
    String key = "DDP." + TEST_SCHEMA + "." + TEST_ICEBERG_TABLE;
    NamespaceTable dremioTable = (NamespaceTable) getTableMetadataFromIceberg(key, VersionContext.NOT_SPECIFIED);

    //Verify the TableMetadata members are populated.
    Assert.assertNotNull(dremioTable.getSchema());
    DatasetConfig tableMetadataConfig = dremioTable.getDatasetConfig();
    Assert.assertNotNull(tableMetadataConfig);
    Assert.assertEquals(tableMetadataConfig.getName(), TEST_SCHEMA + "." + TEST_ICEBERG_TABLE);
  }

  @Ignore
  @Test
  public void TestPassingInvalidBranchVersion() {
    String key = "DDP." + TEST_SCHEMA + "." + TEST_ICEBERG_TABLE;
    VersionContext badVersionContext = VersionContext.ofBranch("xyzbranch");
    //Pass an invalid branch
    Throwable throwable = Assert.assertThrows(StatusRuntimeException.class, () -> getTableMetadataFromIceberg(key, badVersionContext));
    Assert.assertTrue(throwable.getMessage().contains("Named reference 'xyzbranch' not found"));
  }

  private DremioTable getTableMetadataFromIceberg(String key, VersionContext versionContext) {
      OptionManager optionManager = getSabotContext().getOptionManager();

      final CatalogServiceImpl pluginRegistry = (CatalogServiceImpl) getSabotContext().getCatalogService();
      final ManagedStoragePlugin msp = pluginRegistry.getManagedSource("dfs_test");

      final VersionedDatasetAdapter versionedDatasetAdapter = VersionedDatasetAdapter.newBuilder()
        .setVersionedTableKey(key)
        .setVersionContext(versionContext)
        .setOptionManager(optionManager)
        .setStoragePlugin(msp.getPlugin())
        .build();
      return versionedDatasetAdapter.getTable(SystemUser.SYSTEM_USERNAME);

  }

  private static void createNonPartitionTable(String createQuery) throws Exception {
    try (AutoCloseable ignored = enableIcebergTables()) {
      test(createQuery);
    }
  }

}
