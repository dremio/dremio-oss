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
package com.dremio.exec.store.dfs.system;

import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME;
import static com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSystemIcebergTableStoragePlugin extends BaseTestQuery {
  private static HadoopCatalog hadoopCatalog = null;
  private static SystemIcebergTablesStoragePlugin storagePlugin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setupDefaultTestCluster();
    storagePlugin = getCatalogService().getSource(SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);
    hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    hadoopCatalog.initialize(
        "hadoop",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION, storagePlugin.getConfig().getPath().toString()));
  }

  @Test
  public void testTableUpgradePaths() {
    // Simulate tables created by previous version of plugin, older schema, that used hadoop catalog
    Table fileHistoryRawTable =
        hadoopCatalog.createTable(
            TableIdentifier.of(COPY_FILE_HISTORY_TABLE_NAME),
            CopyFileHistoryTableSchemaProvider.getSchema(1));
    fileHistoryRawTable.updateProperties().set("test_property", "value").commit();
    fileHistoryRawTable.newFastAppend().commit(); // at-least one snapshot
    assertThat(storagePlugin.isTableExists(fileHistoryRawTable.location())).isFalse();

    Table jobHistoryRawTable =
        hadoopCatalog.createTable(
            TableIdentifier.of(COPY_JOB_HISTORY_TABLE_NAME),
            CopyJobHistoryTableSchemaProvider.getSchema(1));
    jobHistoryRawTable.updateProperties().set("test_property", "value").commit();
    jobHistoryRawTable.newFastAppend().commit();
    assertThat(storagePlugin.isTableExists(jobHistoryRawTable.location())).isFalse();

    storagePlugin.createEmptySystemIcebergTablesIfNotExists();
    storagePlugin.updateSystemIcebergTables();

    // Validate it's the same table, that got migrated
    assertThat(storagePlugin.isTableExists(fileHistoryRawTable.location())).isTrue();

    // Access tables from the storage plugin, and ensure it contains properties from the raw version
    Table fileHistoryTableFromPlugin = storagePlugin.getTable(fileHistoryRawTable.location());
    assertThat(fileHistoryTableFromPlugin.properties().get("test_property")).isEqualTo("value");

    Table jobHistoryTableFromPlugin = storagePlugin.getTable(jobHistoryRawTable.location());
    assertThat(jobHistoryTableFromPlugin.properties().get("test_property")).isEqualTo("value");

    // Validate schema upgraded to V2
    assertThat(fileHistoryTableFromPlugin.schema().toString())
        .isEqualTo(CopyFileHistoryTableSchemaProvider.getSchema(2).toString());
    assertThat(jobHistoryTableFromPlugin.schema().toString())
        .isEqualTo(CopyJobHistoryTableSchemaProvider.getSchema(2).toString());

    // Validate partition spec upgraded to V2
    assertThat(fileHistoryTableFromPlugin.spec().isPartitioned()).isTrue();
    assertThat(fileHistoryTableFromPlugin.spec().toString())
        .isEqualTo(CopyFileHistoryTableSchemaProvider.getPartitionSpec(2).toString());
    assertThat(jobHistoryTableFromPlugin.spec().isPartitioned()).isTrue();
    assertThat(jobHistoryTableFromPlugin.spec().toString())
        .isEqualTo(CopyJobHistoryTableSchemaProvider.getPartitionSpec(2).toString());

    // Validate schema property is upgraded to V2
    assertThat(
            fileHistoryTableFromPlugin
                .properties()
                .get(SystemIcebergTableMetadata.SCHEMA_VERSION_PROPERTY))
        .isEqualTo("2");

    // Updates in the plugin tables will not reflect in the hadoop variant since commits to the new
    // catalog aren't visible at hadoop layer
    fileHistoryTableFromPlugin.updateProperties().set("test_property", "new_value").commit();
    jobHistoryTableFromPlugin.updateProperties().set("test_property", "new_value").commit();

    fileHistoryRawTable.refresh();
    jobHistoryRawTable.refresh();
    assertThat(fileHistoryRawTable.properties().get("test_property")).isEqualTo("value");
    assertThat(jobHistoryRawTable.properties().get("test_property")).isEqualTo("value");
  }

  @Test
  public void isSupportedTablePath() {
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "copy_job_history")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "copy_file_history")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "copy_errors_history")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "COPY_JOB_HISTORY")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "COPY_FILE_HISTORY")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "COPY_ERRORS_HISTORY")));
    assertTrue(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "CopY_ErrORs_History")));
    assertFalse(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "copy_jobs_history")));
    assertFalse(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "copy_file_history_tbl")));
    assertFalse(storagePlugin.isSupportedTablePath(Arrays.asList("sys", "COPY_ERROR_HISTORY")));
  }
}
