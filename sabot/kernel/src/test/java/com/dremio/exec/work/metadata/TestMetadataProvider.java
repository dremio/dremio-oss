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
package com.dremio.exec.work.metadata;

import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_CONNECT;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_DESCR;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.proto.UserProtos.CatalogMetadata;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.GetSchemasResp;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.SchemaMetadata;
import com.dremio.exec.proto.UserProtos.TableMetadata;

/**
 * Tests for metadata provider APIs.
 */
public class TestMetadataProvider extends BaseTestQuery {

  @BeforeClass
  public static void setupMetadata() throws Exception {
    test("SELECT * from cp.\"tpch/customer.parquet\"");
  }

  @Test
  public void catalogs() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS"); // SQL equivalent

    GetCatalogsResp resp = client.getCatalogs(null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS " +
    //    "WHERE CATALOG_NAME LIKE '%DRE%' ESCAPE '\\'"); // SQL equivalent
    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setPattern("%DRE%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilterNegative() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS
    //     WHERE CATALOG_NAME LIKE '%DRIj\\\\hgjh%' ESCAPE '\\'"); // SQL equivalent

    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setPattern("%DRIj\\%hgjh%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(0, catalogs.size());
  }

  @Test
  @Ignore
  public void schemas() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(9, schemas.size());

    Set<String> expectedSchemaNames = new HashSet<>();
    expectedSchemaNames.add("INFORMATION_SCHEMA");
    expectedSchemaNames.add("cp.default");
    expectedSchemaNames.add("dfs.default");
    expectedSchemaNames.add("dfs.root");
    expectedSchemaNames.add("dfs.tmp");
    expectedSchemaNames.add("dfs_test.default");
    expectedSchemaNames.add("dfs_test.home");
    expectedSchemaNames.add("dfs_test.tmp");
    expectedSchemaNames.add("sys");

    Iterator<SchemaMetadata> iterator = schemas.iterator();
    while (iterator.hasNext()) {
      String schemeName = iterator.next().getSchemaName();
      assertTrue(expectedSchemaNames.contains(schemeName));
      expectedSchemaNames.remove(schemeName);
    }
    assertEquals(expectedSchemaNames.size(), 0);

  }

  @Test
  public void schemasWithSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%y%'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, LikeFilter.newBuilder().setPattern("%y%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(2, schemas.size());

    verifySchema("sys", schemas.get(0));
    verifySchema("sys.cache", schemas.get(1));
  }

  @Test
  public void schemasWithSchemaNameFilterAndUnderscore() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%y%' ESCAPE '\'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, LikeFilter.newBuilder().setPattern("INFORMATION\\_SCHEMA").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();

    assertEquals(1, schemas.size());

    verifySchema("INFORMATION_SCHEMA", schemas.get(0));
  }

  @Test
  @Ignore
  public void schemasWithCatalogNameFilterAndSchemaNameFilter() throws Exception {

    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA " +
    //    "WHERE CATALOG_NAME LIKE '%RI%' AND SCHEMA_NAME LIKE '%y%'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(
        LikeFilter.newBuilder().setPattern("%RI%").build(),
        LikeFilter.newBuilder().setPattern("%dfs_test%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(3, schemas.size());

    Iterator<SchemaMetadata> iterator = schemas.iterator();
    verifySchema("dfs_test.default", iterator.next());
    verifySchema("dfs_test.home", iterator.next());
    verifySchema("dfs_test.tmp", iterator.next());
  }

  @Test
  public void tables() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\""); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(23, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("INFORMATION_SCHEMA", "CATALOGS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "SCHEMATA", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "TABLES", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "VIEWS", iterator.next());
    verifyTable("sys", "boot", iterator.next());
    verifyTable("sys", "dependencies", iterator.next());
    verifyTable("sys", "fragments", iterator.next());
    verifyTable("sys", "materializations", iterator.next());
    verifyTable("sys", "memory", iterator.next());
    verifyTable("sys", "nodes", iterator.next());
    verifyTable("sys", "options", iterator.next());
    verifyTable("sys", "reflections", iterator.next());
    verifyTable("sys", "refreshes", iterator.next());
    verifyTable("sys", "services", iterator.next());
    verifyTable("sys", "slicing_threads", iterator.next());
    verifyTable("sys", "threads", iterator.next());
    verifyTable("sys", "version", iterator.next());
    verifyTable("sys.cache", "datasets", iterator.next());
    verifyTable("sys.cache", "mount_points", iterator.next());
    verifyTable("sys.cache", "objects", iterator.next());
    verifyTable("sys.cache", "storage_plugins", iterator.next());
  }

  @Test
  public void tablesWithTableFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_TYPE IN ('TABLE')"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, Arrays.asList("TABLE")).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(1, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("cp", "tpch/customer.parquet", iterator.next());
  }

  @Test
  public void tablesWithSystemTableFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_TYPE IN ('SYSTEM_TABLE')"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, Arrays.asList("SYSTEM_TABLE")).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(22, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("INFORMATION_SCHEMA", "CATALOGS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "SCHEMATA", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "TABLES", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "VIEWS", iterator.next());
    verifyTable("sys", "boot", iterator.next());
    verifyTable("sys", "dependencies", iterator.next());
    verifyTable("sys", "fragments", iterator.next());
    verifyTable("sys", "materializations", iterator.next());
    verifyTable("sys", "memory", iterator.next());
    verifyTable("sys", "nodes", iterator.next());
    verifyTable("sys", "options", iterator.next());
    verifyTable("sys", "reflections", iterator.next());
    verifyTable("sys", "refreshes", iterator.next());
    verifyTable("sys", "services", iterator.next());
    verifyTable("sys", "slicing_threads", iterator.next());
    verifyTable("sys", "threads", iterator.next());
    verifyTable("sys", "version", iterator.next());
    verifyTable("sys.cache", "datasets", iterator.next());
    verifyTable("sys.cache", "mount_points", iterator.next());
    verifyTable("sys.cache", "objects", iterator.next());
    verifyTable("sys.cache", "storage_plugins", iterator.next());
  }

  @Test
  public void tablesWithTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null,
        LikeFilter.newBuilder().setPattern("%o%").build(), null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(13, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("INFORMATION_SCHEMA", "CATALOGS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", iterator.next());

    verifyTable("sys", "boot", iterator.next());
    verifyTable("sys", "materializations", iterator.next());
    verifyTable("sys", "memory", iterator.next());
    verifyTable("sys", "nodes", iterator.next());
    verifyTable("sys", "options", iterator.next());
    verifyTable("sys", "reflections", iterator.next());
    verifyTable("sys", "version", iterator.next());
    verifyTable("sys.cache", "mount_points", iterator.next());
    verifyTable("sys.cache", "objects", iterator.next());
    verifyTable("sys.cache", "storage_plugins", iterator.next());
  }

  @Test
  public void tablesWithTableNameFilterAndSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" " +
    //    "WHERE TABLE_SCHEMA LIKE '%N\\_S%' ESCAPE '\\' AND TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null,
        LikeFilter.newBuilder().setPattern("%N\\_S%").setEscape("\\").build(),
        LikeFilter.newBuilder().setPattern("%o%").build(),
        null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    Iterator<TableMetadata> iterator = tables.iterator();
    assertEquals(2, tables.size());
    verifyTable("INFORMATION_SCHEMA", "CATALOGS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", iterator.next());
  }

  @Test
  public void columns() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"); // SQL equivalent

    final GetColumnsResp resp1 = client.getColumns(null, null, null, null).get();
    assertEquals(RequestStatus.OK, resp1.getStatus());

    final List<ColumnMetadata> columns1 = resp1.getColumnsList();
    assertEquals(192, columns1.size());
    assertTrue("incremental update column shouldn't be returned",
      columns1.stream().noneMatch(input -> input.getColumnName().equals(IncrementalUpdateUtils.UPDATE_COLUMN)));
  }

  @Test
  public void columnsWithColumnNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null, null,
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(21, columns.size());


    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("INFORMATION_SCHEMA", "COLUMNS", "ORDINAL_POSITION", iterator.next());
    verifyColumn("INFORMATION_SCHEMA", "COLUMNS", "NUMERIC_PRECISION", iterator.next());
    verifyColumn("INFORMATION_SCHEMA", "COLUMNS", "NUMERIC_PRECISION_RADIX", iterator.next());
    verifyColumn("INFORMATION_SCHEMA", "COLUMNS", "DATETIME_PRECISION", iterator.next());
    verifyColumn("INFORMATION_SCHEMA", "COLUMNS", "INTERVAL_PRECISION", iterator.next());

    verifyColumn("cp", "tpch/customer.parquet", "c_phone", iterator.next());

    verifyColumn("sys", "dependencies", "dependency_path", iterator.next());
    verifyColumn("sys", "materializations", "data_partitions", iterator.next());
    verifyColumn("sys", "materializations", "last_refresh_from_pds", iterator.next());
    verifyColumn("sys", "memory", "fabric_port", iterator.next());
    verifyColumn("sys", "nodes", "user_port", iterator.next());
    verifyColumn("sys", "nodes", "fabric_port", iterator.next());
    verifyColumn("sys", "services", "user_port", iterator.next());
    verifyColumn("sys", "services", "fabric_port", iterator.next());
    verifyColumn("sys", "slicing_threads", "fabric_port", iterator.next());
    verifyColumn("sys", "threads", "fabric_port", iterator.next());
    verifyColumn("sys.cache", "datasets", "storage_plugin_name", iterator.next());
    verifyColumn("sys.cache", "mount_points", "mount_point_path", iterator.next());
    verifyColumn("sys.cache", "mount_points", "mount_point_id", iterator.next());
    verifyColumn("sys.cache", "storage_plugins", "storage_plugin_name", iterator.next());
    verifyColumn("sys.cache", "storage_plugins", "storage_plugin_id", iterator.next());
  }

  @Test
  public void columnsWithColumnNameFilterAndTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    //     WHERE TABLE_NAME LIKE '%nodes' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null,
        LikeFilter.newBuilder().setPattern("%des").build(),
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(2, columns.size());

    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("sys", "nodes", "user_port", iterator.next());
    verifyColumn("sys", "nodes", "fabric_port", iterator.next());
  }

  @Test
  public void columnsWithAllSupportedFilters() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
    //    "TABLE_CATALOG LIKE '%MIO' AND TABLE_SCHEMA LIKE 'sys' AND " +
    //    "TABLE_NAME LIKE '%nodes' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(
        LikeFilter.newBuilder().setPattern("%MIO").build(),
        LikeFilter.newBuilder().setPattern("sys").build(),
        LikeFilter.newBuilder().setPattern("%des").build(),
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();

    assertEquals(2, columns.size());

    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("sys", "nodes", "user_port", iterator.next());
    verifyColumn("sys", "nodes", "fabric_port", iterator.next());
  }

  /** Helper method to verify schema contents */
  private static void verifySchema(String schemaName, SchemaMetadata schema) {
    assertEquals(IS_CATALOG_NAME, schema.getCatalogName());
    assertEquals(schemaName, schema.getSchemaName());
  }

  /** Helper method to verify table contents */
  private static void verifyTable(String schemaName, String tableName, TableMetadata table) {
    assertEquals(IS_CATALOG_NAME, table.getCatalogName());
    assertEquals(schemaName, table.getSchemaName());
    assertEquals(tableName, table.getTableName());
  }

  /** Helper method to verify column contents */
  private static void verifyColumn(String schemaName, String tableName, String columnName, ColumnMetadata column) {
    assertEquals(IS_CATALOG_NAME, column.getCatalogName());
    assertEquals(schemaName, column.getSchemaName());
    assertEquals(tableName, column.getTableName());
    assertEquals(columnName, column.getColumnName());
  }


}
