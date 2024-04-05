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

import com.dremio.common.expression.CompleteType;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableType;
import com.dremio.service.catalog.View;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceTestUtils;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.flatbuffers.FlatBufferBuilder;
import io.protostuff.ByteString;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test information schema catalog. */
public class TestInformationSchemaCatalog {
  private static final String DEFAULT_CATALOG = "DREMIO";
  private static final String SOURCE = "source";
  private static final String EMPTYSPACE = "emptyspace";
  private static final String SPACE = "space";
  private static final String FOLDER = "space.folder";
  private static final String SUBFOLDER = "space.folder.subfolder";
  private static final String PDS = "space.folder.pds";

  private LegacyKVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;
  private InformationSchemaCatalog catalog;

  @Before
  public void setUp() throws Exception {
    kvStoreProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();

    namespaceService = new NamespaceServiceImpl(kvStoreProvider, new CatalogStatusEventsImpl());

    NamespaceTestUtils.addSource(namespaceService, SOURCE);
    NamespaceTestUtils.addSpace(namespaceService, EMPTYSPACE);
    NamespaceTestUtils.addSpace(namespaceService, SPACE);
    NamespaceTestUtils.addFolder(namespaceService, FOLDER);
    NamespaceTestUtils.addFolder(namespaceService, SUBFOLDER);

    final Field field1 = CompleteType.INT.toField("a");
    final Field child1 = CompleteType.VARCHAR.toField("c");
    final Field field2 = CompleteType.struct(ImmutableList.of(child1)).toField("b");
    final org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(ImmutableList.of(field1, field2));
    final FlatBufferBuilder builder = new FlatBufferBuilder();
    schema.getSchema(builder);
    builder.finish(schema.getSchema(builder));
    NamespaceTestUtils.addPhysicalDS(namespaceService, PDS, builder.sizedByteArray());

    final String vds1 = "space.folder.vds1";
    NamespaceTestUtils.addDS(namespaceService, vds1, "select * from space.folder.pds");

    final String vds2 = "space.folder.subfolder.vds2";
    NamespaceTestUtils.addDS(namespaceService, vds2, "select * from space.folder.vds1");

    this.catalog = new InformationSchemaCatalogImpl(namespaceService, null, null);
  }

  @After
  public void tearDown() throws Exception {
    if (kvStoreProvider != null) {
      kvStoreProvider.close();
    }
  }

  @Test
  public void listCatalogs() {
    Assert.assertEquals(1, Iterators.size(catalog.listCatalogs(null)));
    Assert.assertEquals(DEFAULT_CATALOG, catalog.listCatalogs(null).next().getCatalogName());

    Assert.assertEquals("", catalog.listCatalogs(null).next().getCatalogConnect());

    Assert.assertEquals(
        "The internal metadata used by Dremio",
        catalog.listCatalogs(null).next().getCatalogDescription());
  }

  private static Schema schema(String schemaName) {
    return Schema.newBuilder()
        .setCatalogName(DEFAULT_CATALOG)
        .setSchemaName(schemaName)
        .setSchemaOwner("<owner>")
        .setSchemaType(SchemaType.SIMPLE)
        .setIsMutable(false)
        .build();
  }

  @Test
  public void listSchemata() {
    Assert.assertEquals(5, Iterators.size(catalog.listSchemata(null)));
    final Set<Schema> schemaSet1 =
        Sets.newHashSet(
            schema(SOURCE), schema(EMPTYSPACE), schema(SPACE), schema(FOLDER), schema(SUBFOLDER));
    Assert.assertEquals(schemaSet1, Sets.newHashSet(catalog.listSchemata(null)));
  }

  private static Table table(String parentName, String tableName, TableType tableType) {
    return Table.newBuilder()
        .setCatalogName(DEFAULT_CATALOG)
        .setSchemaName(parentName)
        .setTableName(tableName)
        .setTableType(tableType)
        .build();
  }

  @Test
  public void listTables() {
    Assert.assertEquals(3, Iterators.size(catalog.listTables(null)));
    final Table pdsTable = table(FOLDER, "pds", TableType.TABLE);
    final Table vds1Table = table(FOLDER, "vds1", TableType.VIEW);
    final Table vds2Table = table(SUBFOLDER, "vds2", TableType.VIEW);
    final Set<Table> tableSet1 = Sets.newHashSet(pdsTable, vds1Table, vds2Table);
    Assert.assertEquals(tableSet1, Sets.newHashSet(catalog.listTables(null)));
  }

  @Test
  public void testTableSchemaIsSubsetOfSchemaName() {
    final Set<String> schemaNameSet = Sets.newHashSet();
    catalog.listSchemata(null).forEachRemaining(e -> schemaNameSet.add(e.getSchemaName()));
    catalog
        .listTables(null)
        .forEachRemaining(e -> Assert.assertTrue(schemaNameSet.contains(e.getSchemaName())));
  }

  private static View view(String parentName, String tableName, String sql) {
    return View.newBuilder()
        .setCatalogName(DEFAULT_CATALOG)
        .setSchemaName(parentName)
        .setTableName(tableName)
        .setViewDefinition(sql)
        .build();
  }

  @Test
  public void listViews() {
    Assert.assertEquals(2, Iterators.size(catalog.listViews(null)));
    final View view1 = view(FOLDER, "vds1", "select * from space.folder.pds");
    final View view2 = view(SUBFOLDER, "vds2", "select * from space.folder.vds1");
    final Set<View> viewSet1 = Sets.newHashSet(view1, view2);
    Assert.assertEquals(viewSet1, Sets.newHashSet(catalog.listViews(null)));
  }

  @Test
  public void listColumns() {
    Assert.assertEquals(1, Iterators.size(catalog.listTableSchemata(null)));

    final Field field1 = CompleteType.INT.toField("a");
    final Field child1 = CompleteType.VARCHAR.toField("c");
    final Field field2 = CompleteType.struct(ImmutableList.of(child1)).toField("b");
    final Set<Field> topLevelFields = Sets.newHashSet(field1, field2);

    catalog
        .listTableSchemata(null)
        .forEachRemaining(
            tableSchema ->
                BatchSchema.deserialize(
                        ByteString.copyFrom(tableSchema.getBatchSchema().toByteArray()))
                    .iterator()
                    .forEachRemaining(field -> Assert.assertTrue(topLevelFields.contains(field))));
  }
}
