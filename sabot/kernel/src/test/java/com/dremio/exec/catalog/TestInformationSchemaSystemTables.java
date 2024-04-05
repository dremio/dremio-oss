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

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static com.dremio.test.DremioTest.DEFAULT_SABOT_CONFIG;

import com.dremio.exec.proto.SearchProtos;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotNode;
import com.dremio.service.catalog.ListSysCatalogsRequest;
import com.dremio.service.catalog.ListSysColumnsRequest;
import com.dremio.service.catalog.ListSysSchemasRequest;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test information schema system tables. */
public class TestInformationSchemaSystemTables {

  private final String userName = "dcstest";
  private SabotNode sabotNode;

  @Before
  public void setUp() throws Exception {
    ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
    sabotNode =
        new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
    User db =
        SimpleUser.newBuilder()
            .setUserName(userName)
            .setCreatedAt(System.currentTimeMillis())
            .setEmail("dcstest@dremio.com")
            .setFirstName("dcs")
            .setLastName("test")
            .build();
    sabotNode.getContext().getUserService().createUser(db);
  }

  @Test
  public void testListSysCatalogs() {
    Iterator<UserProtos.CatalogMetadata> catalogIterator =
        sabotNode
            .getContext()
            .getInformationSchemaServiceBlockingStub()
            .listSysCatalogs(ListSysCatalogsRequest.newBuilder().setUsername(userName).build());
    Assert.assertTrue(catalogIterator.hasNext());
    UserProtos.CatalogMetadata catalog = catalogIterator.next();
    Assert.assertEquals("DREMIO", catalog.getCatalogName());
    Assert.assertEquals("The internal metadata used by Dremio", catalog.getDescription());
    Assert.assertEquals("", catalog.getConnect());
  }

  @Test
  public void testListSysSchemas() {
    SearchProtos.SearchQuery.Equals equals1 =
        SearchProtos.SearchQuery.Equals.newBuilder()
            .setField("schema_name")
            .setStringValue("TEST")
            .build();
    SearchProtos.SearchQuery searchQuery1 =
        SearchProtos.SearchQuery.newBuilder().setEquals(equals1).build();
    SearchProtos.SearchQuery.Equals equals2 =
        SearchProtos.SearchQuery.Equals.newBuilder()
            .setField("schema_name")
            .setStringValue("INFORMATION_SCHEMA")
            .build();
    SearchProtos.SearchQuery searchQuery2 =
        SearchProtos.SearchQuery.newBuilder().setEquals(equals2).build();
    SearchProtos.SearchQuery searchQuery =
        SearchProtos.SearchQuery.newBuilder()
            .setOr(
                SearchProtos.SearchQuery.Or.newBuilder()
                    .addClauses(searchQuery1)
                    .addClauses(searchQuery2)
                    .build())
            .build();
    Iterator<UserProtos.SchemaMetadata> schemaMetadataIterator =
        sabotNode
            .getContext()
            .getInformationSchemaServiceBlockingStub()
            .listSysSchemas(
                ListSysSchemasRequest.newBuilder()
                    .setUsername(userName)
                    .setQuery(searchQuery)
                    .build());
    while (schemaMetadataIterator.hasNext()) {
      UserProtos.SchemaMetadata schemaMetadata = schemaMetadataIterator.next();
      Assert.assertEquals("INFORMATION_SCHEMA", schemaMetadata.getSchemaName());
    }
  }

  @Test
  public void testListSysColumns1() {
    SearchProtos.SearchQuery.Equals equals =
        SearchProtos.SearchQuery.Equals.newBuilder()
            .setField("table_name")
            .setStringValue("CATALOGS")
            .build();
    SearchProtos.SearchQuery searchQuery1 =
        SearchProtos.SearchQuery.newBuilder().setEquals(equals).build();
    SearchProtos.SearchQuery.Like like =
        SearchProtos.SearchQuery.Like.newBuilder()
            .setField("schema_name")
            .setPattern("INFORMATION_SCHEMA")
            .build();
    SearchProtos.SearchQuery searchQuery2 =
        SearchProtos.SearchQuery.newBuilder().setLike(like).build();
    SearchProtos.SearchQuery searchQuery =
        SearchProtos.SearchQuery.newBuilder()
            .setAnd(
                SearchProtos.SearchQuery.And.newBuilder()
                    .addClauses(searchQuery1)
                    .addClauses(searchQuery2)
                    .build())
            .build();
    Iterator<UserProtos.ColumnMetadata> columnMetadataIterator =
        sabotNode
            .getContext()
            .getInformationSchemaServiceBlockingStub()
            .listSysColumns(
                ListSysColumnsRequest.newBuilder()
                    .setUsername(userName)
                    .setQuery(searchQuery)
                    .build());
    Assert.assertEquals("CATALOG_NAME", columnMetadataIterator.next().getColumnName());
    Assert.assertEquals("CATALOG_DESCRIPTION", columnMetadataIterator.next().getColumnName());
    Assert.assertEquals("CATALOG_CONNECT", columnMetadataIterator.next().getColumnName());
  }

  @Test
  public void testListSysColumns2() {
    SearchProtos.SearchQuery.Equals equals =
        SearchProtos.SearchQuery.Equals.newBuilder()
            .setField("table_name")
            .setStringValue("TEST")
            .build();
    SearchProtos.SearchQuery searchQuery1 =
        SearchProtos.SearchQuery.newBuilder().setEquals(equals).build();
    SearchProtos.SearchQuery.Like like =
        SearchProtos.SearchQuery.Like.newBuilder()
            .setField("schema_name")
            .setPattern("INFORMATION_SCHEMA")
            .build();
    SearchProtos.SearchQuery searchQuery2 =
        SearchProtos.SearchQuery.newBuilder().setLike(like).build();
    SearchProtos.SearchQuery searchQuery =
        SearchProtos.SearchQuery.newBuilder()
            .setAnd(
                SearchProtos.SearchQuery.And.newBuilder()
                    .addClauses(searchQuery1)
                    .addClauses(searchQuery2)
                    .build())
            .build();
    Iterator<UserProtos.ColumnMetadata> columnMetadataIterator =
        sabotNode
            .getContext()
            .getInformationSchemaServiceBlockingStub()
            .listSysColumns(
                ListSysColumnsRequest.newBuilder()
                    .setUsername(userName)
                    .setQuery(searchQuery)
                    .build());
    Assert.assertEquals(0, Iterators.size(columnMetadataIterator));
  }
}
