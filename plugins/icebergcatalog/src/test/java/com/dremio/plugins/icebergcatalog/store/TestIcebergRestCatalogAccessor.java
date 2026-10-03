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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestIcebergRestCatalogAccessor {
  private IcebergRestCatalogAccessor icebergRestCatalogAccessor;
  @Mock private Supplier<Catalog> catalogSupplier;
  @Mock private OptionManager optionManager;

  @BeforeEach
  public void setup() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS)).thenReturn(10L);
    icebergRestCatalogAccessor =
        new IcebergRestCatalogAccessor(catalogSupplier, optionManager, null, true);
  }

  @Test
  public void testCheckState() throws Exception {
    when(catalogSupplier.get()).thenReturn(mock(RESTCatalog.class));
    icebergRestCatalogAccessor.checkState();
    verify(catalogSupplier).get();
  }

  @Test
  public void testCatalogCacheReplaceSwapsBeforeClosingThePreviousClient() throws Exception {
    RESTCatalog stale = mock(RESTCatalog.class);
    RESTCatalog fresh = mock(RESTCatalog.class);
    ExpiringCatalogCache cache = new ExpiringCatalogCache(() -> stale, 60, TimeUnit.SECONDS);
    Assertions.assertSame(stale, cache.get());
    // A concurrent reader never sees an empty cache while the stale client is being closed.
    doAnswer(
            invocation -> {
              Assertions.assertSame(fresh, cache.getIfPresent());
              Assertions.assertSame(fresh, cache.get());
              return null;
            })
        .when(stale)
        .close();

    cache.replace(fresh);

    verify(stale).close();
    Assertions.assertSame(fresh, cache.get());
    cache.close();
    verify(fresh).close();
    Assertions.assertNull(cache.getIfPresent());
  }

  @Test
  public void testGetViewLocationUriFromUserSubmittedLocation() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "table");

    String userSubmittedLocation = "file:///catalog/some/custom/table/location";
    Assertions.assertEquals(
        userSubmittedLocation,
        icebergRestCatalogAccessor.getViewLocation(userSubmittedLocation, tableIdentifier));
  }

  @Test
  public void testGetViewLocationUriFromExistingTable() {
    String rootCatalogLocationUri = "file:///catalog";
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "table");
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Set.of(tableIdentifier), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);

    String expectedLocation = "file:///catalog/namespace/table";
    Assertions.assertEquals(
        expectedLocation, icebergRestCatalogAccessor.getViewLocation(null, tableIdentifier));
  }

  @Test
  public void testCreateFolderHappyPath() {
    String rootCatalogLocationUri = "file:///catalog";
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Collections.emptySet(), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertEquals(
        Map.of("location", "file:///catalog/folder2"),
        icebergRestCatalogAccessor.createFolder(
            List.of("sourceName", "folder2"), Collections.emptyMap()));
    Assertions.assertEquals(1, catalog.getNumNamespacesCreated());
  }

  @Test
  public void testUpdateFolderHappyPath() {
    String rootCatalogLocationUri = "file:///catalog";
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Collections.emptySet(), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertEquals(
        Map.of("location", "file:///catalog/folder2"),
        icebergRestCatalogAccessor.createFolder(
            List.of("sourceName", "folder2"), Collections.emptyMap()));
    Map<String, String> updatedProperties = Map.of("location", "file:///bucket2/folder2");
    Assertions.assertEquals(
        updatedProperties,
        icebergRestCatalogAccessor.updateFolder(
            List.of("sourceName", "folder2"), updatedProperties, Set.of()));
  }

  @Test
  public void testDropFolderHappyPath() {
    String rootCatalogLocationUri = "file:///catalog";
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Collections.emptySet(), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertEquals(
        Map.of("location", "file:///catalog/folder2"),
        icebergRestCatalogAccessor.createFolder(
            List.of("sourceName", "folder2"), Collections.emptyMap()));
    Assertions.assertTrue(icebergRestCatalogAccessor.dropFolder(List.of("sourceName", "folder2")));
    Assertions.assertEquals(Collections.emptyList(), catalog.listNamespaces());
    Assertions.assertEquals(1, catalog.getNumNamespacesCreated());
  }

  @Test
  public void testNamespaceExists() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    catalog.createNamespace(Namespace.of("namespace", "folder"));
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertTrue(
        icebergRestCatalogAccessor.namespaceExists(
            ImmutableList.of("catalog", "namespace", "folder")));
  }

  @Test
  public void testNamespaceDoesNotExist() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    catalog.createNamespace(Namespace.of("namespace", "folder"));
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertFalse(
        icebergRestCatalogAccessor.namespaceExists(
            ImmutableList.of("catalog", "namespace", "folder1")));
  }

  @Test
  public void testListDatasetIdentifiersFromRoot() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace"));
    catalog.createNamespace(Namespace.of("namespace", "folder"));
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("namespace", "folder"), "table1");
    catalog.createTable(table1, null);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(ImmutableList.of("gibberish"));
    Assertions.assertEquals(1, listedDatasets.size());
    Assertions.assertEquals(table1, listedDatasets.toArray()[0]);
  }

  @Test
  public void testListDatasetIdentifiersFromNamespace() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace"));
    catalog.createNamespace(Namespace.of("namespace", "folder"));
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("namespace", "folder"), "table1");
    catalog.createTable(table1, null);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(
            ImmutableList.of("gibberish", "namespace"));
    Assertions.assertEquals(1, listedDatasets.size());
    Assertions.assertEquals(table1, listedDatasets.toArray()[0]);
  }

  @Test
  public void testListDatasetIdentifiersFromSubNamespace() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace", "folder"));
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("namespace", "folder"), "table1");
    catalog.createTable(table1, null);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(
            ImmutableList.of("gibberish", "namespace", "folder"));
    Assertions.assertEquals(1, listedDatasets.size());
    Assertions.assertEquals(table1, listedDatasets.toArray()[0]);
  }

  @Test
  public void testListDatasetIdentifiersWithViewsWithNoViews() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace", "folder"));
    TableIdentifier view1 = TableIdentifier.of(Namespace.of("namespace", "folder"), "view1");
    catalog.buildView(view1);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(ImmutableList.of("gibberish"));
    Assertions.assertEquals(0, listedDatasets.size());
  }

  @Test
  public void testListDatasetIdentifiersWithViewsHappyPath() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace"));
    TableIdentifier view1 = TableIdentifier.of(Namespace.of("namespace"), "view1");
    catalog.buildView(view1);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(ImmutableList.of("namespace"));
    Assertions.assertEquals(1, listedDatasets.size());
    Assertions.assertEquals(view1, listedDatasets.toArray()[0]);
  }

  @Test
  public void testListDatasetIdentifiersWithMultipleLevels() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(new HashSet<>(), "");

    catalog.createNamespace(Namespace.of("namespace"));
    catalog.createNamespace(Namespace.of("namespace", "folder"));
    TableIdentifier view1 = TableIdentifier.of(Namespace.of("namespace", "folder"), "view1");
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("namespace"), "table1");
    catalog.buildView(view1);
    catalog.createTable(table1, null);

    when(catalogSupplier.get()).thenReturn(catalog);

    Set<TableIdentifier> listedDatasets =
        icebergRestCatalogAccessor.listDatasetIdentifiers(ImmutableList.of("gibberish"));
    Assertions.assertEquals(2, listedDatasets.size());
    Assertions.assertTrue(listedDatasets.contains(view1));
    Assertions.assertTrue(listedDatasets.contains(table1));
  }

  @Test
  public void testCreateView() {
    Schema schema = mock(Schema.class);
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    when(catalogSupplier.get()).thenReturn(catalog);
    when(optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    Assertions.assertNotNull(
        icebergRestCatalogAccessor.createView(
            ImmutableList.of("catalog", "folder1", "v1"),
            null,
            new ArrayList<>(),
            schema,
            "select * from t1"));
    Assertions.assertTrue(
        icebergRestCatalogAccessor.datasetExists(ImmutableList.of("catalog", "folder1", "v1")));
  }

  @Test
  public void testCreateViewFailed() {
    Schema schema = mock(Schema.class);
    when(optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    when(catalogSupplier.get()).thenReturn(catalog);
    catalog.createNamespace(Namespace.of("folder1"));
    Assertions.assertNotNull(
        icebergRestCatalogAccessor.createView(
            ImmutableList.of("catalog", "folder1", "v1"),
            null,
            new ArrayList<>(),
            schema,
            "select * from t1"));
    Assertions.assertTrue(
        icebergRestCatalogAccessor.datasetExists(ImmutableList.of("catalog", "folder1", "v1")));
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            icebergRestCatalogAccessor.createView(
                ImmutableList.of("catalog", "folder1", "v1"),
                null,
                new ArrayList<>(),
                schema,
                "select * from t1"));
  }

  @Test
  public void testDropView() {
    Schema schema = mock(Schema.class);
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertNotNull(
        icebergRestCatalogAccessor.createView(
            ImmutableList.of("catalog", "folder1", "v1"),
            null,
            new ArrayList<>(),
            schema,
            "select * from t1"));
    icebergRestCatalogAccessor.dropView(ImmutableList.of("catalog", "folder1", "v1"));
    Assertions.assertFalse(
        icebergRestCatalogAccessor.datasetExists(ImmutableList.of("catalog", "folder1", "v1")));
  }

  @Test
  public void testDropViewFailed() {
    MockIcebergCatalog catalog = new MockIcebergCatalog(Collections.emptySet(), "");
    when(catalogSupplier.get()).thenReturn(catalog);
    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> icebergRestCatalogAccessor.dropView(ImmutableList.of("catalog", "folder1", "v1")));
  }

  @Test
  public void testGetFolderStreamHappyPath() {
    String rootCatalogLocationUri = "file:///catalog";
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Collections.emptySet(), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder0"), Collections.emptyMap());
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder1"), Collections.emptyMap());
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder2"), Collections.emptyMap());
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder2", "folder3"), Collections.emptyMap());
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder2", "folder3", "folder4"), Collections.emptyMap());
    Iterator<IcebergNamespaceWithProperties> iterator =
        icebergRestCatalogAccessor.getFolderStream().iterator();
    int numFolders = 0;
    while (iterator.hasNext()) {
      iterator.next();
      numFolders++;
    }
    Assertions.assertEquals(5, numFolders);
  }

  @Test
  public void testGetFolderFailure() {
    String rootCatalogLocationUri = "file:///catalog";
    MockIcebergCatalog catalog =
        new MockIcebergCatalog(Collections.emptySet(), rootCatalogLocationUri);
    when(catalogSupplier.get()).thenReturn(catalog);
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder0"), Collections.emptyMap());
    icebergRestCatalogAccessor.createFolder(
        List.of("sourceName", "folder0", "folder1"), Collections.emptyMap());
    catalog.setShouldFailGetterActions(true);
    Iterator<IcebergNamespaceWithProperties> iterator =
        icebergRestCatalogAccessor.getFolderStream().iterator();
    Assertions.assertFalse(iterator.hasNext());
  }
}
