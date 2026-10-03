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
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED;
import static com.dremio.plugins.icebergcatalog.store.AbstractRestCatalogAccessor.DEFAULT_BASE_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.ViewDatasetHandle;
import com.dremio.connector.metadata.options.ForceUpdateOption;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.InMemoryCatalog;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRestCatalogAccessor {

  @Mock private RESTCatalog mockRestCatalog;
  @Mock private BaseView mockBaseView;
  @Mock private ViewOperations viewOperations;
  @Mock private ViewMetadata mockViewMetadata;
  @Mock private OptionManager mockOptionManager;
  @Mock private Supplier<Catalog> invalidSupplierCatalog;
  @Mock private Supplier<Catalog> validSupplierCatalog;

  private CatalogAccessor restCatalogAccessor;
  private CatalogAccessor restCatalogAccessorWithViewSupport;
  private static final long CATALOG_EXPIRY_SECONDS = 2L;

  @Before
  public void setup() {
    when(validSupplierCatalog.get()).thenReturn(mockRestCatalog);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS)).thenReturn(10L);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(10L);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS))
        .thenReturn(CATALOG_EXPIRY_SECONDS);

    when(mockBaseView.operations()).thenReturn(viewOperations);
    when(viewOperations.current()).thenReturn(mockViewMetadata);

    restCatalogAccessor =
        new IcebergRestCatalogAccessor(validSupplierCatalog, mockOptionManager, null, false);
    restCatalogAccessorWithViewSupport =
        new IcebergRestCatalogAccessor(validSupplierCatalog, mockOptionManager, null, false);
  }

  @After
  public void teardown() throws Exception {
    restCatalogAccessor = null;
  }

  @Test
  public void testDatasetExists() {
    when(mockRestCatalog.tableExists(TableIdentifier.of("b", "c"))).thenReturn(true);
    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testDatasetDoesNotExists() {
    when(mockRestCatalog.tableExists(any())).thenReturn(false);
    assertFalse(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testDatasetExistsThrowsNullPointerExceptionForNullList() {
    assertThatThrownBy(() -> restCatalogAccessor.datasetExists(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testClose() throws Exception {
    restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c"));
    restCatalogAccessor.close();
    verify(((Closeable) mockRestCatalog), times(1)).close();
  }

  @Test
  public void testCatalogSupplierCalledAfterExpiry() throws Exception {
    when(mockRestCatalog.tableExists(any())).thenReturn(true);

    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));

    Thread.sleep(TimeUnit.SECONDS.toMillis(CATALOG_EXPIRY_SECONDS + 1));
    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
  }

  private void testNSAllowList(
      Set<Namespace> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces,
      List<Integer> expectedTableNameNums) {
    RESTCatalog catalog = mock(RESTCatalog.class);
    // -A
    //  |-tab0
    //  |-tab1
    //  |-a
    //  | |-tab2
    //  | |-alpha
    //  | |   |-tab3
    //  | |   |-tab4
    //  | |
    //  | |-beta
    //  |     |-tab5
    //  |
    //  |-b
    //    |-gamma
    //        |-tab6
    // -B
    //  |-c
    //  | |-delta
    //  |     |-tab7
    //  |-d
    //    |-tab8
    //    |-epsilon
    //        |-tab9

    when(catalog.listNamespaces(any(Namespace.class)))
        .then(
            invocationOnMock -> {
              Namespace argNs = invocationOnMock.getArgument(0);
              if (argNs.equals(Namespace.empty())) {
                return Lists.newArrayList(Namespace.of("A"), Namespace.of("B"));
              } else if (argNs.equals(Namespace.of("A"))) {
                return Lists.newArrayList(Namespace.of("A", "a"), Namespace.of("A", "b"));
              } else if (argNs.equals(Namespace.of("B"))) {
                return Lists.newArrayList(Namespace.of("B", "c"), Namespace.of("B", "d"));
              } else if (argNs.equals(Namespace.of("A", "a"))) {
                return Lists.newArrayList(
                    Namespace.of("A", "a", "alpha"), Namespace.of("A", "a", "beta"));
              } else if (argNs.equals(Namespace.of("A", "b"))) {
                return Lists.newArrayList(Namespace.of("A", "b", "gamma"));
              } else if (argNs.equals(Namespace.of("B", "c"))) {
                return Lists.newArrayList(Namespace.of("B", "c", "delta"));
              } else if (argNs.equals(Namespace.of("B", "d"))) {
                return Lists.newArrayList(Namespace.of("B", "d", "epsilon"));
              } else {
                return Lists.newArrayList();
              }
            });

    when(catalog.listTables(any(Namespace.class)))
        .then(
            invocationOnMock -> {
              Namespace argNs = invocationOnMock.getArgument(0);
              List<String> tables = Lists.newArrayList();
              if (argNs.equals(Namespace.of("A"))) {
                tables.add("tab0");
                tables.add("tab1");
              } else if (argNs.equals(Namespace.of("A", "a"))) {
                tables.add("tab2");
              } else if (argNs.equals(Namespace.of("A", "a", "alpha"))) {
                tables.add("tab3");
                tables.add("tab4");
              } else if (argNs.equals(Namespace.of("A", "a", "beta"))) {
                tables.add("tab5");
              } else if (argNs.equals(Namespace.of("A", "b", "gamma"))) {
                tables.add("tab6");
              } else if (argNs.equals(Namespace.of("B", "c", "delta"))) {
                tables.add("tab7");
              } else if (argNs.equals(Namespace.of("B", "d"))) {
                tables.add("tab8");
              } else if (argNs.equals(Namespace.of("B", "d", "epsilon"))) {
                tables.add("tab9");
              } else {
                return Lists.newArrayList();
              }
              return tables.stream()
                  .map(t -> TableIdentifier.of(argNs, t))
                  .collect(Collectors.toList());
            });

    List<TableIdentifier> tables =
        ((IcebergRestCatalogAccessor) restCatalogAccessor)
            .streamTables(catalog, allowedNamespaces, isRecursiveAllowedNamespaces)
            .collect(Collectors.toList());
    assertEquals(expectedTableNameNums.size(), tables.size());
    List<Integer> tableNumList = tableIdentifiersToTableNameNumbers(tables);
    tableNumList.removeAll(expectedTableNameNums);
    assertTrue(tableNumList.isEmpty());
  }

  @Test
  public void testNSAllowListFullRecursive() {
    testNSAllowList(
        Sets.newHashSet(Namespace.empty()), true, Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testNSAllowListPartialRecursion() {
    testNSAllowList(
        Sets.newHashSet(Namespace.of("A", "a", "alpha"), Namespace.of("B", "d")),
        true,
        Lists.newArrayList(3, 4, 8, 9));
  }

  @Test
  public void testNSAllowListNoRecursion() {
    testNSAllowList(
        Sets.newHashSet(Namespace.of("A", "a", "alpha"), Namespace.of("B", "d")),
        false,
        Lists.newArrayList(3, 4, 8));
  }

  @Test
  public void testNSAllowListHigherLevelRecursion() {
    testNSAllowList(
        Sets.newHashSet(Namespace.of("A", "a"), Namespace.of("B")),
        true,
        Lists.newArrayList(2, 3, 4, 5, 7, 8, 9));
  }

  private static List<Integer> tableIdentifiersToTableNameNumbers(
      List<TableIdentifier> tableIdentifiers) {
    return tableIdentifiers.stream()
        .map(ti -> Character.getNumericValue(ti.name().charAt(3)))
        .collect(Collectors.toList());
  }

  @Test
  public void testGetViewMetadata() {
    when(mockRestCatalog.loadView(any())).thenReturn(mockBaseView);
    List<String> dataset = List.of("source", "db", "view");
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    ViewMetadata viewMetadata = restCatalogAccessorWithViewSupport.getViewMetadata(dataset);
    assertEquals(mockViewMetadata, viewMetadata);
  }

  @Test
  public void testInvalidCatalog() {
    InMemoryCatalog mockInMemoryCatalog = mock(InMemoryCatalog.class);
    when(invalidSupplierCatalog.get()).thenReturn(mockInMemoryCatalog);
    CatalogAccessor restCatalogAccessorWithInvalidCatalog =
        new IcebergRestCatalogAccessor(invalidSupplierCatalog, mockOptionManager, null, false);
    assertThatThrownBy(
            () -> restCatalogAccessorWithInvalidCatalog.datasetExists(Arrays.asList("a", "b", "c")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("RESTCatalog instance expected");
  }

  @Test
  public void testDatasetExistsWithView() {
    when(mockRestCatalog.viewExists(TableIdentifier.of("b", "c"))).thenReturn(true);
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    assertTrue(restCatalogAccessorWithViewSupport.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testDatasetDoesNotExistsWithView() {
    when(mockRestCatalog.viewExists(any())).thenReturn(false);
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    assertFalse(restCatalogAccessorWithViewSupport.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testListDatasetHandles() throws ConnectorException {
    TableIdentifier mockTableIdentifier = mock(TableIdentifier.class);
    TableIdentifier mockViewIdentifier = mock(TableIdentifier.class);
    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);

    when(mockRestCatalog.listTables(any())).thenReturn(List.of(mockTableIdentifier));
    when(mockRestCatalog.listViews(any())).thenReturn(List.of(mockViewIdentifier));
    when(mockTableIdentifier.namespace()).thenReturn(Namespace.of("root"));
    when(mockViewIdentifier.namespace()).thenReturn(Namespace.of("root"));
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    DatasetHandleListing datasetHandleListing =
        restCatalogAccessorWithViewSupport.listDatasetHandles("root", mockPlugin);
    List<DatasetHandle> datasetHandles = ImmutableList.copyOf(datasetHandleListing.iterator());

    assertNotNull(datasetHandles);
    assertEquals(2, datasetHandles.size());
  }

  @Test
  public void testListDatasetHandlesViewsDisabled() throws ConnectorException {
    TableIdentifier mockTableIdentifier = mock(TableIdentifier.class);
    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);

    when(mockRestCatalog.listTables(any())).thenReturn(List.of(mockTableIdentifier));
    when(mockTableIdentifier.namespace()).thenReturn(Namespace.of("root"));

    DatasetHandleListing datasetHandleListing =
        restCatalogAccessor.listDatasetHandles("root", mockPlugin);
    List<DatasetHandle> datasetHandles = ImmutableList.copyOf(datasetHandleListing.iterator());

    assertNotNull(datasetHandles);
    assertEquals(1, datasetHandles.size());
    verify(mockRestCatalog, never()).listViews(any());
  }

  @Test
  public void testListDatasetHandlesOnlyViews() throws ConnectorException {
    TableIdentifier mockViewIdentifier = mock(TableIdentifier.class);
    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);

    when(mockRestCatalog.listTables(any())).thenReturn(List.of());
    when(mockRestCatalog.listViews(any())).thenReturn(List.of(mockViewIdentifier));
    when(mockViewIdentifier.namespace()).thenReturn(Namespace.of("root"));
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);

    DatasetHandleListing datasetHandleListing =
        restCatalogAccessorWithViewSupport.listDatasetHandles("root", mockPlugin);
    List<DatasetHandle> datasetHandles = ImmutableList.copyOf(datasetHandleListing.iterator());

    assertNotNull(datasetHandles);
    assertEquals(1, datasetHandles.size());
    assertDoesNotThrow(() -> datasetHandles.get(0).unwrap(ViewDatasetHandle.class));
  }

  @Test
  public void testListDatasetHandlesNoTablesOrViews() throws ConnectorException {
    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    when(mockRestCatalog.listTables(any())).thenReturn(List.of());
    when(mockRestCatalog.listViews(any())).thenReturn(List.of());

    DatasetHandleListing datasetHandleListing =
        restCatalogAccessorWithViewSupport.listDatasetHandles("root", mockPlugin);
    List<DatasetHandle> datasetHandles = ImmutableList.copyOf(datasetHandleListing.iterator());

    assertNotNull(datasetHandles);
    assertTrue(datasetHandles.isEmpty());
  }

  @Test
  public void testListDatasetHandlesReturnsEmptyOnException() throws ConnectorException {
    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);
    assertThat(restCatalogAccessorWithViewSupport.listDatasetHandles("root", mockPlugin).iterator())
        .toIterable()
        .isEmpty();
  }

  @Test
  public void testGetViewMetadataDataset() {
    // Mock the behavior of the viewHandle
    DatasetHandle mockViewHandle = mock(DatasetHandle.class);
    IcebergCatalogViewProvider mockViewProvider = mock(IcebergCatalogViewProvider.class);
    when(mockViewHandle.unwrap(IcebergCatalogViewProvider.class)).thenReturn(mockViewProvider);

    // Call the method
    DatasetMetadata result = restCatalogAccessor.getViewMetadata(mockViewHandle);

    // Verify the result
    assertNotNull(result);
    assertEquals(mockViewProvider, result);
  }

  @Test
  public void testGetDefaultBaseLocation() {
    String defaultBaseLocationInCatalog = "s3://bucket/path";
    when(mockRestCatalog.properties())
        .thenReturn(ImmutableMap.of(DEFAULT_BASE_LOCATION, defaultBaseLocationInCatalog));
    String expectedDefaultBaseLocation = restCatalogAccessor.getDefaultBaseLocation();
    assertThat(expectedDefaultBaseLocation).isEqualTo(defaultBaseLocationInCatalog);
  }

  @Test
  public void testTableCacheUserAware() throws Exception {
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED)).thenReturn(true);
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);

    final GetDatasetOption forceUpdateOnce = new ForceUpdateOption(ForceUpdateOption.Mode.ONCE);
    final GetDatasetOption forceUpdateAlways = new ForceUpdateOption(ForceUpdateOption.Mode.ALWAYS);

    final TableIdentifier tableName = TableIdentifier.of("table_cached");
    final TableIdentifier viewName = TableIdentifier.of("view_cached");

    Table table = mock(Table.class);
    View view = mock(View.class);

    AtomicInteger loadTableCalls = new AtomicInteger(0);
    AtomicInteger loadViewCalls = new AtomicInteger(0);

    doAnswer(
            invocationOnMock -> {
              loadTableCalls.incrementAndGet();
              return table;
            })
        .when(mockRestCatalog)
        .loadTable(eq(tableName));

    doAnswer(
            invocationOnMock -> {
              loadViewCalls.incrementAndGet();
              return view;
            })
        .when(mockRestCatalog)
        .loadView(eq(viewName));

    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;

    String user1 = UUID.randomUUID().toString();
    String user2 = UUID.randomUUID().toString();
    // Tables - (also testing ForceUpdateOption class)
    RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.of(user1))
        .run(
            () -> {
              accessor.loadTable(tableName);
              accessor.loadTable(tableName);
            });
    // expecting 1 cache miss and 1 cache hit
    assertEquals(1, loadTableCalls.get());
    RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.of(user2))
        .run(
            () -> {
              accessor.loadTable(tableName);
              accessor.loadTable(tableName);
            });
    // expecting 1 cache miss and 1 cache hit for different user
    assertEquals(2, loadTableCalls.get());

    // Views - (also testing ForceUpdateOption class)
    RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.of(user1))
        .run(
            () -> {
              accessor.loadView(viewName);
              accessor.loadView(viewName);
            });
    // expecting 1 cache miss and 1 cache hit
    assertEquals(1, loadViewCalls.get());
    RequestContext.current()
        .with(UserContext.CTX_KEY, UserContext.of(user2))
        .run(
            () -> {
              accessor.loadView(viewName);
              accessor.loadView(viewName);
            });
    // expecting 1 cache miss and 1 cache hit for different user
    assertEquals(2, loadViewCalls.get());
  }

  @Test
  public void testTableCacheInvalidationOnDML() {
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED)).thenReturn(true);
    when(mockOptionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);

    final GetDatasetOption forceUpdateOnce = new ForceUpdateOption(ForceUpdateOption.Mode.ONCE);
    final GetDatasetOption forceUpdateAlways = new ForceUpdateOption(ForceUpdateOption.Mode.ALWAYS);

    final TableIdentifier tableName = TableIdentifier.of("table_cached");
    final TableIdentifier viewName = TableIdentifier.of("view_cached");

    Table table = mock(Table.class);
    View view = mock(View.class);

    AtomicInteger loadTableCalls = new AtomicInteger(0);
    AtomicInteger loadViewCalls = new AtomicInteger(0);

    doAnswer(
            invocationOnMock -> {
              loadTableCalls.incrementAndGet();
              return table;
            })
        .when(mockRestCatalog)
        .loadTable(eq(tableName));

    doAnswer(
            invocationOnMock -> {
              loadViewCalls.incrementAndGet();
              return view;
            })
        .when(mockRestCatalog)
        .loadView(eq(viewName));

    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;

    // Tables - (also testing ForceUpdateOption class)
    // No GetDatasetOption specified - expecting 1 cache miss, 2 cache hits
    accessor.loadTable(tableName);
    accessor.loadTable(tableName);
    accessor.loadTable(tableName);
    assertEquals(1, loadTableCalls.get());

    // Force update once
    accessor.loadTable(tableName, forceUpdateOnce);
    assertEquals(2, loadTableCalls.get());
    accessor.loadTable(tableName, forceUpdateOnce);
    assertEquals(2, loadTableCalls.get());

    // Force update always
    accessor.loadTable(tableName, forceUpdateAlways);
    assertEquals(3, loadTableCalls.get());
    accessor.loadTable(tableName, forceUpdateAlways);
    assertEquals(4, loadTableCalls.get());

    // Views
    // GetDatasetOption specified - expecting 1 cache miss, 2 cache hits
    accessor.loadView(viewName);
    accessor.loadView(viewName);
    accessor.loadView(viewName);
    assertEquals(1, loadViewCalls.get());

    // Force update
    accessor.loadView(viewName, forceUpdateAlways);
    assertEquals(2, loadViewCalls.get());
    accessor.loadView(viewName, forceUpdateAlways);
    assertEquals(3, loadViewCalls.get());
  }

  @Test
  public void testGetViewHandleInternal() {
    List<String> viewPath = List.of("catalog", "ns", "view");
    final TableIdentifier tableIdentifier = TableIdentifier.of("ns", "view");

    SupportsIcebergRootPointer mockPlugin = mock(SupportsIcebergRootPointer.class);
    ViewVersion mockViewVersion = mock(ViewVersion.class);
    when(mockPlugin.loadViewMetadata(any(TableIdentifier.class))).thenReturn(mockBaseView);
    when(mockViewMetadata.currentVersion()).thenReturn(mockViewVersion);
    when(mockViewVersion.defaultCatalog()).thenReturn("catalog");

    AbstractRestCatalogAccessor accessor =
        (AbstractRestCatalogAccessor) restCatalogAccessorWithViewSupport;
    DatasetHandle datasetHandle =
        accessor.getViewHandleInternal(viewPath, tableIdentifier, mockPlugin);
    assertThat(datasetHandle).isInstanceOf(IcebergCatalogViewProvider.class);
    IcebergCatalogViewProvider icebergCatalogViewProvider =
        datasetHandle.unwrap(IcebergCatalogViewProvider.class);
    icebergCatalogViewProvider.getCatalog();

    verify(mockPlugin, times(1)).loadViewMetadata(tableIdentifier);
  }

  @Test
  public void testInvalidateTableCacheForAllUsers() {
    // Arrange
    List<String> tablePath = Arrays.asList("test_source", "namespace1", "test_table");
    TableIdentifier tableId = TableIdentifier.of("namespace1", "test_table");

    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    Table mockTable = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations mockTableOps = mock(TableOperations.class);
    TableMetadata mockTableMetadata = mock(TableMetadata.class);

    when(((HasTableOperations) mockTable).operations()).thenReturn(mockTableOps);
    when(mockTableOps.current()).thenReturn(mockTableMetadata);

    RESTCatalog mockCatalog = mock(RESTCatalog.class);
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
    when(validSupplierCatalog.get()).thenReturn(mockCatalog);

    // Act - Load table to populate cache, then invalidate, then load again

    // First load - should populate cache
    restCatalogAccessor.getTableMetadata(tablePath);
    verify(mockCatalog, times(1)).loadTable(any(TableIdentifier.class));

    // Second load - should hit cache (no additional catalog call)
    restCatalogAccessor.getTableMetadata(tablePath);
    verify(mockCatalog, times(1)).loadTable(any(TableIdentifier.class)); // Still 1 call

    // Invalidate cache for all users
    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;
    accessor.invalidateTableCacheForAllUsers(tableId);

    // Third load - should miss cache after invalidation (new catalog call)
    restCatalogAccessor.getTableMetadata(tablePath);
    verify(mockCatalog, times(2)).loadTable(any(TableIdentifier.class)); // Now 2 calls
  }

  @Test
  public void testInvalidateTableCacheForAllUsersWithEmptyCache() {
    // Arrange
    TableIdentifier tableId = TableIdentifier.of("namespace1", "test_table");

    // Act - Test invalidation on empty cache (no prior loads)
    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;
    // This should not throw an exception even with empty cache
    assertDoesNotThrow(() -> accessor.invalidateTableCacheForAllUsers(tableId));
  }

  @Test
  public void testInvalidateTableCacheForAllUsersWithNullTableIdentifier() {
    // Arrange
    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;

    // Act & Assert
    assertThatThrownBy(() -> accessor.invalidateTableCacheForAllUsers(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TableIdentifier cannot be null");
  }

  @Test
  public void testInvalidateTableCacheForAllUsersWithSingleUser() {
    // Arrange
    List<String> tablePath = Arrays.asList("test_source", "namespace1", "test_table");
    TableIdentifier tableId = TableIdentifier.of("namespace1", "test_table");

    // Create a mock table that implements HasTableOperations
    Table mockTable = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations mockTableOps = mock(TableOperations.class);
    TableMetadata mockTableMetadata = mock(TableMetadata.class);

    // Configure the HasTableOperations behavior
    when(((HasTableOperations) mockTable).operations()).thenReturn(mockTableOps);
    when(mockTableOps.current()).thenReturn(mockTableMetadata);

    // Create a mock catalog to track loadTable calls
    RESTCatalog mockCatalog = mock(RESTCatalog.class);
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
    when(validSupplierCatalog.get()).thenReturn(mockCatalog);

    // Act - Test with single user scenario

    // Load table once to populate cache
    restCatalogAccessor.getTableMetadata(tablePath);
    verify(mockCatalog, times(1)).loadTable(any(TableIdentifier.class));

    // Invalidate cache
    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;
    accessor.invalidateTableCacheForAllUsers(tableId);

    // Load again - should call catalog again after invalidation
    restCatalogAccessor.getTableMetadata(tablePath);
    verify(mockCatalog, times(2)).loadTable(any(TableIdentifier.class));
  }

  @Test
  public void testInvalidateTableCacheForAllUsersWithMultipleNamespaces() {
    // Arrange
    List<String> tablePath1 = Arrays.asList("test_source", "namespace1", "test_table");
    List<String> tablePath2 = Arrays.asList("test_source", "namespace2", "test_table");
    TableIdentifier tableId1 = TableIdentifier.of("namespace1", "test_table");
    TableIdentifier tableId2 = TableIdentifier.of("namespace2", "test_table");

    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    Table mockTable1 = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    Table mockTable2 = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations mockTableOps1 = mock(TableOperations.class);
    TableOperations mockTableOps2 = mock(TableOperations.class);
    TableMetadata mockTableMetadata1 = mock(TableMetadata.class);
    TableMetadata mockTableMetadata2 = mock(TableMetadata.class);

    when(((HasTableOperations) mockTable1).operations()).thenReturn(mockTableOps1);
    when(mockTableOps1.current()).thenReturn(mockTableMetadata1);
    when(((HasTableOperations) mockTable2).operations()).thenReturn(mockTableOps2);
    when(mockTableOps2.current()).thenReturn(mockTableMetadata2);

    RESTCatalog mockCatalog = mock(RESTCatalog.class);
    when(mockCatalog.loadTable(tableId1)).thenReturn(mockTable1);
    when(mockCatalog.loadTable(tableId2)).thenReturn(mockTable2);
    when(validSupplierCatalog.get()).thenReturn(mockCatalog);

    // Act

    // Load both tables  twice to populate cache
    restCatalogAccessor.getTableMetadata(tablePath1);
    restCatalogAccessor.getTableMetadata(tablePath2);
    restCatalogAccessor.getTableMetadata(tablePath1);
    restCatalogAccessor.getTableMetadata(tablePath2);
    verify(mockCatalog, times(1)).loadTable(tableId1);
    verify(mockCatalog, times(1)).loadTable(tableId2);

    // Invalidate only namespace1.test_table
    AbstractRestCatalogAccessor accessor = (AbstractRestCatalogAccessor) restCatalogAccessor;
    accessor.invalidateTableCacheForAllUsers(tableId1);

    // Load namespace1 table again - should call catalog (cache invalidated)
    restCatalogAccessor.getTableMetadata(tablePath1);
    verify(mockCatalog, times(2)).loadTable(tableId1); // Called again

    // Load namespace2 table again - should NOT call catalog (cache still valid)
    restCatalogAccessor.getTableMetadata(tablePath2);
    verify(mockCatalog, times(1)).loadTable(tableId2); // Still only 1 call
  }

  @Test
  public void testGetFolderStreamListsNestedNamespaces() {
    Namespace a = Namespace.of("a");
    Namespace ab = Namespace.of("a", "b");
    Namespace abc = Namespace.of("a", "b", "c");
    when(mockRestCatalog.listNamespaces(Namespace.empty())).thenReturn(List.of(a));
    when(mockRestCatalog.listNamespaces(a)).thenReturn(List.of(ab));
    when(mockRestCatalog.listNamespaces(ab)).thenReturn(List.of(abc));
    when(mockRestCatalog.listNamespaces(abc)).thenReturn(List.of());
    when(mockRestCatalog.loadNamespaceMetadata(any(Namespace.class)))
        .thenReturn(ImmutableMap.of("location", "s3://bucket/ns"));

    List<Namespace> folders =
        restCatalogAccessor
            .getFolderStream()
            .map(IcebergNamespaceWithProperties::getNamespace)
            .collect(Collectors.toList());

    assertThat(folders).containsExactlyInAnyOrder(a, ab, abc);
  }

  @Test
  public void testListDatasetIdentifiersInNestedNamespace() {
    Namespace abc = Namespace.of("a", "b", "c");
    TableIdentifier table = TableIdentifier.of(abc, "t");
    when(mockRestCatalog.listTables(abc)).thenReturn(List.of(table));

    assertThat(restCatalogAccessor.listDatasetIdentifiers(List.of("source", "a", "b", "c")))
        .containsExactly(table);
  }

  // --- checkState: cached client and cached tables ---

  private static RESTCatalog catalogWithTable(TableIdentifier tableId) {
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations ops = mock(TableOperations.class);
    when(((HasTableOperations) table).operations()).thenReturn(ops);
    when(ops.current()).thenReturn(mock(TableMetadata.class));
    RESTCatalog catalog = mock(RESTCatalog.class);
    when(catalog.loadTable(tableId)).thenReturn(table);
    return catalog;
  }

  @Test
  public void testDeniedRootListingInStateCheckKeepsCachedTables() throws Exception {
    // The recommended Polaris setup (allowed namespaces, no catalog-level LIST_NAMESPACES) fails
    // the root listing on every state check; that must not disable the table cache.
    List<String> tablePath = Arrays.asList("test_source", "ns1", "t1");
    TableIdentifier tableId = TableIdentifier.of("ns1", "t1");
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    RESTCatalog catalog = catalogWithTable(tableId);
    when(catalog.listNamespaces(Namespace.empty()))
        .thenThrow(new ForbiddenException("Forbidden: not authorized for op LIST_NAMESPACES"));
    when(validSupplierCatalog.get()).thenReturn(catalog);

    restCatalogAccessor.getTableMetadata(tablePath);
    assertThatThrownBy(() -> restCatalogAccessor.checkState())
        .isInstanceOf(IcebergRestCatalogAccessor.NamespaceListingForbiddenException.class);
    restCatalogAccessor.getTableMetadata(tablePath);

    verify(catalog, times(1)).loadTable(tableId);
  }

  @Test
  public void testServerErrorInStateCheckKeepsTheCachedClient() throws Exception {
    RESTCatalog catalog = mock(RESTCatalog.class);
    when(catalog.listNamespaces(Namespace.empty()))
        .thenReturn(List.of())
        .thenThrow(new ServiceFailureException("Server error: Internal server error"));
    when(validSupplierCatalog.get()).thenReturn(catalog);

    restCatalogAccessor.checkState();
    assertThatThrownBy(() -> restCatalogAccessor.checkState())
        .isInstanceOf(ServiceFailureException.class);

    // No new client was built, and the cached one (possibly in use by queries) stays open.
    verify(validSupplierCatalog, times(1)).get();
    verify(catalog, never()).close();
  }

  @Test
  public void testStaleSessionIsReplacedAndCachedTablesAreDropped() throws Exception {
    List<String> tablePath = Arrays.asList("test_source", "ns1", "t1");
    TableIdentifier tableId = TableIdentifier.of("ns1", "t1");
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    RESTCatalog stale = catalogWithTable(tableId);
    when(stale.listNamespaces(Namespace.empty()))
        .thenThrow(new NotAuthorizedException("Not authorized: invalid or expired token"));
    RESTCatalog fresh = catalogWithTable(tableId);
    when(fresh.listNamespaces(Namespace.empty())).thenReturn(List.of());
    when(validSupplierCatalog.get()).thenReturn(stale, fresh);

    restCatalogAccessor.getTableMetadata(tablePath);
    restCatalogAccessor.checkState();
    restCatalogAccessor.getTableMetadata(tablePath);

    // The cached table was bound to the closed stale client: it is loaded again by the new one.
    verify(stale).close();
    verify(stale, times(1)).loadTable(tableId);
    verify(fresh, times(1)).loadTable(tableId);
  }
}
