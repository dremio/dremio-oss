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

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.store.iceberg.dremioudf.core.udf.InMemoryCatalog;
import com.dremio.options.OptionManager;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRestCatalogAccessor {

  @Mock private Supplier<Catalog> mockCatalogSupplier;

  private CatalogAccessor restCatalogAccessor;
  private static final long CATALOG_EXPIRY_SECONDS = 2L;

  @Before
  public void setup() {
    Configuration mockConfiguration = mock(Configuration.class);
    OptionManager mockOptionManager = mock(OptionManager.class);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS)).thenReturn(10L);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(10L);
    when(mockOptionManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS))
        .thenReturn(CATALOG_EXPIRY_SECONDS);
    restCatalogAccessor =
        new RestCatalogAccessor(
            mockCatalogSupplier, mockConfiguration, mockOptionManager, null, false);
  }

  @After
  public void teardown() throws Exception {
    restCatalogAccessor = null;
  }

  @Test
  public void testDatasetExistsThrowsExceptionForWrongCatalog() {
    InMemoryCatalog mockInMemoryCatalog = mock(InMemoryCatalog.class);
    when(mockCatalogSupplier.get()).thenReturn(mockInMemoryCatalog);
    assertThatThrownBy(() -> restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("RESTCatalog instance expected");
  }

  @Test
  public void testDatasetExists() {
    RESTCatalog mockRESTCatalog = mock(RESTCatalog.class);
    when(mockRESTCatalog.tableExists(TableIdentifier.of("b", "c"))).thenReturn(true);
    when(mockCatalogSupplier.get()).thenReturn(mockRESTCatalog);
    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testDatasetDoesNotExists() {
    RESTCatalog mockRESTCatalog = mock(RESTCatalog.class);
    when(mockRESTCatalog.tableExists(any())).thenReturn(false);
    when(mockCatalogSupplier.get()).thenReturn(mockRESTCatalog);
    assertFalse(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void testDatasetExistsThrowsNullPointerExceptionForNullList() {
    RESTCatalog mockRESTCatalog = mock(RESTCatalog.class);
    when(mockCatalogSupplier.get()).thenReturn(mockRESTCatalog);
    assertThatThrownBy(() -> restCatalogAccessor.datasetExists(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testDatasetExistsThrowsIllegalStateExceptionForEmptyList() {
    RESTCatalog mockRESTCatalog = mock(RESTCatalog.class);
    when(mockCatalogSupplier.get()).thenReturn(mockRESTCatalog);
    assertThatThrownBy(() -> restCatalogAccessor.datasetExists(List.of()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testClose() throws Exception {
    restCatalogAccessor.close();
    RESTCatalog mockCatalog = mock(RESTCatalog.class);
    when(mockCatalogSupplier.get()).thenReturn(mockCatalog);
    doThrow(new IOException()).when((Closeable) mockCatalog).close();
    restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c"));
    restCatalogAccessor.close();
    verify(((Closeable) mockCatalog), times(1)).close();
  }

  @Test
  public void testCatalogSupplierCalledAfterExpiry() throws Exception {
    RESTCatalog mockRESTCatalog = mock(RESTCatalog.class);
    when(mockRESTCatalog.tableExists(any())).thenReturn(true);
    when(mockCatalogSupplier.get()).thenReturn(mockRESTCatalog);

    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
    verify(mockCatalogSupplier, times(1)).get();

    Thread.sleep(TimeUnit.SECONDS.toMillis(CATALOG_EXPIRY_SECONDS + 1));
    assertTrue(restCatalogAccessor.datasetExists(Arrays.asList("a", "b", "c")));
    verify(mockCatalogSupplier, times(2)).get();
  }

  private static void testNSAllowList(
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
        RestCatalogAccessor.streamTables(catalog, allowedNamespaces, isRecursiveAllowedNamespaces)
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
}
