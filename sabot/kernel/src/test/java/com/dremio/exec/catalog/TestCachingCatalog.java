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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestCachingCatalog {

  private static final NamespaceKey NAMESPACE_KEY_1 =
      new NamespaceKey(ImmutableList.of("xx", "a", "b"));
  private static final NamespaceKey NAMESPACE_KEY_2 =
      new NamespaceKey(ImmutableList.of("xx", "a", "c"));

  private CachingCatalog cachingCatalog;
  private Map<NamespaceKey, DremioTable> namespaceKeyToTableMap;

  @Mock private Catalog delegate;
  @Mock private DremioTable t1;
  @Mock private DremioTable t2;

  @BeforeEach
  public void setup() {
    cachingCatalog = new CachingCatalog(delegate);
    namespaceKeyToTableMap = Map.of(NAMESPACE_KEY_1, t1, NAMESPACE_KEY_2, t2);
  }

  @Test
  public void testCachingForGetTableNoResolveWithNamespaceKey() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableNoResolve(eq(NAMESPACE_KEY_1))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableNoResolve(NAMESPACE_KEY_1),
        () -> verify(delegate).getTableNoResolve(eq(NAMESPACE_KEY_1)));
  }

  @Test
  public void testCachingForGetTableNoResolveWithEntityKey() {
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(NAMESPACE_KEY_1);
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableNoResolve(eq(catalogEntityKey))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableNoResolve(catalogEntityKey),
        () -> verify(delegate).getTableNoResolve(eq(catalogEntityKey)));
  }

  @Test
  public void testCachingForGetTableNoColumnCount() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableNoColumnCount(eq(NAMESPACE_KEY_1))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableNoColumnCount(NAMESPACE_KEY_1),
        () -> verify(delegate).getTableNoColumnCount(eq(NAMESPACE_KEY_1)));
  }

  @Test
  public void testCachingForGetTableWithNamespaceKey() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTable(eq(NAMESPACE_KEY_1))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTable(NAMESPACE_KEY_1),
        () -> verify(delegate).getTable(eq(NAMESPACE_KEY_1)));
  }

  @Test
  public void testCachingForGetTableWithEntityKey() {
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(NAMESPACE_KEY_1);
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTable(eq(catalogEntityKey))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTable(catalogEntityKey),
        () -> verify(delegate).getTable(eq(catalogEntityKey)));
  }

  @Test
  public void testCachingForGetTableWithDatasetId() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTable(eq("xx.a.b"))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTable("xx.a.b"), () -> verify(delegate).getTable(eq("xx.a.b")));
  }

  @Test
  public void testCachingForGetTableForQuery() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableForQuery(eq(NAMESPACE_KEY_1))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableForQuery(NAMESPACE_KEY_1),
        () -> verify(delegate).getTableForQuery(eq(NAMESPACE_KEY_1)));
  }

  @Test
  public void testCachingForGetTableSnapshotForQuery() {
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(NAMESPACE_KEY_1);
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableSnapshotForQuery(eq(catalogEntityKey))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableSnapshotForQuery(catalogEntityKey),
        () -> verify(delegate).getTableSnapshotForQuery(eq(catalogEntityKey)));
  }

  @Test
  public void testCachingForGetTableSnapshot() {
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(NAMESPACE_KEY_1);
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(delegate.getTableSnapshot(eq(catalogEntityKey))).thenReturn(t1);
    validateCaching(
        () -> cachingCatalog.getTableSnapshot(catalogEntityKey),
        () -> verify(delegate).getTableSnapshot(eq(catalogEntityKey)));
  }

  @Test
  public void testCachingForBulkGetTables() {
    when(t1.getPath()).thenReturn(NAMESPACE_KEY_1);
    when(t2.getPath()).thenReturn(NAMESPACE_KEY_2);

    Map<NamespaceKey, Integer> catalogCallsPerKey = new HashMap<>();

    when(delegate.bulkGetTables(any()))
        .thenAnswer(
            invocation -> {
              BulkRequest<NamespaceKey> request = invocation.getArgument(0);
              BulkResponse.Builder<NamespaceKey, Optional<DremioTable>> builder =
                  BulkResponse.builder();
              request.forEach(
                  key -> {
                    builder.add(key, Optional.ofNullable(namespaceKeyToTableMap.get(key)));
                    catalogCallsPerKey.merge(key, 1, (k, v) -> v + 1);
                  });
              return builder.build();
            });

    DremioTable table1;
    DremioTable table2;
    BulkRequest<NamespaceKey> request =
        BulkRequest.<NamespaceKey>builder().add(NAMESPACE_KEY_1).build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response =
        cachingCatalog.bulkGetTables(request);
    table1 = response.get(NAMESPACE_KEY_1).response().toCompletableFuture().join().get();
    assertThat(table1).isSameAs(t1);

    request = BulkRequest.<NamespaceKey>builder().add(NAMESPACE_KEY_1).add(NAMESPACE_KEY_2).build();
    response = cachingCatalog.bulkGetTables(request);
    table1 = response.get(NAMESPACE_KEY_1).response().toCompletableFuture().join().get();
    table2 = response.get(NAMESPACE_KEY_2).response().toCompletableFuture().join().get();
    assertThat(table1).isSameAs(t1);
    assertThat(table2).isSameAs(t2);

    // Each underlying catalog call should only happen once per key
    assertThat(catalogCallsPerKey)
        .containsEntry(NAMESPACE_KEY_1, 1)
        .containsEntry(NAMESPACE_KEY_2, 1);
  }

  @Test
  public void testUnexpectedExceptionDuringBulkTableLookup() {
    BulkRequest<NamespaceKey> request =
        BulkRequest.<NamespaceKey>builder().add(NAMESPACE_KEY_1).build();

    when(delegate.bulkGetTables(any())).thenThrow(new RuntimeException("Fail!"));

    // Nothing should be cached because an unexpected exception is thrown
    assertThatThrownBy(() -> cachingCatalog.bulkGetTables(request));
    assertThatThrownBy(() -> cachingCatalog.bulkGetTables(request));
    verify(delegate, times(2)).bulkGetTables(any());
  }

  @Test
  public void testNoResolveFunctionsDoNotCacheExceptions() {
    NamespaceKey namespaceKey = NAMESPACE_KEY_1;
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(namespaceKey);

    when(delegate.getTableNoResolve(eq(namespaceKey))).thenThrow(new RuntimeException("Fail!"));
    when(delegate.getTableNoResolve(eq(catalogEntityKey))).thenThrow(new RuntimeException("Fail!"));

    // Nothing should be cached because an unexpected exception is thrown
    assertThatThrownBy(() -> cachingCatalog.getTableNoResolve(namespaceKey));
    assertThatThrownBy(() -> cachingCatalog.getTableNoResolve(namespaceKey));
    assertThatThrownBy(() -> cachingCatalog.getTableNoResolve(catalogEntityKey));
    assertThatThrownBy(() -> cachingCatalog.getTableNoResolve(catalogEntityKey));
    verify(delegate, times(2)).getTableNoResolve(eq(namespaceKey));
    verify(delegate, times(2)).getTableNoResolve(eq(catalogEntityKey));
  }

  private void validateCaching(Supplier<DremioTable> catalogLookup, Runnable verification) {
    DremioTable table = catalogLookup.get();
    assertThat(table).isSameAs(t1);
    table = catalogLookup.get();
    assertThat(table).isSameAs(t1);
    verification.run();
  }
}
