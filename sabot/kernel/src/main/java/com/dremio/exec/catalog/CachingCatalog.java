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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.concurrent.bulk.BulkFunction;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.tablefunctions.TimeTravelTableMacro;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link Catalog} implementation that caches table requests. */
public class CachingCatalog extends DelegatingCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachingCatalog.class);

  private final ConcurrentMap<String, DremioTable> tablesByDatasetId;

  // The same table may appear under multiple keys when a context is set in the query or view
  // definition
  private final ConcurrentMap<TableCacheKey, CompletableFuture<Optional<DremioTable>>> tableCache;
  private final ConcurrentMap<TableCacheKey, DremioTable> canonicalKeyTables;
  private final ConcurrentMap<FunctionCacheKey, Collection<Function>> functionsCache;
  private final CatalogAccessStats catalogAccessStats;

  @VisibleForTesting
  public CachingCatalog(Catalog delegate) {
    this(
        delegate,
        new ConcurrentHashMap<>(),
        new ConcurrentHashMap<>(),
        new ConcurrentHashMap<>(),
        new ConcurrentHashMap<>(),
        new CatalogAccessStats());
  }

  private CachingCatalog(
      Catalog delegate,
      ConcurrentMap<TableCacheKey, CompletableFuture<Optional<DremioTable>>> tableCache,
      ConcurrentMap<String, DremioTable> tablesByDatasetId,
      ConcurrentMap<TableCacheKey, DremioTable> canonicalKeyTables,
      ConcurrentMap<FunctionCacheKey, Collection<Function>> functionsCache,
      CatalogAccessStats catalogAccessStats) {
    super(delegate);
    this.tableCache = tableCache;
    this.tablesByDatasetId = tablesByDatasetId;
    this.canonicalKeyTables = canonicalKeyTables;
    this.functionsCache = functionsCache;
    this.catalogAccessStats = catalogAccessStats;
  }

  private CachingCatalog(Catalog delegate, CachingCatalog cache) {
    this(
        delegate,
        cache.tableCache,
        cache.tablesByDatasetId,
        cache.canonicalKeyTables,
        cache.functionsCache,
        cache.catalogAccessStats);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return getOrLoadTableNoResolve(key, this::toTableCacheKey, super::getTableNoResolve);
  }

  @Override
  public DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey) {
    return getOrLoadTableNoResolve(
        catalogEntityKey, this::toTableCacheKey, super::getTableNoResolve);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return getOrLoadTable(key, this::toTableCacheKey, super::getTableNoColumnCount);
  }

  @Override
  public void updateView(
      final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException {
    clearDatasetCache(key, null);
    super.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException {
    clearDatasetCache(key, null);
    super.dropView(key, viewOptions);
  }

  @Override
  public CatalogAccessStats getCatalogAccessStats() {
    return catalogAccessStats;
  }

  private NamespaceKey toResolvedKey(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved != null) {
      return resolved;
    }
    return key;
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return getOrLoadTable(key, k -> toTableCacheKey(toResolvedKey(k)), super::getTable);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return getOrLoadTable(key, k -> toTableCacheKey(toResolvedKey(k)), super::getTableForQuery);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return ImmutableList.copyOf(canonicalKeyTables.values());
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return new CachingCatalog(getDelegate().resolveCatalog(subject), this);
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new CachingCatalog(getDelegate().resolveCatalog(sourceVersionMapping), this);
  }

  @Override
  public Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext) {
    return new CachingCatalog(getDelegate().resolveCatalogResetContext(sourceName, versionContext));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CachingCatalog(getDelegate().resolveCatalog(newDefaultSchema), this);
  }

  @Override
  public Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {
    Catalog newDelegate = getDelegate().visit(catalogRewrite);
    if (newDelegate == getDelegate()) {
      return catalogRewrite.apply(this);
    } else {
      return catalogRewrite.apply(new CachingCatalog(newDelegate));
    }
  }

  private Collection<Function> getAndMapFunctions(
      CatalogEntityKey path, FunctionType functionType) {
    return super.getFunctions(path, functionType).stream()
        .map(
            function -> {
              if (function instanceof TimeTravelTableMacro) {
                return new TimeTravelTableMacro(
                    (tablePath, versionContext) ->
                        this.getTableSnapshotForQuery(
                            CatalogEntityKey.newBuilder()
                                .keyComponents(tablePath)
                                .tableVersionContext(versionContext)
                                .build()));
              }
              return function;
            })
        .collect(Collectors.toList());
  }

  @Override
  public Collection<Function> getFunctions(CatalogEntityKey path, FunctionType functionType) {
    final FunctionCacheKey cacheKey =
        FunctionCacheKey.toNormalizedKey(getCatalogIdentity(), path, functionType);
    return functionsCache.computeIfAbsent(
        cacheKey, ignored -> getAndMapFunctions(path, functionType));
  }

  @Override
  public DremioTable getTableSnapshotForQuery(CatalogEntityKey catalogEntityKey) {
    return getOrLoadTable(catalogEntityKey, this::toTableCacheKey, super::getTableSnapshotForQuery);
  }

  @Override
  public DremioTable getTableSnapshot(CatalogEntityKey catalogEntityKey) {
    return getOrLoadTable(catalogEntityKey, this::toTableCacheKey, super::getTableSnapshot);
  }

  @Override
  public DremioTable getTable(CatalogEntityKey catalogEntityKey) {
    return getOrLoadTable(catalogEntityKey, this::toTableCacheKey, super::getTable);
  }

  @Nonnull
  @Override
  public Optional<TableMetadataVerifyResult> verifyTableMetadata(
      CatalogEntityKey key, TableMetadataVerifyRequest metadataVerifyRequest) {
    return super.verifyTableMetadata(key, metadataVerifyRequest);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return tablesByDatasetId.computeIfAbsent(
        datasetId,
        id -> {
          Stopwatch stopwatch = Stopwatch.createStarted();
          DremioTable table = super.getTable(id);
          // Update cache
          if (table != null) {
            long loadTimeMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            updateCacheAndStats(table, null, loadTimeMillis);
          }
          return table;
        });
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTables(
      BulkRequest<NamespaceKey> keys) {
    return getOrLoadTables(
        keys, k -> toTableCacheKey(toResolvedKey(k)), getDelegate()::bulkGetTables);
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTablesForQuery(
      BulkRequest<NamespaceKey> keys) {
    return getOrLoadTables(
        keys, k -> toTableCacheKey(toResolvedKey(k)), getDelegate()::bulkGetTablesForQuery);
  }

  /**
   * Some Catalog functions will attempt to fetch a table using two different keys: one resolved to
   * the default schema context, and the provided key. However, some functions will only attempt to
   * fetch a table using the provided key. In this case, if the Catalog cannot find a table for a
   * particular key, we should remove any cached entries as this could interfere with functions that
   * try with multiple keys.
   */
  private <T> DremioTable getOrLoadTableNoResolve(
      T key,
      java.util.function.Function<T, TableCacheKey> cacheKeyMapper,
      java.util.function.Function<T, DremioTable> tableLoader) {
    DremioTable table = null;
    try {
      table = getOrLoadTable(key, cacheKeyMapper, tableLoader);
    } finally {
      if (table == null) {
        tableCache.remove(cacheKeyMapper.apply(key));
      }
    }

    return table;
  }

  private <T> DremioTable getOrLoadTable(
      T key,
      java.util.function.Function<T, TableCacheKey> cacheKeyMapper,
      java.util.function.Function<T, DremioTable> tableLoader) {

    BulkRequest<T> singleRequest = BulkRequest.<T>builder(1).add(key).build();

    BulkResponse.Response<T, Optional<DremioTable>> singleResponse =
        getOrLoadTables(
                singleRequest,
                cacheKeyMapper,
                // Wrapper function for loading a single table. Execution of table loader function
                // is wrapped in a completed future.
                bulkRequest ->
                    bulkRequest.handleRequests(
                        k -> {
                          try {
                            DremioTable table = tableLoader.apply(k);
                            return CompletableFuture.completedFuture(Optional.ofNullable(table));
                          } catch (Exception e) {
                            return CompletableFuture.failedFuture(e);
                          }
                        }))
            .get(key);

    try {
      return singleResponse.response().toCompletableFuture().join().orElse(null);
    } catch (Exception e) {
      return ExceptionUtils.asRuntimeException(
          com.dremio.exec.planner.ExceptionUtils.unwrapCompletionException(e));
    }
  }

  private <T> BulkResponse<T, Optional<DremioTable>> getOrLoadTables(
      BulkRequest<T> bulkRequest,
      java.util.function.Function<T, TableCacheKey> cacheKeyMapper,
      BulkFunction<T, Optional<DremioTable>> bulkTableLoader) {

    int numKeys = bulkRequest.size();
    ImmutableList.Builder<CacheKeyAndProxyFuture<T>> newKeysBuilder =
        ImmutableList.builderWithExpectedSize(numKeys);
    BulkRequest.Builder<T> requestBuilder = BulkRequest.builder(numKeys);
    BulkResponse.Builder<T, Optional<DremioTable>> resultsBuilder = BulkResponse.builder(numKeys);

    bulkRequest.forEach(
        key -> {
          // Probe cache
          TableCacheKey cacheKey = cacheKeyMapper.apply(key);
          CompletableFuture<Optional<DremioTable>> cached = tableCache.get(cacheKey);

          if (cached == null) {
            // If entry not present, try inserting proxy entry into cache
            CompletableFuture<Optional<DremioTable>> proxy = new CompletableFuture<>();
            cached = tableCache.putIfAbsent(cacheKey, proxy);
            if (cached == null) {
              // Successful insertion into cache means this is a new cache entry
              // and the table needs to be loaded
              newKeysBuilder.add(CacheKeyAndProxyFuture.of(key, cacheKey, proxy));
              requestBuilder.add(key);
              cached = proxy;
            }
          }

          resultsBuilder.add(key, cached);
        });

    ImmutableList<CacheKeyAndProxyFuture<T>> newKeys = newKeysBuilder.build();
    BulkRequest<T> newKeysRequest = requestBuilder.build();
    BulkResponse<T, Optional<DremioTable>> results = resultsBuilder.build();

    // Short circuit if all entries already present in cache
    if (newKeys.isEmpty()) {
      return results;
    }

    // Bulk load new entries
    BulkResponse<T, Optional<DremioTable>> bulkLoad =
        handleException(newKeys, () -> bulkTableLoader.apply(newKeysRequest));

    // Complete proxies and update cache when bulk loads complete
    for (CacheKeyAndProxyFuture<T> cacheKeyAndProxy : newKeys) {
      T key = cacheKeyAndProxy.key();
      TableCacheKey cacheKey = cacheKeyAndProxy.cacheKey();
      CompletableFuture<Optional<DremioTable>> proxy = cacheKeyAndProxy.proxyFuture();
      BulkResponse.Response<T, Optional<DremioTable>> response = bulkLoad.get(key);
      CompletionStage<Optional<DremioTable>> tableLoad = response.response();

      tableLoad.whenComplete(
          (tableOptional, error) -> {
            long loadTimeMillis = response.elapsed(TimeUnit.MILLISECONDS);
            if (error == null) {
              proxy.complete(tableOptional);
              tableOptional.ifPresentOrElse(
                  table -> updateCacheAndStats(table, cacheKey, loadTimeMillis),
                  () -> logTableLoad(cacheKey, null, loadTimeMillis));
            } else {
              proxy.completeExceptionally(error);
              logTableLoad(cacheKey, null, loadTimeMillis);
            }
          });
    }

    return results;
  }

  /**
   * If an unexpected exception is thrown when tyring to execute the bulk loading function, we
   * should complete and remove the proxy entries from the cache so that we don't have `dangling`
   * proxies hanging around.
   */
  private <T> BulkResponse<T, Optional<DremioTable>> handleException(
      List<CacheKeyAndProxyFuture<T>> keysAndProxies,
      Supplier<BulkResponse<T, Optional<DremioTable>>> bulkTableLoader) {
    try {
      return bulkTableLoader.get();
    } catch (Exception e) {
      RuntimeException error = new RuntimeException("Error trying to bulk load table metadata", e);
      keysAndProxies.forEach(
          keyAndProxy -> {
            TableCacheKey cacheKey = keyAndProxy.cacheKey();
            CompletableFuture<Optional<DremioTable>> proxy = keyAndProxy.proxyFuture();
            tableCache.remove(cacheKey);
            proxy.completeExceptionally(error);
          });
      throw error;
    }
  }

  /**
   * Inserts canonical key mapping into cache and updates load time and resolution count stats when
   * table has been loaded
   */
  private void updateCacheAndStats(DremioTable table, TableCacheKey cacheKey, long loadTimeMillis) {
    // Write canonical key to cache
    CatalogIdentity identity = getCatalogIdentity();
    TableCacheKey canonicalCacheKey = TableCacheKey.toCanonicalKey(identity, table);
    CompletableFuture<Optional<DremioTable>> proxy =
        tableCache.computeIfAbsent(canonicalCacheKey, k -> new CompletableFuture<>());
    // We always update in case the returned table type holds more information,
    // e.g. fine-grained access table.
    proxy.obtrudeValue(Optional.of(table));

    // When request key and canonical key do not match, then we know for certain
    // two calls were made into dataset manager
    int resolutionIncrement =
        cacheKey == null || cacheKey.getKeyComponents().equals(canonicalCacheKey.getKeyComponents())
            ? 1
            : 2;

    catalogAccessStats.add(
        canonicalCacheKey.getCatalogEntityKey(),
        loadTimeMillis,
        resolutionIncrement,
        Optional.of(table)
            .map(DremioTable::getDatasetConfig)
            .map(DatasetConfig::getType)
            .orElse(null));

    canonicalKeyTables.put(canonicalCacheKey, table);

    logTableLoad(canonicalCacheKey, table, loadTimeMillis);
  }

  @Value.Immutable
  interface CacheKeyAndProxyFuture<T> {

    T key();

    TableCacheKey cacheKey();

    CompletableFuture<Optional<DremioTable>> proxyFuture();

    static <T> CacheKeyAndProxyFuture<T> of(
        T key, TableCacheKey cacheKey, CompletableFuture<Optional<DremioTable>> proxyFuture) {
      return new ImmutableCacheKeyAndProxyFuture.Builder<T>()
          .setKey(key)
          .setCacheKey(cacheKey)
          .setProxyFuture(proxyFuture)
          .build();
    }
  }

  private static void logTableLoad(
      @Nullable TableCacheKey cacheKey, @Nullable DremioTable table, long loadTimeMillis) {
    if (cacheKey != null && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{} catalog lookup - table=[{}] versionContext=[{}] elapsed={}ms",
          table != null ? "Successful" : "Failed",
          PathUtils.constructFullPath(cacheKey.getKeyComponents()),
          // TODO: DX-85701
          // Update to cacheKey.getTableVersionContext() without the null check.
          cacheKey.getTableVersionContext() != null
              ? cacheKey.getTableVersionContext()
              : TableVersionContext.NOT_SPECIFIED,
          loadTimeMillis);
    }
  }

  /**
   * FunctionCacheKey is a combination of user/normalized namespace key/function type used for
   * functions cache
   */
  private static final class FunctionCacheKey {
    private final CatalogIdentity subject;
    private final String namespaceKey;
    private final FunctionType functionType;

    private FunctionCacheKey(
        CatalogIdentity subject, String namespaceKey, FunctionType functionType) {
      this.subject = subject;
      this.namespaceKey = namespaceKey;
      this.functionType = functionType;
    }

    // TODO(DX-92355) : Rework normalize key for functions to be case sensitive for versioned UDFs
    // and to include TableVersionContext
    public static FunctionCacheKey toNormalizedKey(
        CatalogIdentity subject, CatalogEntityKey catalogEntityKey, FunctionType functionType) {
      return new FunctionCacheKey(
          subject, catalogEntityKey.toNamespaceKey().getSchemaPath().toLowerCase(), functionType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FunctionCacheKey key = (FunctionCacheKey) o;
      return functionType == key.functionType
          && Objects.equals(subject, key.subject)
          && Objects.equals(namespaceKey, key.namespaceKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, namespaceKey, functionType);
    }
  }

  /** TableCacheKey is a pair of user/CatalogEntityKey. */
  protected static final class TableCacheKey extends Pair<CatalogIdentity, CatalogEntityKey> {
    private TableCacheKey(CatalogIdentity subject, CatalogEntityKey entityKey) {
      super(subject, entityKey);
    }

    /**
     * Creates a normalized TableCacheKey so that we can recognize cache hits for SCHEMA.TABLE1 or
     * schema.table1 table paths
     */
    static TableCacheKey toNormalizedKey(
        CatalogIdentity subject, List<String> keyComponents, TableVersionContext versionContext) {
      return new TableCacheKey(
          subject,
          CatalogEntityKey.newBuilder()
              .keyComponents(
                  keyComponents.stream().map(String::toLowerCase).collect(Collectors.toList()))
              .tableVersionContext(versionContext)
              .build());
    }

    static TableCacheKey toCanonicalKey(CatalogIdentity subject, DremioTable table) {
      return toNormalizedKey(
          subject, table.getPath().getPathComponents(), table.getVersionContext());
    }

    CatalogIdentity getCatalogIdentity() {
      return left;
    }

    CatalogEntityKey getCatalogEntityKey() {
      return right;
    }

    List<String> getKeyComponents() {
      return right.getKeyComponents();
    }

    TableVersionContext getTableVersionContext() {
      return right.getTableVersionContext();
    }
  }

  private TableCacheKey toTableCacheKey(NamespaceKey key) {
    TableVersionContext versionContext = null;
    if (CatalogUtil.requestedPluginSupportsVersionedTables(key, this)) {
      versionContext =
          TableVersionContext.of(
              getMetadataRequestOptions().getVersionForSource(key.getRoot(), key));
    }
    return TableCacheKey.toNormalizedKey(
        getCatalogIdentity(), key.getPathComponents(), versionContext);
  }

  private TableCacheKey toTableCacheKey(CatalogEntityKey entityKey) {
    return TableCacheKey.toNormalizedKey(
        getCatalogIdentity(), entityKey.getKeyComponents(), entityKey.getTableVersionContext());
  }

  private CatalogIdentity getCatalogIdentity() {
    try {
      return getMetadataRequestOptions().getSchemaConfig().getAuthContext().getSubject();
    } catch (Exception ignore) {
      // Some tests with mocked Catalog throws exceptions here
      // FIXME: fix those tests to mock things properly instead of try-catching and using null here
    }
    return null;
  }

  @Override
  public void clearDatasetCache(NamespaceKey dataset, TableVersionContext context) {
    super.clearDatasetCache(dataset, context);
    tablesByDatasetId.values().removeIf(t -> t.getPath().equals(dataset));
    canonicalKeyTables.values().removeIf(t -> t.getPath().equals(dataset));
    tableCache
        .entrySet()
        .removeIf(
            t -> {
              try {
                Optional<DremioTable> tableOptional = t.getValue().join();
                if (tableOptional.isPresent()) {
                  return tableOptional.get().getPath().equals(dataset);
                } else {
                  TableCacheKey normalizedDatasetKey = toTableCacheKey(dataset);
                  return t.getKey()
                      .getValue()
                      .toNamespaceKey()
                      .equals(normalizedDatasetKey.getValue().toNamespaceKey());
                }
              } catch (Exception e) {
                return false;
              }
            });
    invalidateNamespaceCache(dataset);
  }
}
