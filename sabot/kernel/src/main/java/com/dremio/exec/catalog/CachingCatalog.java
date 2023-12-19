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

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.calcite.schema.Function;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.tablefunctions.TimeTravelTableMacro;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * {@link Catalog} implementation that caches table requests.
 */
public class CachingCatalog extends DelegatingCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachingCatalog.class);

  private final Map<String, DremioTable> tablesByDatasetId;

  // The same table may appear under multiple keys when a context is set in the query or view definition
  private final Map<TableCacheKey, DremioTable> tablesByNamespaceKey;
  // Time spent accessing a particular table or view by canonical key (in String form).
  private final Map<TableCacheKey, Long> canonicalKeyAccessTime;
  // Number of resolutions for a particular canonical key
  // For example, a query may reference a table with canonical key source.table but if a context ctx is set in the query
  // or view definition then we need to be able to look up the table as both ctx.source.table and source.table.
  private final Map<TableCacheKey, Long> canonicalKeyResolutionCount;
  private final Map<TableCacheKey, DremioTable> canonicalKeyTables;
  private final Map<FunctionCacheKey, Collection<Function>> functionsCache;

  @VisibleForTesting
  public CachingCatalog(Catalog delegate) {
    this(delegate, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  }

  private CachingCatalog(Catalog delegate, Map<TableCacheKey, DremioTable> tablesByNamespaceKey,
                         Map<String, DremioTable> tablesByDatasetId,
                         Map<TableCacheKey, Long> canonicalKeyAccessTime,
                         Map<TableCacheKey, Long> canonicalKeyResolutionCount,
                         Map<TableCacheKey, DremioTable> canonicalKeyTables,
                         Map<FunctionCacheKey, Collection<Function>> functionsCache) {
    super(delegate);
    this.tablesByNamespaceKey = tablesByNamespaceKey;
    this.tablesByDatasetId = tablesByDatasetId;
    this.canonicalKeyAccessTime = canonicalKeyAccessTime;
    this.canonicalKeyResolutionCount = canonicalKeyResolutionCount;
    this.canonicalKeyTables = canonicalKeyTables;
    this.functionsCache = functionsCache;
  }

  private CachingCatalog(Catalog delegate, CachingCatalog cache) {

    this(delegate, cache.tablesByNamespaceKey, cache.tablesByDatasetId, cache.canonicalKeyAccessTime, cache.canonicalKeyResolutionCount, cache.canonicalKeyTables, cache.functionsCache);
  }

  private DremioTable putTable(TableCacheKey requestKey, DremioTable table) {
    if (table == null) {
      return null;
    }
    final NamespaceKey canonicalizedKey = table.getPath();
    tablesByNamespaceKey.put(TableCacheKey.of(requestKey.getCatalogIdentity(), canonicalizedKey.getPathComponents(), requestKey.getTableVersionContext()), table);
    tablesByNamespaceKey.put(requestKey, table);

    final TableCacheKey canonicalKey = TableCacheKey.of(getCatalogIdentity(), table);
    canonicalKeyResolutionCount.compute(canonicalKey, (k, v) -> (v == null) ? 1 : v + 1);
    if (!requestKey.getKeyComponents().equals(canonicalizedKey.asLowerCase().getPathComponents())) {
      // When request key and canonical key do not match, then we know for certain two calls were made into dataset manager
      canonicalKeyResolutionCount.compute(canonicalKey, (k, v) -> (v == null) ? 1 : v + 1);
    }
    return table;
  }

  private boolean containsNamespaceKey(TableCacheKey key) {
    return tablesByNamespaceKey.containsKey(key);
  }

  private DremioTable getByNamespaceKey(TableCacheKey key) {
    return tablesByNamespaceKey.get(key);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    final TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), getVersionContextIfVersioned(key));
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableNoResolve(key), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableSnapshotNoResolve(NamespaceKey key, TableVersionContext context) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), context);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableSnapshotNoResolve(key, context), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), getVersionContextIfVersioned(key));
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableNoColumnCount(key), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public void updateView(final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes) throws IOException {
    clearDatasetCache(key, null);
    super.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException {
    clearDatasetCache(key, null);
    super.dropView(key, viewOptions);
  }

  @Override
  @WithSpan
  public DremioTable getTable(NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), resolved.getPathComponents(), getVersionContextIfVersioned(resolved));
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTable(key), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }

    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), resolved.getPathComponents(), getVersionContextIfVersioned(resolved));
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableForQuery(key), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  private TableVersionContext getVersionContextIfVersioned(NamespaceKey key) {
    if (CatalogUtil.requestedPluginSupportsVersionedTables(key, this)) {
      return TableVersionContext.of(getMetadataRequestOptions().getVersionForSource(key.getRoot(), key));
    }
    return null;
  }

  private TableVersionContext getVersionContextIfVersioned(NamespaceKey key, TableVersionContext tableVersionContext) {
    if (CatalogUtil.requestedPluginSupportsVersionedTables(key, this) || tableVersionContext.isTimeTravelType()) {
      return tableVersionContext;
    }
    return null;
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return ImmutableList.copyOf(canonicalKeyTables.values());
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return new CachingCatalog(delegate.resolveCatalog(subject), this);
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new CachingCatalog(delegate.resolveCatalog(sourceVersionMapping), this);
  }

  @Override
  public Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext) {
    return new CachingCatalog(delegate.resolveCatalogResetContext(sourceName, versionContext));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CachingCatalog(delegate.resolveCatalog(newDefaultSchema), this);
  }

  @Override
  public Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {
    Catalog newDelegate = delegate.visit(catalogRewrite);
    if (newDelegate == delegate) {
      return catalogRewrite.apply(this);
    } else {
      return catalogRewrite.apply(new CachingCatalog(newDelegate));
    }
  }

  private DremioTable timedGet(Stopwatch stopwatch, Supplier<DremioTable> s, TableCacheKey key) {
    DremioTable table = null;
    try {
      table = s.get();
    } finally {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      if (table != null) {
        TableCacheKey canonicalKey = TableCacheKey.of(getCatalogIdentity(), table);
        canonicalKeyAccessTime.compute(canonicalKey, (k, v) -> (v == null) ? elapsed : v + elapsed);
        canonicalKeyTables.put(canonicalKey, table);
        LOGGER.debug("Successful catalog lookup - table=[{}] versionContext=[{}] elapsed={}ms",
            PathUtils.constructFullPath(canonicalKey.getKeyComponents()),
            canonicalKey.getTableVersionContext() != null ? canonicalKey.getTableVersionContext() :
                TableVersionContext.NOT_SPECIFIED,
            elapsed);
      } else {
        if (key != null) {
          LOGGER.debug("Failed catalog lookup - table=[{}] versionContext=[{}] elapsed={}ms",
              PathUtils.constructFullPath(key.getKeyComponents()),
              key.getTableVersionContext() != null ? key.getTableVersionContext() : TableVersionContext.NOT_SPECIFIED,
              elapsed);
        }
      }
    }
    return table;
  }

  /**
   * Logs time spent accessing the catalog for any table or view, and version context (if available).
   */
  @Override
  public void addCatalogStats() {
    long totalTime = canonicalKeyAccessTime.values().stream().mapToLong(Long::longValue).sum();
    long totalResolutions = canonicalKeyResolutionCount.values().stream().mapToLong(Long::longValue).sum();
    getMetadataStatsCollector().addDatasetStat(String.format("Catalog Access for %d Total Dataset(s)", canonicalKeyResolutionCount.keySet().size()),
      String.format("using %d resolved key(s)", totalResolutions), totalTime);
    canonicalKeyAccessTime.entrySet().stream().sorted(Comparator.comparing(Map.Entry<TableCacheKey, Long>::getValue).reversed()).
      forEach(entry -> getMetadataStatsCollector().addDatasetStat(
        String.format("Catalog Access for %s%s(%s)", PathUtils.constructFullPath(entry.getKey().getKeyComponents()),
          getTableVersionContext(entry),
          canonicalKeyTables.get(entry.getKey()).getDatasetConfig().getType()),
        String.format("using %d resolved key(s)", canonicalKeyResolutionCount.get(entry.getKey())),
        canonicalKeyAccessTime.get(entry.getKey())));
  }

  private Collection<Function> getAndMapFunctions(NamespaceKey path, FunctionType functionType) {
    return super.getFunctions(path, functionType).stream()
      .map(function -> {
        if (function instanceof TimeTravelTableMacro) {
          return new TimeTravelTableMacro(
            (tablePath, versionContext) -> {
              return this.getTableSnapshotForQuery(new NamespaceKey(tablePath), versionContext);
            });
        }
        return function;
      })
      .collect(Collectors.toList());
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path, FunctionType functionType) {
    // NamespaceKey is normalized by default
    final FunctionCacheKey key = FunctionCacheKey.of(getCatalogIdentity(), path.getSchemaPath().toLowerCase(), functionType);
    if (!functionsCache.containsKey(key)) {
      Collection<Function> f = getAndMapFunctions(path, functionType);
      functionsCache.put(key, f);
      return f;
    }
    return functionsCache.get(key);
  }

  @Override
  public DremioTable getTableSnapshotForQuery(NamespaceKey key, TableVersionContext context) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), getVersionContextIfVersioned(key, context));
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableSnapshotForQuery(key, context), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), context);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(stopwatch, () -> super.getTableSnapshot(key, context), tableKey));
    }
    return getByNamespaceKey(tableKey);
  }

  @Nonnull
  @Override
  public Optional<TableMetadataVerifyResult> verifyTableMetadata(NamespaceKey key, TableMetadataVerifyRequest metadataVerifyRequest) {
    return super.verifyTableMetadata(key, metadataVerifyRequest);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    DremioTable table;
    if (!tablesByDatasetId.containsKey(datasetId)) {
      table = timedGet(stopwatch, () -> super.getTable(datasetId), null);
      if (table != null) {
        tablesByDatasetId.put(datasetId, table);
      }
    }
    return tablesByDatasetId.get(datasetId);
  }

  /**
   * Searches tablesByNamespaceKey for a specific table and retrieves its version context.
   */
  private String getTableVersionContext(Map.Entry<TableCacheKey, Long> catalogEntry) {
    if (catalogEntry.getKey().getTableVersionContext() != null) {
      return String.format(" [%s] ", catalogEntry.getKey().getTableVersionContext());
    }
    return " ";
  }

  /**
   * FunctionCacheKey is a combination of user/normalized namespace key/function type used for functions cache
   */
  private static final class FunctionCacheKey {
    private final CatalogIdentity subject;
    private final String namespaceKey;
    private final FunctionType functionType;

    private FunctionCacheKey(CatalogIdentity subject, String namespaceKey, FunctionType functionType) {
      this.subject = subject;
      this.namespaceKey = namespaceKey;
      this.functionType = functionType;
    }

    public static FunctionCacheKey of(CatalogIdentity subject, String namespaceKey, FunctionType functionType) {
      return new FunctionCacheKey(subject, namespaceKey, functionType);
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
      return functionType == key.functionType &&
        Objects.equals(subject, key.subject) &&
        Objects.equals(namespaceKey, key.namespaceKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, namespaceKey, functionType);
    }
  }

  /**
   * TableCacheKey is a pair of user/CatalogEntityKey.
   */
  private static final class TableCacheKey extends Pair<CatalogIdentity, CatalogEntityKey> {
    /**
     * Creates a Pair.
     *
     * @param subject left value
     * @param entityKey right value
     */
    private TableCacheKey(CatalogIdentity subject, CatalogEntityKey entityKey) {
      super(subject, entityKey);
    }

    /**
     * Creates a normalized TableCacheKey so that we can recognize cache hits for SCHEMA.TABLE1 or schema.table1 table paths
     * @param subject
     * @param keyComponents
     * @param versionContext
     * @return
     */
    public static TableCacheKey of(CatalogIdentity subject, List<String> keyComponents, TableVersionContext versionContext) {
      return new TableCacheKey(subject, CatalogEntityKey.newBuilder()
        .keyComponents(keyComponents.stream().map(String::toLowerCase).collect(Collectors.toList()))
        .tableVersionContext(versionContext).build());
    }

    public static TableCacheKey of(CatalogIdentity subject, DremioTable table) {
      return new TableCacheKey(subject, CatalogEntityKey.newBuilder()
        .keyComponents(table.getPath().getPathComponents())
        .tableVersionContext(table.getVersionContext()).build());
    }

    CatalogIdentity getCatalogIdentity() {
      return left;
    }

    List<String> getKeyComponents() {
      return right.getKeyComponents();
    }

    TableVersionContext getTableVersionContext() {
      return right.getTableVersionContext();
    }
  }

  private CatalogIdentity getCatalogIdentity() {
    try {
      return getMetadataRequestOptions().getSchemaConfig().getAuthContext().getSubject();
    } catch (Exception ignore) {
      // Some tests with mocked Catalog throws exceptions here
    }
    return null;
  }

  @Override
  public void clearDatasetCache(NamespaceKey dataset, TableVersionContext context) {
    super.clearDatasetCache(dataset, context);
    tablesByDatasetId.values().removeIf(t -> t.getPath().equals(dataset));
    tablesByNamespaceKey.values().removeIf(t -> t.getPath().equals(dataset));
    canonicalKeyTables.values().removeIf(t -> t.getPath().equals(dataset));
    invalidateNamespaceCache(dataset);
  }

}
