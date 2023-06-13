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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Function;
import org.apache.calcite.util.Pair;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.tablefunctions.TimeTravelTableMacro;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * {@link Catalog} implementation that caches table requests.
 */
public class CachingCatalog extends DelegatingCatalog {

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
    final DatasetConfig dataset = table.getDatasetConfig();
    if (dataset != null) {
      final NamespaceKey canonicalizedKey = new NamespaceKey(dataset.getFullPathList());
      tablesByNamespaceKey.put(TableCacheKey.of(requestKey.getCatalogIdentity(), canonicalizedKey.getPathComponents(), requestKey.getTableVersionContext()), table);
    }
    tablesByNamespaceKey.put(requestKey, table);
    return table;
  }

  private boolean containsNamespaceKey(TableCacheKey key) {
    return tablesByNamespaceKey.containsKey(key);
  }

  private DremioTable getByNamespaceKey(TableCacheKey key) {
    return tablesByNamespaceKey.get(key);
  }

  private void removeByNamespaceKey(final NamespaceKey key) {
    // remove table from cache for all users
    tablesByNamespaceKey.entrySet().removeIf(entry -> entry.getKey().getKeyComponents().equals(key.getPathComponents()));
    invalidateNamespaceCache(key);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    final TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), null);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTableNoResolve(key)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), null);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTableNoColumnCount(key)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public void updateView(final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes) throws IOException {
    removeByNamespaceKey(key);
    super.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException {
    removeByNamespaceKey(key);
    super.dropView(key, viewOptions);
  }

  @Override
  @WithSpan
  public DremioTable getTable(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), resolved.getPathComponents(), null);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTable(key)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), resolved.getPathComponents(), null);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTableForQuery(key)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return ImmutableList.copyOf(tablesByNamespaceKey.values());
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
  public boolean supportsVersioning(NamespaceKey namespaceKey) {
    return delegate.supportsVersioning(namespaceKey);
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
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema), this);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema, checkValidity), this);
  }

  @Override
  public Catalog resolveCatalog(boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(checkValidity), this);
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

  private DremioTable timedGet(Supplier<DremioTable> s) {
    long start = System.currentTimeMillis();
    DremioTable table = s.get();
    long end = System.currentTimeMillis();
    if (table != null) {
      TableCacheKey key = TableCacheKey.of(getCatalogIdentity(), table.getPath().getPathComponents(), getTableVersionContext(table));
      canonicalKeyResolutionCount.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
      canonicalKeyAccessTime.compute(key, (k, v) -> (v == null) ? end - start : v + (end - start));
      canonicalKeyTables.put(key, table);
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
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), context);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTableSnapshotForQuery(key, context)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
    TableCacheKey tableKey = TableCacheKey.of(getCatalogIdentity(), key.getPathComponents(), context);
    if (!containsNamespaceKey(tableKey)) {
      return putTable(tableKey, timedGet(() -> super.getTableSnapshot(key, context)));
    }
    return getByNamespaceKey(tableKey);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    DremioTable table;
    if (!tablesByDatasetId.containsKey(datasetId)) {
      table = timedGet(() -> super.getTable(datasetId));
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

    public static TableCacheKey of(CatalogIdentity subject, List<String> keyComponents, TableVersionContext versionContext) {
      return new TableCacheKey(subject, CatalogEntityKey.newBuilder().keyComponents(keyComponents).tableVersionContext(versionContext).build());
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

  /**
   * Utility function to get the table version context for a given DremioTable.
   * TODO: Refactor DremioTable interface so TableVersionContext can be directly accessed.
   */
  private TableVersionContext getTableVersionContext(DremioTable table) {
    try {
      if (table instanceof NamespaceTable || table instanceof MaterializedDatasetTable) {
        return table.getDataset().getVersionContext();
      } else if (table instanceof ViewTable) {
        return TableVersionContext.of(((ViewTable) table).getVersionContext());
      }
    } catch (Exception e) {
      return null;
    }
    return null;
  }

  private CatalogIdentity getCatalogIdentity() {
    try {
      return getMetadataRequestOptions().getSchemaConfig().getAuthContext().getSubject();
    } catch (Exception ignore) {
      // Some tests with mocked Catalog throws exceptions here
    }
    return null;
  }
}
