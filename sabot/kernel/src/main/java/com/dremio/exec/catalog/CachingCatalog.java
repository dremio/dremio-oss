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
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

/**
 * {@link Catalog} implementation that caches table requests.
 * One case not handled yet is {@link SimpleCatalog#getFunctions(NamespaceKey, FunctionType)}.
 */
public class CachingCatalog extends DelegatingCatalog {

  // The same table may appear under multiple keys when a context is set in the query or view definition
  private final Map<NamespaceKey, DremioTable> tablesByNamespaceKey;
  // Time spent accessing a particular table or view by canonical key (in String form).
  private final Map<String, Long> canonicalKeyAccessTime;
  // Number of resolutions for a particular canonical key
  // For example, a query may reference a table with canonical key source.table but if a context ctx is set in the query
  // or view definition then we need to be able to look up the table as both ctx.source.table and source.table.
  private final Map<String, Long> canonicalKeyResolutionCount;
  private final Map<String, DremioTable> canonicalKeyTables;

  CachingCatalog(Catalog delegate) {
    this(delegate, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  }

  private CachingCatalog(Catalog delegate, Map<NamespaceKey, DremioTable> tablesByNamespaceKey,
                         Map<String, Long> canonicalKeyAccessTime,
                         Map<String, Long> canonicalKeyResolutionCount, Map<String, DremioTable> canonicalKeyTables) {
    super(delegate);
    this.tablesByNamespaceKey = tablesByNamespaceKey;
    this.canonicalKeyAccessTime = canonicalKeyAccessTime;
    this.canonicalKeyResolutionCount = canonicalKeyResolutionCount;
    this.canonicalKeyTables = canonicalKeyTables;
  }

  private DremioTable putTable(NamespaceKey requestKey, DremioTable table) {
    if (table == null) {
      return null;
    }
    final DatasetConfig dataset = table.getDatasetConfig();
    if (dataset != null) {
      final NamespaceKey canonicalizedKey = new NamespaceKey(dataset.getFullPathList());
      tablesByNamespaceKey.put(canonicalizedKey, table);
    }
    tablesByNamespaceKey.put(requestKey, table);
    return table;
  }

  private boolean containsNamespaceKey(NamespaceKey key) {
    return tablesByNamespaceKey.containsKey(key);
  }

  private DremioTable getByNamespaceKey(NamespaceKey key) {
    return tablesByNamespaceKey.get(key);
  }

  private DremioTable removeByNamespaceKey(NamespaceKey key) {
    return tablesByNamespaceKey.remove(key);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    if (!containsNamespaceKey(key)) {
      return putTable(key, timedGet(() -> super.getTableNoResolve(key)));
    }
    return getByNamespaceKey(key);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    if (!containsNamespaceKey(key)) {
      return putTable(key, timedGet(() -> super.getTableNoColumnCount(key)));
    }
    return getByNamespaceKey(key);
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
  public DremioTable getTable(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    if (!containsNamespaceKey(resolved)) {
      return putTable(resolved, timedGet(() -> super.getTable(key)));
    }
    return getByNamespaceKey(resolved);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    if (!containsNamespaceKey(resolved)) {
      return putTable(resolved, timedGet(() -> super.getTableForQuery(key)));
    }
    return getByNamespaceKey(resolved);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return ImmutableList.copyOf(tablesByNamespaceKey.values());
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return new CachingCatalog(delegate.resolveCatalog(subject), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new CachingCatalog(delegate.resolveCatalog(sourceVersionMapping), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
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
    return new CachingCatalog(delegate.resolveCatalog(newDefaultSchema), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema, checkValidity), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
  }

  @Override
  public Catalog resolveCatalog(boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(checkValidity), tablesByNamespaceKey, canonicalKeyAccessTime, canonicalKeyResolutionCount, canonicalKeyTables);
  }

  @Override
  public Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {
    Catalog newDelegate = delegate.visit(catalogRewrite);
    if(newDelegate == delegate) {
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
      String path = table.getPath().getSchemaPath();
      canonicalKeyResolutionCount.compute(path, (k, v) -> (v == null) ? 1 : v + 1);
      canonicalKeyAccessTime.compute(path, (k, v) -> (v == null) ? end-start : v + (end-start));
      canonicalKeyTables.put(path, table);
    }
    return table;
  }

  /**
   * Logs time spent accessing the catalog for any table or view.
   */
  @Override
  public void addCatalogStats() {
    long totalTime = canonicalKeyAccessTime.values().stream().mapToLong(Long::longValue).sum();
    long totalResolutions = canonicalKeyResolutionCount.values().stream().mapToLong(Long::longValue).sum();
    getMetadataStatsCollector().addDatasetStat(String.format("Catalog Access for %d Total Dataset(s)", canonicalKeyResolutionCount.keySet().size()),
      String.format("using %d resolved key(s)", totalResolutions), totalTime);
    canonicalKeyAccessTime.entrySet().stream().sorted(Comparator.comparing(Map.Entry<String, Long>::getValue).reversed()).
      forEach(entry -> getMetadataStatsCollector().addDatasetStat(
        String.format("Catalog Access for %s (%s)", entry.getKey(), canonicalKeyTables.get(entry.getKey()).getDatasetConfig().getType()),
        String.format("using %d resolved key(s)", canonicalKeyResolutionCount.get(entry.getKey())),
        canonicalKeyAccessTime.get(entry.getKey())));
  }

}
