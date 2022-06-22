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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  private final Map<NamespaceKey, DremioTable> tablesByNamespaceKey;

  CachingCatalog(Catalog delegate) {
    this(delegate, new ConcurrentHashMap<NamespaceKey, DremioTable>());
  }

  private CachingCatalog(Catalog delegate, Map<NamespaceKey, DremioTable> tablesByNamespaceKey) {
    super(delegate);
    this.tablesByNamespaceKey = tablesByNamespaceKey;
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

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    if (!tablesByNamespaceKey.containsKey(key)) {
      return putTable(key, super.getTableNoResolve(key));
    }
    return tablesByNamespaceKey.get(key);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    if (!tablesByNamespaceKey.containsKey(key)) {
      return putTable(key, super.getTableNoColumnCount(key));
    }
    return tablesByNamespaceKey.get(key);
  }

  @Override
  public void updateView(final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes) throws IOException {
    tablesByNamespaceKey.remove(key);
    super.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException {
    tablesByNamespaceKey.remove(key);
    super.dropView(key, viewOptions);
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    if (!tablesByNamespaceKey.containsKey(resolved)) {
      return putTable(resolved, super.getTable(key));
    }
    return tablesByNamespaceKey.get(resolved);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if (resolved == null) {
      resolved = key;
    }
    if (!tablesByNamespaceKey.containsKey(resolved)) {
      return putTable(resolved, super.getTableForQuery(key));
    }
    return tablesByNamespaceKey.get(resolved);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return ImmutableList.copyOf(tablesByNamespaceKey.values());
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return new CachingCatalog(delegate.resolveCatalog(subject), tablesByNamespaceKey);
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new CachingCatalog(delegate.resolveCatalog(sourceVersionMapping), tablesByNamespaceKey);
  }

  @Override
  public Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext) {
    return new CachingCatalog(delegate.resolveCatalogResetContext(sourceName, versionContext));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CachingCatalog(delegate.resolveCatalog(newDefaultSchema), tablesByNamespaceKey);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema), tablesByNamespaceKey);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(subject, newDefaultSchema, checkValidity), tablesByNamespaceKey);
  }

  @Override
  public Catalog resolveCatalog(boolean checkValidity) {
    return new CachingCatalog(delegate.resolveCatalog(checkValidity), tablesByNamespaceKey);
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
}
