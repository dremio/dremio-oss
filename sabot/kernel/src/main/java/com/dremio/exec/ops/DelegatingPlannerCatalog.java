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
package com.dremio.exec.ops;

import java.util.Collection;
import java.util.Map;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.schema.Function;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.namespace.NamespaceKey;

/**
 * PlannerCatalog that doesn't add any planning specific behavior and passes metadata requests
 * to the underlying catalog.  Used by auto-complete.
 */
public final class DelegatingPlannerCatalog<T extends SimpleCatalog<T>> implements PlannerCatalog {

  private T delegate;
  private DelegatingPlannerCatalog(T delegate) {
    this.delegate = delegate;
  }

  public static <T extends SimpleCatalog<?>> PlannerCatalog newInstance(T catalog) {
    return new DelegatingPlannerCatalog(catalog);
  }

  @Override
  public DremioTable getTableWithSchema(NamespaceKey key) {
    return delegate.getTable(key);
  }

  @Override
  public DremioTable getTableIgnoreSchema(NamespaceKey key) {
    return delegate.getTableNoResolve(key);
  }

  @Override
  public DremioTable getTableSnapshotWithSchema(NamespaceKey key, TableVersionContext versionContext) {
    return delegate.getTableSnapshot(key, versionContext);
  }

  @Override
  public DremioTable getTableSnapshotIgnoreSchema(NamespaceKey key, TableVersionContext versionContext) {
    return delegate.getTableSnapshotNoResolve(key, versionContext);
  }

  @Override
  public DremioTable getValidatedTableWithSchema(NamespaceKey key) {
    return delegate.getTableForQuery(key);
  }

  @Override
  public DremioTable getValidatedTableIgnoreSchema(NamespaceKey key) {
    return delegate.getTableNoResolve(key);
  }

  @Override
  public DremioTable getValidatedTableSnapshotWithSchema(NamespaceKey key, TableVersionContext context) {
    return delegate.getTableSnapshotForQuery(key, context);
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path, SimpleCatalog.FunctionType functionType) {
    return delegate.getFunctions(path, functionType);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearDatasetCache(NamespaceKey dataset, TableVersionContext versionContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearConvertedCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return delegate.getDefaultSchema();
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return JavaTypeFactoryImpl.INSTANCE;
  }

  @Override
  public boolean containerExists(NamespaceKey path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateSelection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(NamespaceKey newDefaultSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(CatalogIdentity subject) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(Map<String, VersionContext> sourceVersionMapping) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(java.util.function.Function<Catalog, Catalog> catalogTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Catalog getMetadataCatalog() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dispose() {
    throw new UnsupportedOperationException();
  }
}
