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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.schema.Function;

import com.dremio.common.expression.CompleteType;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;

/**
 * Delegating implementation of {@link Catalog}
 */
public class DelegatingCatalog implements Catalog {

  protected final Catalog delegate;

  public DelegatingCatalog(Catalog delegate) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate catalog required");
  }

  @Override
  public void validateSelection() {
    delegate.validateSelection();
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return delegate.getTableNoResolve(key);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return delegate.getTableNoColumnCount(key);
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey key, DatasetConfig dataset) throws NamespaceException {
    delegate.addOrUpdateDataset(key, dataset);
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return delegate.getTable(key);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return delegate.getTable(datasetId);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return delegate.getAllRequestedTables();
  }

  @Override
  public NamespaceKey resolveSingle(NamespaceKey key) {
    return delegate.resolveSingle(key);
  }

  @Override
  public boolean containerExists(NamespaceKey path) {
    return delegate.containerExists(path);
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    return delegate.listSchemas(path);
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    return delegate.listDatasets(path);
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path) {
    return delegate.getFunctions(path);
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return delegate.getDefaultSchema();
  }

  @Override
  public NamespaceKey resolveToDefault(NamespaceKey key) {
    return delegate.resolveToDefault(key);
  }

  @Override
  public Catalog resolveCatalog(boolean checkValidity) {
    return delegate.resolveCatalog(checkValidity);
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema) {
    return delegate.resolveCatalog(username, newDefaultSchema);
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return delegate.resolveCatalog(username, newDefaultSchema, checkValidity);
  }

  @Override
  public Catalog resolveCatalog(String username) {
    return delegate.resolveCatalog(username);
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return delegate.resolveCatalog(newDefaultSchema);
  }

  @Override
  public MetadataStatsCollector getMetadataStatsCollector() {
    return delegate.getMetadataStatsCollector();
  }

  @Override
  public CreateTableEntry createNewTable(NamespaceKey key, IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions, Map<String, Object> storageOptions) {
    return delegate.createNewTable(key, icebergTableProps, writerOptions, storageOptions);
  }

  @Override
  public void createEmptyTable(NamespaceKey key, BatchSchema batchSchema, WriterOptions writerOptions) {
    delegate.createEmptyTable(key, batchSchema, writerOptions);
  }

  @Override
  public void createView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    delegate.createView(key, view, attributes);
  }

  @Override
  public void updateView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    delegate.updateView(key, view, attributes);
  }

  @Override
  public void dropView(NamespaceKey key) throws IOException {
    delegate.dropView(key);
  }

  @Override
  public void dropTable(NamespaceKey key) {
    delegate.dropTable(key);
  }

  @Override
  public void forgetTable(NamespaceKey key) {
    delegate.forgetTable(key);
  }

  @Override
  public void truncateTable(NamespaceKey key) {
    delegate.truncateTable(key);
  }

  @Override
  public void addColumns(NamespaceKey table, List<Field> colsToAdd) {
    delegate.addColumns(table, colsToAdd);
  }

  @Override
  public void dropColumn(NamespaceKey table, String columnToDrop) {
    delegate.dropColumn(table, columnToDrop);
  }

  @Override
  public void changeColumn(NamespaceKey table, String columnToChange, Field fieldFromSqlColDeclaration) {
    delegate.changeColumn(table, columnToChange, fieldFromSqlColDeclaration);
  }

  @Override
  public void createDataset(NamespaceKey key, com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator) {
    delegate.createDataset(key, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    return delegate.refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    return delegate.refreshSourceStatus(key);
  }

  @Override
  public Iterable<String> getSubPartitions(NamespaceKey key, List<String> partitionColumns, List<String> partitionValues) throws PartitionNotFoundException {
    return delegate.getSubPartitions(key, partitionColumns, partitionValues);
  }

  @Override
  public boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException {
    return delegate.createOrUpdateDataset(userNamespaceService, source, datasetPath, datasetConfig, attributes);
  }

  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    delegate.updateDatasetSchema(datasetKey, newSchema);
  }

  @Override
  public void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    delegate.updateDatasetField(datasetKey, originField, fieldSchema);
  }

  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    return delegate.getSource(name);
  }

  @Override
  public void createSource(SourceConfig config, NamespaceAttribute... attributes) {
    delegate.createSource(config, attributes);
  }

  @Override
  public void updateSource(SourceConfig config,  NamespaceAttribute... attributes) {
    delegate.updateSource(config, attributes);
  }

  @Override
  public void deleteSource(SourceConfig config) {
    delegate.deleteSource(config);
  }

  @Override
  public boolean alterDataset(final NamespaceKey key, final Map<String, AttributeValue> attributes) {
    return delegate.alterDataset(key, attributes);
  }

  @Override
  public Iterator<com.dremio.service.catalog.Catalog> listCatalogs(SearchQuery searchQuery) {
    return delegate.listCatalogs(searchQuery);
  }

  @Override
  public Iterator<Schema> listSchemata(SearchQuery searchQuery) {
    return delegate.listSchemata(searchQuery);
  }

  @Override
  public Iterator<Table> listTables(SearchQuery searchQuery) {
    return delegate.listTables(searchQuery);
  }

  @Override
  public Iterator<com.dremio.service.catalog.View> listViews(SearchQuery searchQuery) {
    return delegate.listViews(searchQuery);
  }

  @Override
  public Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery) {
    return delegate.listTableSchemata(searchQuery);
  }
}
