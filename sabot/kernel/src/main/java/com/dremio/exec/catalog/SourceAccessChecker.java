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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
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
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;

/**
 * Catalog decorator that handles source access checks.
 */
class SourceAccessChecker implements Catalog {

  private final MetadataRequestOptions options;
  private final Catalog delegate;

  private SourceAccessChecker(MetadataRequestOptions options, Catalog delegate) {
    this.options = options;
    this.delegate = delegate;
  }

  private static boolean isInternal(String name) {
    return name.startsWith("__") || name.startsWith("$");
  }

  private boolean isInvisible(NamespaceKey key) {
    final String root = key.getRoot();
    return isInternal(root) &&
        !"__home".equalsIgnoreCase(root) &&
        !"$scratch".equalsIgnoreCase(root);
  }

  private void throwIfInvisible(NamespaceKey key) {
    if (isInvisible(key)) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }
  }

  private DremioTable getIfVisible(NamespaceKey key, Supplier<DremioTable> tableSupplier) {
    if (key != null && isInvisible(key)) {
      return null;
    }

    final DremioTable table = tableSupplier.get();
    return table == null || isInvisible(table.getPath()) ? null : table;
  }

  @Override
  public void validateSelection() {
    delegate.validateSelection();
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTableNoResolve(key));
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTableNoColumnCount(key));
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey key, DatasetConfig dataset) throws NamespaceException {
    delegate.addOrUpdateDataset(key, dataset);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return getIfVisible(null, () -> delegate.getTable(datasetId));
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTable(key));
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
    if (isInvisible(path)) {
      return false;
    }

    return delegate.containerExists(path);
  }

  @Override
  public NamespaceKey resolveToDefault(NamespaceKey key) {
    return delegate.resolveToDefault(key);
  }

  @Override
  public MetadataStatsCollector getMetadataStatsCollector() {
    return delegate.getMetadataStatsCollector();
  }

  @Override
  public void createEmptyTable(NamespaceKey key, BatchSchema batchSchema, WriterOptions writerOptions) {
    delegate.createEmptyTable(key, batchSchema, writerOptions);
  }

  @Override
  public CreateTableEntry createNewTable(
    NamespaceKey key,
    IcebergTableProps icebergTableProps,
    WriterOptions writerOptions,
    Map<String, Object> storageOptions) {
    throwIfInvisible(key);
    return delegate.createNewTable(key, icebergTableProps, writerOptions, storageOptions);
  }

  @Override
  public void createView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    throwIfInvisible(key);

    delegate.createView(key, view, attributes);
  }

  @Override
  public void updateView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    throwIfInvisible(key);

    delegate.updateView(key, view, attributes);
  }

  @Override
  public void dropView(NamespaceKey key) throws IOException {
    throwIfInvisible(key);

    delegate.dropView(key);
  }

  @Override
  public void dropTable(NamespaceKey key) {
    throwIfInvisible(key);

    delegate.dropTable(key);
  }

  @Override
  public void forgetTable(NamespaceKey key) {
    throwIfInvisible(key);
    delegate.forgetTable(key);
  }

  @Override
  public void truncateTable(NamespaceKey key) {
    throwIfInvisible(key);
    delegate.truncateTable(key);
  }

  @Override
  public void addColumns(NamespaceKey table, List<Field> colsToAdd) {
    throwIfInvisible(table);
    delegate.addColumns(table, colsToAdd);
  }

  @Override
  public void dropColumn(NamespaceKey table, String columnToDrop) {
    throwIfInvisible(table);
    delegate.dropColumn(table, columnToDrop);
  }

  @Override
  public void changeColumn(NamespaceKey table, String columnToChange, Field fieldFromSqlColDeclaration) {
    throwIfInvisible(table);
    delegate.changeColumn(table, columnToChange, fieldFromSqlColDeclaration);
  }

  @Override
  public void createDataset(NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator) {
    throwIfInvisible(key);

    delegate.createDataset(key, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    throwIfInvisible(key);

    return delegate.refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    throwIfInvisible(key);

    return delegate.refreshSourceStatus(key);
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key,
      List<String> partitionColumns,
      List<String> partitionValues
  ) throws PartitionNotFoundException {
    throwIfInvisible(key);

    return delegate.getSubPartitions(key, partitionColumns, partitionValues);
  }

  @Override
  public boolean createOrUpdateDataset(
      NamespaceService userNamespaceService,
      NamespaceKey source,
      NamespaceKey datasetPath,
      DatasetConfig datasetConfig,
      NamespaceAttribute... attributes
  ) throws NamespaceException {
    throwIfInvisible(source);

    return delegate.createOrUpdateDataset(userNamespaceService, source, datasetPath, datasetConfig, attributes);
  }

  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    throwIfInvisible(datasetKey);

    delegate.updateDatasetSchema(datasetKey, newSchema);
  }

  @Override
  public void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    throwIfInvisible(datasetKey);

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
  public void updateSource(SourceConfig config, NamespaceAttribute... attributes) {
    delegate.updateSource(config, attributes);
  }

  @Override
  public void deleteSource(SourceConfig config) {
    delegate.deleteSource(config);
  }

  private <T> Iterable<T> checkAndGetList(NamespaceKey path, Supplier<Iterable<T>> iterableSupplier) {
    if (path.size() != 0 && isInvisible(path)) {
      return Collections.emptyList();
    }

    return iterableSupplier.get();
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    return checkAndGetList(path, () -> delegate.listSchemas(path));
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    return checkAndGetList(path, () -> delegate.listDatasets(path));
  }

  @Override
  public Collection<org.apache.calcite.schema.Function> getFunctions(NamespaceKey path) {
    return (Collection<org.apache.calcite.schema.Function>) checkAndGetList(path, () -> delegate.getFunctions(path));
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return delegate.getDefaultSchema();
  }

  @Override
  public Catalog resolveCatalog(boolean checkValidity) {
    return secureIfNeeded(options.cloneWith(options.getSchemaConfig().getUserName(), options.getSchemaConfig().getDefaultSchema(), checkValidity),
      delegate.resolveCatalog(checkValidity));
  }

  @Override
  public Catalog resolveCatalog(String username) {
    return secureIfNeeded(options.cloneWith(username, options.getSchemaConfig().getDefaultSchema(), options.checkValidity()),
        delegate.resolveCatalog(username));
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema) {
    return secureIfNeeded(options.cloneWith(username, newDefaultSchema, options.checkValidity()),
        delegate.resolveCatalog(username, newDefaultSchema));
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return secureIfNeeded(options.cloneWith(username, newDefaultSchema, checkValidity),
      delegate.resolveCatalog(username, newDefaultSchema, checkValidity));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return secureIfNeeded(options.cloneWith(options.getSchemaConfig().getUserName(), newDefaultSchema, options.checkValidity()), delegate.resolveCatalog(newDefaultSchema));
  }

  /**
   * Decorates the given catalog to check source access, if enabled by the options.
   *
   * @param options  options
   * @param delegate delegate catalog
   * @return decorated catalog, if needed
   */
  public static Catalog secureIfNeeded(MetadataRequestOptions options, Catalog delegate) {
    return options.getSchemaConfig().exposeInternalSources() ||
        SystemUser.isSystemUserName(options.getSchemaConfig().getUserName())
        ? delegate : new SourceAccessChecker(options, delegate);
  }

  @Override
  public boolean alterDataset(final NamespaceKey key, final Map<String, AttributeValue> attributes) {
    throwIfInvisible(key);
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
