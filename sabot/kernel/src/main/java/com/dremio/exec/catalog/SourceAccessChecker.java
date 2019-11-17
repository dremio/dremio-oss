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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.ischema.tables.TablesTable;
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

  private void throwIfInvisible(NamespaceKey key, String format, Object... args) {
    if (isInvisible(key)) {
      throw UserException.validationError()
          .message(format, args)
          .buildSilently();
    }
  }

  private DremioTable checkAndGetTable(NamespaceKey key, Supplier<DremioTable> tableSupplier) {
    if (key != null && isInvisible(key)) {
      return null;
    }

    final DremioTable table = tableSupplier.get();
    return table == null || isInvisible(table.getPath()) ? null : table;
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return checkAndGetTable(key, () -> delegate.getTableNoResolve(key));
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return checkAndGetTable(key, () -> delegate.getTableNoColumnCount(key));
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return checkAndGetTable(null, () -> delegate.getTable(datasetId));
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return checkAndGetTable(key, () -> delegate.getTable(key));
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
  public String getUser() {
    return delegate.getUser();
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
  public CreateTableEntry createNewTable(
      NamespaceKey key,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions
  ) {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    return delegate.createNewTable(key, writerOptions, storageOptions);
  }

  @Override
  public void createView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    delegate.createView(key, view, attributes);
  }

  @Override
  public void updateView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    delegate.updateView(key, view, attributes);
  }

  @Override
  public void dropView(NamespaceKey key) throws IOException {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    delegate.dropView(key);
  }

  @Override
  public void dropTable(NamespaceKey key) {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    delegate.dropTable(key);
  }

  @Override
  public void createDataset(NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator) {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    delegate.createDataset(key, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    return delegate.refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

    return delegate.refreshSourceStatus(key);
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key,
      List<String> partitionColumns,
      List<String> partitionValues
  ) throws PartitionNotFoundException {
    throwIfInvisible(key, "Unknown source %s", key.getRoot());

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
    throwIfInvisible(source, "Unknown source %s", source.getRoot());

    return delegate.createOrUpdateDataset(userNamespaceService, source, datasetPath, datasetConfig, attributes);
  }

  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    throwIfInvisible(datasetKey, "Unknown source %s", datasetKey.getRoot());

    delegate.updateDatasetSchema(datasetKey, newSchema);
  }

  @Override
  public void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    throwIfInvisible(datasetKey, "Unknown source %s", datasetKey.getRoot());

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

  @Override
  public boolean isSourceConfigMetadataImpacting(SourceConfig sourceConfig) {
    return delegate.isSourceConfigMetadataImpacting(sourceConfig);
  }

  @Override
  public SourceState getSourceState(String name) {
    return delegate.getSourceState(name);
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
  public Iterable<TablesTable.Table> listDatasets(NamespaceKey path) {
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
  public Catalog resolveCatalog(String username) {
    return secureIfNeeded(options.cloneWith(username, options.getSchemaConfig().getDefaultSchema()),
        delegate.resolveCatalog(username));
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema) {
    return secureIfNeeded(options.cloneWith(username, newDefaultSchema),
        delegate.resolveCatalog(username, newDefaultSchema));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return secureIfNeeded(options.cloneWith(getUser(), newDefaultSchema), delegate.resolveCatalog(newDefaultSchema));
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
}
