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
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
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
public abstract class DelegatingCatalog implements Catalog {

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
  public void addPrimaryKey(NamespaceKey namespaceKey, List<String> columns) {
    delegate.addPrimaryKey(namespaceKey, columns);
  }

  @Override
  public void dropPrimaryKey(NamespaceKey namespaceKey) {
    delegate.dropPrimaryKey(namespaceKey);
  }

  @Override
  public List<String> getPrimaryKey(NamespaceKey table) {
    return delegate.getPrimaryKey(table);
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return delegate.getTable(key);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return delegate.getTableForQuery(key);
  }

  @Override
  public String getDatasetId(NamespaceKey key) {
    return delegate.getDatasetId(key);
  }

  @Override
  public DremioTable getTableSnapshotForQuery(NamespaceKey key, TableVersionContext context) {
    return delegate.getTableSnapshotForQuery(key, context);
  }

  @Override
  public DremioTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
    return delegate.getTableSnapshot(key, context);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return delegate.getTable(datasetId);
  }

  @Override
  public Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table) {
    return delegate.getColumnExtendedProperties(table);
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
  public Collection<Function> getFunctions(NamespaceKey path,
    FunctionType functionType) {
    return delegate.getFunctions(path, functionType);
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
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema) {
    return delegate.resolveCatalog(subject, newDefaultSchema);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return delegate.resolveCatalog(subject, newDefaultSchema, checkValidity);
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return delegate.resolveCatalog(subject);
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return delegate.resolveCatalog(newDefaultSchema);
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return delegate.resolveCatalog(sourceVersionMapping);
  }

  @Override
  public CreateTableEntry createNewTable(NamespaceKey key, IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions, Map<String, Object> storageOptions) {
    return delegate.createNewTable(key, icebergTableProps, writerOptions, storageOptions);
  }

  @Override
  public CreateTableEntry createNewTable(NamespaceKey key, IcebergTableProps icebergTableProps, WriterOptions writerOptions, Map<String, Object> storageOptions, boolean isResultsTable) {
    return delegate.createNewTable(key, icebergTableProps, writerOptions, storageOptions, isResultsTable);

  }


  @Override
  public void createEmptyTable(NamespaceKey key, BatchSchema batchSchema, WriterOptions writerOptions) {
    delegate.createEmptyTable(key, batchSchema, writerOptions);
  }

  @Override
  public void createView(NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes) throws IOException {
    delegate.createView(key, view, viewOptions, attributes);
  }

  @Override
  public void updateView(NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes) throws IOException {
    delegate.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(NamespaceKey key, ViewOptions viewOptions) throws IOException {
    delegate.dropView(key, viewOptions);
  }

  @Override
  public void dropTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    delegate.dropTable(key, tableMutationOptions);
  }

  @Override
  public void forgetTable(NamespaceKey key) {
    delegate.forgetTable(key);
  }

  @Override
  public void alterTable(NamespaceKey key, DatasetConfig datasetConfig, AlterTableOption alterTableOption, TableMutationOptions tableMutationOptions) {
    delegate.alterTable(key, datasetConfig, alterTableOption, tableMutationOptions);
  }

  @Override
  public void truncateTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    delegate.truncateTable(key, tableMutationOptions);
  }

  @Override
  public void rollbackTable(NamespaceKey path, DatasetConfig datasetConfig, RollbackOption rollbackOption, TableMutationOptions tableMutationOptions) {
    delegate.rollbackTable(path, datasetConfig, rollbackOption, tableMutationOptions);
  }

  @Override
  public void addColumns(NamespaceKey table, DatasetConfig datasetConfig, List<Field> colsToAdd, TableMutationOptions tableMutationOptions) {
    delegate.addColumns(table, datasetConfig, colsToAdd, tableMutationOptions);
  }

  @Override
  public void dropColumn(NamespaceKey table, DatasetConfig datasetConfig, String columnToDrop, TableMutationOptions tableMutationOptions) {
    delegate.dropColumn(table, datasetConfig, columnToDrop, tableMutationOptions);
  }

  @Override
  public void changeColumn(NamespaceKey table, DatasetConfig datasetConfig, String columnToChange, Field fieldFromSqlColDeclaration, TableMutationOptions tableMutationOptions) {
    delegate.changeColumn(table, datasetConfig, columnToChange, fieldFromSqlColDeclaration, tableMutationOptions);
  }

  @Override
  public boolean toggleSchemaLearning(NamespaceKey table, boolean enableSchemaLearning) {
      return delegate.toggleSchemaLearning(table, enableSchemaLearning);
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
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions, boolean isPrivilegeValidationNeeded) {
    return delegate.refreshDataset(key, retrievalOptions, isPrivilegeValidationNeeded);
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
  public boolean alterColumnOption(final NamespaceKey key, String columnToChange,
                            final String attributeName, final AttributeValue attributeValue) {
    return delegate.alterColumnOption(key, columnToChange, attributeName, attributeValue);
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

  @Override
  public ResolvedVersionContext resolveVersionContext(String sourceName, VersionContext versionContext) throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    return delegate.resolveVersionContext(sourceName, versionContext);
  }

  @Override
  public void validatePrivilege(NamespaceKey key, SqlGrant.Privilege privilege) {
    delegate.validatePrivilege(key, privilege);
  }

  @Override
  public boolean hasPrivilege(NamespaceKey key, SqlGrant.Privilege privilege) {
    return delegate.hasPrivilege(key, privilege);
  }

  @Override
  public void validateOwnership(NamespaceKey key) {
    delegate.validateOwnership(key);
  }

  @Override public void createFunction(NamespaceKey key,
    UserDefinedFunction userDefinedFunction,
    NamespaceAttribute... attributes) throws IOException {
    delegate.createFunction(key, userDefinedFunction, attributes);
  }

  @Override public void updateFunction(NamespaceKey key,
    UserDefinedFunction userDefinedFunction,
    NamespaceAttribute... attributes) throws IOException {
    delegate.updateFunction(key, userDefinedFunction, attributes);
  }

  @Override public void dropFunction(NamespaceKey key) throws IOException {
    delegate.dropFunction(key);
  }

  @Override public UserDefinedFunction getFunction(NamespaceKey key) throws IOException {
    return delegate.getFunction(key);
  }

  @Override public Iterable<UserDefinedFunction> getAllFunctions() throws IOException {
    return delegate.getAllFunctions();
  }

  @Override public void invalidateNamespaceCache(final NamespaceKey key) {
    delegate.invalidateNamespaceCache(key);
  }

  @Override public MetadataRequestOptions getMetadataRequestOptions() {
    return delegate.getMetadataRequestOptions();
  }
}
