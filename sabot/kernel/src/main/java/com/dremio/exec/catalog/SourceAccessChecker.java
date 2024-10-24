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

import com.dremio.catalog.exception.UnsupportedForgetTableException;
import com.dremio.catalog.model.CatalogEntityId;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.concurrent.bulk.BulkFunction;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.concurrent.bulk.ValueTransformer;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
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
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.arrow.vector.types.pojo.Field;

/** Catalog decorator that handles source access checks. */
final class SourceAccessChecker implements Catalog {

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
    return isInternal(root)
        && !"__home".equalsIgnoreCase(root)
        && !"$scratch".equalsIgnoreCase(root);
  }

  private boolean isInvisible(CatalogEntityKey key) {
    final String root = key.getRootEntity();
    return isInternal(root)
        && !"__home".equalsIgnoreCase(root)
        && !"$scratch".equalsIgnoreCase(root);
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

  private DremioTable getIfVisible(CatalogEntityKey key, Supplier<DremioTable> tableSupplier) {
    if (key != null && isInvisible(key)) {
      return null;
    }

    final DremioTable table = tableSupplier.get();
    return table == null || isInvisible(table.getPath()) ? null : table;
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTableNoResolve(key));
  }

  @Override
  public DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey) {
    return delegate.getTableNoResolve(catalogEntityKey);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTableNoColumnCount(key));
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey key, DatasetConfig dataset)
      throws NamespaceException {
    delegate.addOrUpdateDataset(key, dataset);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return getIfVisible((CatalogEntityKey) null, () -> delegate.getTable(datasetId));
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTable(key));
  }

  @Override
  public DremioTable getTable(CatalogEntityKey key) {
    return getIfVisible(key, () -> delegate.getTable(key));
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return getIfVisible(key, () -> delegate.getTableForQuery(key));
  }

  @Override
  public String getDatasetId(NamespaceKey key) {
    if (isInvisible(key)) {
      return null;
    }

    return delegate.getDatasetId(key);
  }

  @Override
  public DremioTable getTableSnapshotForQuery(CatalogEntityKey catalogEntityKey) {
    return delegate.getTableSnapshotForQuery(catalogEntityKey);
  }

  @Override
  public DatasetType getDatasetType(CatalogEntityKey key) {
    if (isInvisible(key.toNamespaceKey())) {
      return null;
    }

    return delegate.getDatasetType(key);
  }

  @Override
  public DremioTable getTableSnapshot(CatalogEntityKey catalogEntityKey) {
    return delegate.getTableSnapshot(catalogEntityKey);
  }

  @Nonnull
  @Override
  public Optional<TableMetadataVerifyResult> verifyTableMetadata(
      CatalogEntityKey key, TableMetadataVerifyRequest metadataVerifyRequest) {
    throw new UnsupportedOperationException();
  }

  private BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTableIfVisible(
      BulkRequest<NamespaceKey> keys,
      BulkFunction<NamespaceKey, Optional<DremioTable>> bulkFunction) {

    // partition the request keys into two sets - invisible keys and everything else
    java.util.function.Function<NamespaceKey, Boolean> partitionByInvisibility =
        key -> key != null && isInvisible(key);

    // define the bulk functions for each partition
    // - for invisible keys, just return Optional.empty()
    // - for everything else, call into the provided bulkFunction
    java.util.function.Function<Boolean, BulkFunction<NamespaceKey, Optional<DremioTable>>>
        partitionBulkFunctions =
            isInvisible ->
                isInvisible ? BulkFunction.withConstantResponse(Optional.empty()) : bulkFunction;

    // define a value transformer which checks visibility of the returned table, and converts
    // to an Optional.empty() if the table is invisible
    ValueTransformer<NamespaceKey, Optional<DremioTable>, NamespaceKey, Optional<DremioTable>>
        checkIfResponseIsInvisible =
            (key, unused, optTable) ->
                optTable.map(table -> isInvisible(table.getPath()) ? null : table);

    return keys.bulkPartitionAndHandleRequests(
        partitionByInvisibility,
        partitionBulkFunctions,
        java.util.function.Function.identity(),
        checkIfResponseIsInvisible);
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTables(
      BulkRequest<NamespaceKey> keys) {
    return bulkGetTableIfVisible(keys, delegate::bulkGetTables);
  }

  @Override
  public BulkResponse<NamespaceKey, Optional<DremioTable>> bulkGetTablesForQuery(
      BulkRequest<NamespaceKey> keys) {
    return bulkGetTableIfVisible(keys, delegate::bulkGetTablesForQuery);
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
  public boolean containerExists(CatalogEntityKey path) {
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
  public void createEmptyTable(
      NamespaceKey key, BatchSchema batchSchema, WriterOptions writerOptions) {
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
  public CreateTableEntry createNewTable(
      NamespaceKey key,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      boolean isResultsTable) {
    throwIfInvisible(key);
    return delegate.createNewTable(
        key, icebergTableProps, writerOptions, storageOptions, isResultsTable);
  }

  @Override
  public void createView(
      NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException {
    throwIfInvisible(key);

    delegate.createView(key, view, viewOptions, attributes);
  }

  @Override
  public void updateView(
      NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException {
    throwIfInvisible(key);

    delegate.updateView(key, view, viewOptions, attributes);
  }

  @Override
  public void dropView(NamespaceKey key, ViewOptions viewOptions) throws IOException {
    throwIfInvisible(key);

    delegate.dropView(key, viewOptions);
  }

  @Override
  public void dropTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    throwIfInvisible(key);

    delegate.dropTable(key, tableMutationOptions);
  }

  @Override
  public void alterTable(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      AlterTableOption alterTableOption,
      TableMutationOptions tableMutationOptions) {
    throwIfInvisible(key);
    delegate.alterTable(key, datasetConfig, alterTableOption, tableMutationOptions);
  }

  @Override
  public void forgetTable(NamespaceKey key) throws UnsupportedForgetTableException {
    throwIfInvisible(key);
    delegate.forgetTable(key);
  }

  @Override
  public void truncateTable(NamespaceKey key, TableMutationOptions tableMutationOptions) {
    throwIfInvisible(key);
    delegate.truncateTable(key, tableMutationOptions);
  }

  @Override
  public void rollbackTable(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {
    throwIfInvisible(key);
    delegate.rollbackTable(key, datasetConfig, rollbackOption, tableMutationOptions);
  }

  @Override
  public void addColumns(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      List<Field> colsToAdd,
      TableMutationOptions tableMutationOptions) {
    throwIfInvisible(table);
    delegate.addColumns(table, datasetConfig, colsToAdd, tableMutationOptions);
  }

  @Override
  public void dropColumn(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    throwIfInvisible(table);
    delegate.dropColumn(table, datasetConfig, columnToDrop, tableMutationOptions);
  }

  @Override
  public void changeColumn(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      String columnToChange,
      Field fieldFromSqlColDeclaration,
      TableMutationOptions tableMutationOptions) {
    throwIfInvisible(table);
    delegate.changeColumn(
        table, datasetConfig, columnToChange, fieldFromSqlColDeclaration, tableMutationOptions);
  }

  @Override
  public void createDataset(
      NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator) {
    throwIfInvisible(key);

    delegate.createDataset(key, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    throwIfInvisible(key);

    return delegate.refreshDataset(key, retrievalOptions);
  }

  @Override
  public UpdateStatus refreshDataset(
      NamespaceKey key,
      DatasetRetrievalOptions retrievalOptions,
      boolean isPrivilegeValidationNeeded) {
    throwIfInvisible(key);
    return delegate.refreshDataset(key, retrievalOptions, isPrivilegeValidationNeeded);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    throwIfInvisible(key);

    return delegate.refreshSourceStatus(key);
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key, List<String> partitionColumns, List<String> partitionValues)
      throws PartitionNotFoundException {
    throwIfInvisible(key);

    return delegate.getSubPartitions(key, partitionColumns, partitionValues);
  }

  @Override
  public boolean createOrUpdateDataset(
      NamespaceKey source,
      NamespaceKey datasetPath,
      DatasetConfig datasetConfig,
      NamespaceAttribute... attributes)
      throws NamespaceException {
    throwIfInvisible(source);

    return delegate.createOrUpdateDataset(source, datasetPath, datasetConfig, attributes);
  }

  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    throwIfInvisible(datasetKey);

    delegate.updateDatasetSchema(datasetKey, newSchema);
  }

  @Override
  public void updateDatasetField(
      NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    throwIfInvisible(datasetKey);

    delegate.updateDatasetField(datasetKey, originField, fieldSchema);
  }

  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    return delegate.getSource(name);
  }

  @Override
  public <T extends StoragePlugin> T getSource(String name, boolean skipStateCheck) {
    return delegate.getSource(name, skipStateCheck);
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

  private <T> Iterable<T> checkAndGetList(
      NamespaceKey path, Supplier<Iterable<T>> iterableSupplier) {
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
  public Collection<org.apache.calcite.schema.Function> getFunctions(
      CatalogEntityKey path, FunctionType functionType) {
    return delegate.getFunctions(path, functionType);
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return delegate.getDefaultSchema();
  }

  @Override
  public Catalog resolveCatalog(CatalogIdentity subject) {
    return secureIfNeeded(
        options.cloneWith(
            subject, options.getSchemaConfig().getDefaultSchema(), options.checkValidity()),
        delegate.resolveCatalog(subject));
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return secureIfNeeded(
        options.cloneWith(
            options.getSchemaConfig().getAuthContext().getSubject(),
            newDefaultSchema,
            options.checkValidity()),
        delegate.resolveCatalog(newDefaultSchema));
  }

  @Override
  public Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return secureIfNeeded(
        options.cloneWith(sourceVersionMapping), delegate.resolveCatalog(sourceVersionMapping));
  }

  @Override
  public Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext) {
    return secureIfNeeded(
        options.cloneWith(sourceName, versionContext),
        delegate.resolveCatalogResetContext(sourceName, versionContext));
  }

  /**
   * Decorates the given catalog to check source access, if enabled by the options.
   *
   * @param options options
   * @param delegate delegate catalog
   * @return decorated catalog, if needed
   */
  public static Catalog secureIfNeeded(MetadataRequestOptions options, Catalog delegate) {
    return options.getSchemaConfig().exposeInternalSources()
            || SystemUser.isSystemUserName(options.getSchemaConfig().getUserName())
        ? delegate
        : new SourceAccessChecker(options, delegate);
  }

  @Override
  public boolean alterDataset(
      final CatalogEntityKey catalogEntityKey, final Map<String, AttributeValue> attributes) {
    throwIfInvisible(catalogEntityKey.toNamespaceKey());
    return delegate.alterDataset(catalogEntityKey, attributes);
  }

  @Override
  public boolean alterColumnOption(
      final NamespaceKey key,
      String columnToChange,
      final String attributeName,
      final AttributeValue attributeValue) {
    throwIfInvisible(key);
    return delegate.alterColumnOption(key, columnToChange, attributeName, attributeValue);
  }

  @Override
  public void addPrimaryKey(
      NamespaceKey namespaceKey, List<String> columns, VersionContext statementSourceVersion) {
    delegate.addPrimaryKey(namespaceKey, columns, statementSourceVersion);
  }

  @Override
  public void dropPrimaryKey(NamespaceKey namespaceKey, VersionContext statementVersion) {
    delegate.dropPrimaryKey(namespaceKey, statementVersion);
  }

  @Override
  public List<String> getPrimaryKey(NamespaceKey namespaceKey) {
    return delegate.getPrimaryKey(namespaceKey);
  }

  @Override
  public boolean toggleSchemaLearning(NamespaceKey table, boolean enableSchemaLearning) {
    throwIfInvisible(table);
    return delegate.toggleSchemaLearning(table, enableSchemaLearning);
  }

  @Override
  public void alterSortOrder(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {
    delegate.alterSortOrder(table, datasetConfig, schema, sortOrderColumns, tableMutationOptions);
  }

  @Override
  public void updateTableProperties(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {
    delegate.updateTableProperties(
        table, datasetConfig, schema, tableProperties, tableMutationOptions, isRemove);
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
  public Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table) {
    return delegate.getColumnExtendedProperties(table);
  }

  @Override
  public Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {
    Catalog newDelegate = delegate.visit(catalogRewrite);
    return catalogRewrite.apply(new SourceAccessChecker(options, newDelegate));
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(
      String sourceName, VersionContext versionContext)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    return delegate.resolveVersionContext(sourceName, versionContext);
  }

  @Override
  public void validatePrivilege(NamespaceKey key, SqlGrant.Privilege privilege) {
    delegate.validatePrivilege(key, privilege);
  }

  @Override
  public void validateOwnership(CatalogEntityKey key) {
    delegate.validateOwnership(key);
  }

  @Override
  public void invalidateNamespaceCache(final NamespaceKey key) {
    delegate.invalidateNamespaceCache(key);
  }

  @Override
  public MetadataRequestOptions getMetadataRequestOptions() {
    return options;
  }

  @Override
  public void clearDatasetCache(NamespaceKey dataset, TableVersionContext context) {
    delegate.clearDatasetCache(dataset, context);
  }

  @Override
  public void clearPermissionCache(String sourceName) {
    delegate.clearPermissionCache(sourceName);
  }

  //// Begin: NamespacePassthrough Methods
  @Override
  public boolean existsById(CatalogEntityId id) {
    return delegate.existsById(id);
  }

  @Override
  public List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) {
    return delegate.getEntities(lookupKeys);
  }

  @Override
  public void addOrUpdateDataset(
      NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes)
      throws NamespaceException {
    delegate.addOrUpdateDataset(datasetPath, dataset, attributes);
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath)
      throws NamespaceException {
    return delegate.renameDataset(oldDatasetPath, newDatasetPath);
  }

  @Override
  public String getEntityIdByPath(NamespaceKey entityPath) throws NamespaceNotFoundException {
    return delegate.getEntityIdByPath(entityPath);
  }

  @Override
  public NameSpaceContainer getEntityByPath(NamespaceKey entityPath) throws NamespaceException {
    return delegate.getEntityByPath(entityPath);
  }

  @Override
  public DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException {
    return delegate.getDataset(datasetPath);
  }

  @Override
  public Iterable<NamespaceKey> getAllDatasets(final NamespaceKey parent)
      throws NamespaceException {
    return delegate.getAllDatasets(parent);
  }

  @Override
  public void deleteDataset(
      NamespaceKey datasetPath, String version, NamespaceAttribute... attributes)
      throws NamespaceException {
    delegate.deleteDataset(datasetPath, version, attributes);
  }

  @Override
  public List<Integer> getCounts(SearchTypes.SearchQuery... queries) throws NamespaceException {
    return delegate.getCounts(queries);
  }

  @Override
  public Iterable<Document<NamespaceKey, NameSpaceContainer>> find(FindByCondition condition) {
    return delegate.find(condition);
  }

  @Override
  public List<NameSpaceContainer> getEntitiesByIds(List<EntityId> ids) {
    return delegate.getEntitiesByIds(ids);
  }

  @Override
  public List<SourceConfig> getSourceConfigs() {
    return delegate.getSourceConfigs();
  }
  //// End: NamespacePassthrough Methods
}
