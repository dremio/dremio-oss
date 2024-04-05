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
package com.dremio.exec.store.dfs.system;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.dfs.MayBeDistFileSystemPlugin;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Provider;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A custom implementation of a storage plugin used for system iceberg tables */
public class SystemIcebergTablesStoragePlugin
    extends MayBeDistFileSystemPlugin<SystemIcebergTablesStoragePluginConfig> {

  private static final Logger logger =
      LoggerFactory.getLogger(SystemIcebergTablesStoragePlugin.class);
  private final Lock createSystemTablesLock = new ReentrantLock();

  /**
   * Constructs a new SystemIcebergTablesStoragePlugin instance with the given configuration,
   * context, name, and ID provider.
   *
   * @param config The configuration for the storage plugin.
   * @param context The SabotContext associated with this storage plugin.
   * @param name The name of the storage plugin.
   * @param idProvider The provider for the StoragePluginId.
   */
  public SystemIcebergTablesStoragePlugin(
      SystemIcebergTablesStoragePluginConfig config,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    List<String> components = datasetPath.getComponents();
    SystemIcebergTableMetadata tableMetadata = getTableMetadata(datasetPath.getComponents());
    if (components == null || components.isEmpty()) {
      return Optional.empty();
    }
    Preconditions.checkState(components.size() == 2, "Unexpected number of components in path");
    Preconditions.checkState(
        components
                .get(0)
                .equals(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME)
            && components.get(1).equals(tableMetadata.getTableName()));

    SystemIcebergTablesExecutionDatasetAccessor datasetHandle =
        new SystemIcebergTablesExecutionDatasetAccessor(
            datasetPath,
            Suppliers.ofInstance(getTable(tableMetadata.getTableLocation())),
            getFsConfCopy(),
            Table::currentSnapshot,
            this,
            (t, s) -> t.schema(),
            getContext().getOptionManager());

    return Optional.of(datasetHandle);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options)
      throws ConnectorException {
    SystemIcebergTablesExecutionDatasetAccessor metadataProvider =
        datasetHandle.unwrap(SystemIcebergTablesExecutionDatasetAccessor.class);
    return metadataProvider.getDatasetMetadata(options);
  }

  /**
   * Retrieves a view table representing the table. This method constructs a view table based on the
   * provided table schema path and user name. The view table is constructed to display data from
   * the table with customized field types and filtering criteria.
   *
   * @param tableSchemaPath The list of strings representing the table schema path.
   * @param userName The user name for which to filter the view.
   * @return The constructed ViewTable representing the given table with customized field types and
   *     filtering.
   */
  public ViewTable getViewTable(List<String> tableSchemaPath, String userName) {
    BatchSchema batchSchema;
    String tableName;
    if (SystemIcebergViewMetadataFactory.isSupportedViewPath(tableSchemaPath)) {
      SystemIcebergViewMetadata viewMetadata = getViewMetadata(tableSchemaPath);
      batchSchema = viewMetadata.getBatchSchema();
      tableName = viewMetadata.getTableName();
    } else {
      SystemIcebergTableMetadata tableMetadata = getTableMetadata(tableSchemaPath);
      createEmptySystemIcebergTableIfNotExists(tableSchemaPath);
      batchSchema = tableMetadata.getBatchSchema();
      tableName = tableMetadata.getTableName();
    }

    View view =
        Views.fieldTypesToView(
            tableName,
            prepareViewQuery(tableSchemaPath, userName),
            ViewFieldsHelper.getBatchSchemaFields(batchSchema),
            null);
    return new ViewTable(
        new NamespaceKey(tableSchemaPath),
        view,
        CatalogUser.from(SystemUser.SYSTEM_USERNAME),
        batchSchema);
  }

  /**
   * Prepares a SQL query string for constructing a view based on the provided table metadata and
   * user name.
   *
   * @param tableSchemaPath The list of strings representing the table schema path.
   * @param userName The user name for which to filter the view.
   * @return The prepared SQL query string.
   */
  private String prepareViewQuery(List<String> tableSchemaPath, String userName) {
    if (SystemIcebergViewMetadataFactory.isSupportedViewPath(tableSchemaPath)) {
      return getViewMetadata(tableSchemaPath).getViewQuery(userName);
    } else {
      return getTableMetadata(tableSchemaPath).getViewQuery();
    }
  }

  /**
   * Checks if an iceberg table exists at the specified table location.
   *
   * @param tableLocation The location of the table to check.
   * @return True if the table exists; otherwise, false.
   */
  public boolean isTableExists(String tableLocation) {
    try {
      getIcebergModel().getIcebergTable(getIcebergModel().getTableIdentifier(tableLocation));
    } catch (UserException e) {
      return false;
    }
    return true;
  }

  /**
   * Retrieves the iceberg table from the specified table location if it exists.
   *
   * @param tableLocation The location of the table.
   * @return The iceberg table if it exists; otherwise, null.
   */
  public Table getTable(String tableLocation) {
    if (isTableExists(tableLocation)) {
      return getIcebergModel().getIcebergTable(getIcebergModel().getTableIdentifier(tableLocation));
    }
    return null;
  }

  /**
   * Retrieves the table metadata.
   *
   * @return An object describing the iceberg table.
   */
  public SystemIcebergTableMetadata getTableMetadata(List<String> tableSchemaPath) {
    return SystemIcebergTableMetadataFactory.getTableMetadata(
        getName(),
        getConfig().getPath().toString(),
        getContext().getOptionManager(),
        tableSchemaPath);
  }

  /**
   * Checks if a view specified by the given schema path is supported. A view is considered
   * supported if it corresponds to either a supported table or view.
   *
   * @param tableSchemaPath A list of strings representing the schema path for the view.
   * @return {@code true} if the view is supported, {@code false} otherwise.
   */
  public boolean isSupportedTablePath(List<String> tableSchemaPath) {
    return SystemIcebergTableMetadataFactory.isSupportedTablePath(tableSchemaPath)
        || SystemIcebergViewMetadataFactory.isSupportedViewPath(tableSchemaPath);
  }

  /**
   * Retrieves the view metadata for a given view schema path using the context's option manager.
   *
   * @param viewSchemaPath A list of strings representing the schema path for the view.
   * @return The {@link SystemIcebergViewMetadata} object representing the metadata for the
   *     specified view.
   */
  private SystemIcebergViewMetadata getViewMetadata(List<String> viewSchemaPath) {
    return SystemIcebergViewMetadataFactory.getViewMetadata(
        getContext().getOptionManager(), viewSchemaPath);
  }

  /**
   * Refreshes the dataset associated with the systemIcebergTable within {@link
   * SystemIcebergTablesStoragePlugin}. This method triggers the refresh of metadata information for
   * the dataset to ensure it is up-to-date and accurately reflects the current state.The refresh
   * process may involve fetching updates from the underlying data source and updating the metadata.
   */
  public void refreshDataset(List<String> tableSchemaPath) {

    SystemIcebergTableMetadata metadata = getTableMetadata(tableSchemaPath);

    if (!isTableExists(metadata.getTableLocation())) {
      return;
    }

    NamespaceKey tableNamespaceKey = metadata.getNamespaceKey();

    // Build dataset retrieval options with desired settings
    DatasetRetrievalOptions options =
        DatasetRetrievalOptions.newBuilder()
            .setAutoPromote(false)
            .setForceUpdate(true)
            .build()
            .withFallback(DatasetRetrievalOptions.DEFAULT);

    // Get the plugin's context and catalog service
    CatalogService catalogService = getContext().getCatalogService();

    // Retrieve the catalog with a specified schema configuration
    Catalog catalog =
        catalogService.getCatalog(
            MetadataRequestOptions.of(
                SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build()));

    // Refresh the dataset metadata using the configured options
    catalog.refreshDataset(tableNamespaceKey, options);
  }

  /**
   * Creates empty Iceberg tables for all supported system tables if they do not already exist. The
   * method iterates through the list of supported system tables and calls {@link
   * #createEmptySystemIcebergTableIfNotExists(List)} for each table.
   *
   * @see #createEmptySystemIcebergTableIfNotExists(List)
   */
  public void createEmptySystemIcebergTablesIfNotExists() {
    for (String tableName : SystemIcebergTableMetadataFactory.SUPPORTED_TABLES) {
      createEmptySystemIcebergTableIfNotExists(ImmutableList.of(tableName));
    }
  }

  /**
   * Creates an empty Iceberg table for the specified system table if it does not already exist. The
   * method checks for the existence of the table and creates an empty table if it is not found. It
   * uses a lock to ensure thread safety, preventing multiple threads from attempting to create the
   * table simultaneously.
   *
   * @param tableSchemaPath The schema path of the system table for which to create an empty Iceberg
   *     table.
   */
  private void createEmptySystemIcebergTableIfNotExists(List<String> tableSchemaPath) {
    SystemIcebergTableMetadata tableMetadata = getTableMetadata(tableSchemaPath);
    if (!isTableExists(tableMetadata.getTableLocation())) {
      createSystemTablesLock.lock();
      try {
        if (!isTableExists(tableMetadata.getTableLocation())) {
          logger.debug(
              "Iceberg table at location {} does not exists. Creating empty system iceberg table",
              tableMetadata.getTableLocation());
          ImmutableIcebergWriterOptions icebergWriterOptions =
              new ImmutableIcebergWriterOptions.Builder()
                  .setIcebergTableProps(tableMetadata.getIcebergTablePropsForCreate())
                  .build();
          ImmutableTableFormatWriterOptions tableFormatWriterOptions =
              new ImmutableTableFormatWriterOptions.Builder()
                  .setIcebergSpecificOptions(icebergWriterOptions)
                  .build();
          SchemaConfig schemaConfig =
              SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build();
          WriterOptions writerOptions =
              new WriterOptions(
                  null,
                  null,
                  null,
                  null,
                  null,
                  false,
                  Long.MAX_VALUE,
                  tableFormatWriterOptions,
                  null,
                  false);
          createEmptyTable(
              tableMetadata.getNamespaceKey(),
              schemaConfig,
              tableMetadata.getBatchSchema(),
              writerOptions);
          logger.debug(
              "Iceberg table {} created at location {}",
              tableMetadata.getNamespaceKey(),
              tableMetadata.getTableLocation());
        }
      } finally {
        createSystemTablesLock.unlock();
      }
      refreshDataset(tableSchemaPath);
    }
  }
}
