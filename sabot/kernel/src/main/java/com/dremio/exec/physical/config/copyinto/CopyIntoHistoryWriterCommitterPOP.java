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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginResolver;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.copyinto.CopyIntoHistoryEventHandler;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Optional;

/**
 * A custom implementation of a WriterCommitter Physical Operator used for handling COPY INTO
 * errors.
 */
@JsonTypeName("copy-into-history-writer-committer")
public class CopyIntoHistoryWriterCommitterPOP extends WriterCommitterPOP {

  private final SystemIcebergTablesStoragePlugin systemIcebergTablesStoragePlugin;

  /**
   * Constructs a new {@link CopyIntoHistoryWriterCommitterPOP} instance with the given parameters.
   *
   * @param props The operation properties.
   * @param tempLocation The temporary location for the writer committer.
   * @param finalLocation The final location for the writer committer.
   * @param pluginId The ID of the storage plugin.
   * @param icebergTableProps The Iceberg table properties.
   * @param datasetPath The path of the dataset.
   * @param datasetConfig The dataset configuration.
   * @param child The child physical operator.
   * @param isPartialRefresh Whether it is a partial refresh.
   * @param isReadSignatureEnabled Whether read signature is enabled.
   * @param sourceTablePluginId The ID of the source table storage plugin.
   * @param tableFormatOptions The table format options.
   * @param storagePluginResolver The resolver for storage plugins.
   * @param systemIcebergTablesStoragePluginId The storage plugin ID for system iceberg tables.
   */
  public CopyIntoHistoryWriterCommitterPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("tempLocation") String tempLocation,
      @JsonProperty("finalLocation") String finalLocation,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("icebergTableProps") IcebergTableProps icebergTableProps,
      @JsonProperty("datasetPath") NamespaceKey datasetPath,
      @JsonProperty("datasetConfig") DatasetConfig datasetConfig,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("isPartialRefresh") boolean isPartialRefresh,
      @JsonProperty("isReadSignatureEnabled") boolean isReadSignatureEnabled,
      @JsonProperty("sourceTablePluginId") StoragePluginId sourceTablePluginId,
      @JsonProperty("tableFormatOptions") TableFormatWriterOptions tableFormatOptions,
      @JacksonInject StoragePluginResolver storagePluginResolver,
      @JsonProperty("systemIcebergTablesStoragePluginId")
          StoragePluginId systemIcebergTablesStoragePluginId) {
    super(
        props,
        tempLocation,
        finalLocation,
        pluginId,
        icebergTableProps,
        datasetPath,
        datasetConfig,
        child,
        isPartialRefresh,
        isReadSignatureEnabled,
        sourceTablePluginId,
        tableFormatOptions,
        storagePluginResolver);
    this.systemIcebergTablesStoragePlugin =
        systemIcebergTablesStoragePluginId != null
            ? storagePluginResolver.getSource(systemIcebergTablesStoragePluginId)
            : null;
  }

  public CopyIntoHistoryWriterCommitterPOP(
      OpProps props,
      String tempLocation,
      String finalLocation,
      IcebergTableProps icebergTableProps,
      NamespaceKey datasetPath,
      Optional<DatasetConfig> datasetConfig,
      PhysicalOperator child,
      MutablePlugin plugin,
      StoragePlugin sourceTablePlugin,
      boolean isPartialRefresh,
      boolean isReadSignatureEnabled,
      TableFormatWriterOptions tableFormatOptions,
      StoragePluginId sourceTablePluginId,
      SystemIcebergTablesStoragePlugin errorPlugin) {
    super(
        props,
        tempLocation,
        finalLocation,
        icebergTableProps,
        datasetPath,
        datasetConfig,
        child,
        plugin,
        sourceTablePlugin,
        isPartialRefresh,
        isReadSignatureEnabled,
        tableFormatOptions,
        sourceTablePluginId);
    this.systemIcebergTablesStoragePlugin = errorPlugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new CopyIntoHistoryWriterCommitterPOP(
        getProps(),
        getTempLocation(),
        getFinalLocation(),
        getIcebergTableProps(),
        getDatasetPath(),
        getDatasetConfig(),
        child,
        getPlugin(),
        (StoragePlugin) getSourceTablePlugin(),
        isPartialRefresh(),
        isReadSignatureEnabled(),
        getTableFormatOptions(),
        getSourceTablePluginId(),
        getSystemIcebergTablesStoragePlugin());
  }

  /**
   * Retrieves the ID of the system iceberg tables' storage plugin.
   *
   * @return The ID of the storage plugin.
   */
  public StoragePluginId getSystemIcebergTablesStoragePluginId() {
    return systemIcebergTablesStoragePlugin != null
        ? systemIcebergTablesStoragePlugin.getId()
        : null;
  }

  /**
   * Retrieves the SystemIcebergTablesStoragePlugin instance representing the storage plugin.
   *
   * @return The SystemIcebergTablesStoragePlugin instance representing the storage plugin.
   */
  @JsonIgnore
  public SystemIcebergTablesStoragePlugin getSystemIcebergTablesStoragePlugin() {
    return systemIcebergTablesStoragePlugin;
  }

  @JsonIgnore
  public CopyIntoHistoryEventHandler getHistoryEventHandler(OperatorContext context) {
    return new CopyIntoHistoryEventHandler(context, getSystemIcebergTablesStoragePlugin());
  }
}
