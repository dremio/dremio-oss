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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.copyinto.CopyIntoErrorWriterCommitterPOP;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.options.Options;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * A custom implementation of a WriterCommitter Prel used for handling COPY INTO errors.
 */
@Options
public class CopyIntoErrorWriterCommitterPrel extends WriterCommitterPrel {

  private final SystemIcebergTablesStoragePlugin systemIcebergTablesStoragePlugin;

  /**
   * Constructs a new CopyIntoErrorWriterCommitterPrel instance with the given parameters.
   *
   * @param cluster             The cluster for the relational plan.
   * @param traits              The trait set for the relational plan.
   * @param child               The child relational operator.
   * @param plugin              The mutable plugin.
   * @param tempLocation        The temporary location for the writer committer.
   * @param finalLocation       The final location for the writer committer.
   * @param userName            The username associated with the committer.
   * @param createTableEntry    The entry for the table to be created.
   * @param datasetConfig       The dataset configuration.
   * @param partialRefresh      Whether it is a partial refresh.
   * @param readSignatureEnabled  Whether read signature is enabled.
   * @param sourceTablePluginId  The ID of the source table storage plugin.
   * @param systemIcebergTablesStoragePlugin The {@link SystemIcebergTablesStoragePlugin} instance representing the storage plugin for system iceberg tables.
   */
  public CopyIntoErrorWriterCommitterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                                          MutablePlugin plugin, String tempLocation, String finalLocation,
                                          String userName, CreateTableEntry createTableEntry,
                                          Optional<DatasetConfig> datasetConfig, boolean partialRefresh,
                                          boolean readSignatureEnabled, StoragePluginId sourceTablePluginId,
                                          SystemIcebergTablesStoragePlugin systemIcebergTablesStoragePlugin) {
    super(cluster, traits, child, plugin, tempLocation, finalLocation, userName, createTableEntry, datasetConfig,
      partialRefresh, readSignatureEnabled, sourceTablePluginId);
    this.systemIcebergTablesStoragePlugin = systemIcebergTablesStoragePlugin;
  }

  /**
   * Constructs a new CopyIntoErrorWriterCommitterPrel instance with the given parameters (without sourceTablePluginId).
   *
   * @param cluster             The cluster for the relational plan.
   * @param traits              The trait set for the relational plan.
   * @param child               The child relational operator.
   * @param plugin              The mutable plugin.
   * @param tempLocation        The temporary location for the writer committer.
   * @param finalLocation       The final location for the writer committer.
   * @param userName            The username associated with the committer.
   * @param createTableEntry    The entry for the table to be created.
   * @param datasetConfig       The dataset configuration.
   * @param partialRefresh      Whether it is a partial refresh.
   * @param readSignatureEnabled  Whether read signature is enabled.
   * @param systemIcebergTablesStoragePlugin The {@link SystemIcebergTablesStoragePlugin} instance representing the storage plugin for system iceberg tables.
   */
  public CopyIntoErrorWriterCommitterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                                          MutablePlugin plugin, String tempLocation, String finalLocation,
                                          String userName, CreateTableEntry createTableEntry,
                                          Optional<DatasetConfig> datasetConfig, boolean partialRefresh,
                                          boolean readSignatureEnabled, SystemIcebergTablesStoragePlugin systemIcebergTablesStoragePlugin) {
    this(cluster, traits, child, plugin, tempLocation, finalLocation, userName, createTableEntry, datasetConfig,
      partialRefresh, readSignatureEnabled, null, systemIcebergTablesStoragePlugin);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new CopyIntoErrorWriterCommitterPOP(
      creator.props(this, getUserName(), RecordWriter.SCHEMA, RESERVE, LIMIT),
      getTempLocation(),
      getFinalLocation(),
      getCreateTableEntry().getIcebergTableProps(),
      getCreateTableEntry().getDatasetPath(),
      getDatasetConfig(),
      getChildPhysicalOperator(creator),
      getPlugin(),
      null,
      isPartialRefresh(),
      isReadSignatureEnabled(),
      getCreateTableEntry().getOptions().getTableFormatOptions(),
      getSourceTablePluginId(),
      getSystemIcebergTablesStoragePlugin()
    );
  }

  @Override
  public WriterCommitterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CopyIntoErrorWriterCommitterPrel(getCluster(), traitSet, sole(inputs), getPlugin(), getTempLocation(),
      getFinalLocation(), getUserName(), getCreateTableEntry(), getDatasetConfig(), isPartialRefresh(),
      isReadSignatureEnabled(), getSourceTablePluginId(), getSystemIcebergTablesStoragePlugin());
  }

  /**
   * Retrieves the SystemIcebergTablesStoragePlugin instance representing the storage plugin.
   *
   * @return The SystemIcebergTablesStoragePlugin instance representing the storage plugin.
   */
  public SystemIcebergTablesStoragePlugin getSystemIcebergTablesStoragePlugin() {
    return systemIcebergTablesStoragePlugin;
  }
}
