/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;

import com.dremio.common.JSONOptions;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.SourceState;

/** Interface for all implementations of the storage plugins. Different implementations of the storage
 * formats will implement methods that indicate if Dremio can write or read its tables from that format,
 * if there are optimizer rules specific for the format, getting a storage config. etc.
 */
public interface StoragePlugin<C extends ConversionContext> extends AutoCloseable {

  /** Indicates if Dremio can read the table from this format.
  */
  public boolean supportsRead();

  /** Indicates if Dremio can write a table to this format (e.g. as JSON, csv, etc.).
   */
  public boolean supportsWrite();

  /** An implementation of this method will return one or more specialized rules that Dremio query
   *  optimizer can leverage in <i>physical</i> space. Otherwise, it should return an empty set.
   * @return an empty set or a set of plugin specific physical optimizer rules.
   */
  @Deprecated
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext);

  /**
   * Get the physical scan operator for the particular GroupScan (read) node.
   *
   * @param userName User whom to impersonate when when reading the contents as part of Scan.
   * @param selection The configured storage engine specific selection.
   * @param tableSchemaPath the key for the NamespaceService
   * @param columns (optional) The list of column names to scan from the data source.
   * @return
   * @throws IOException
  */
  OldAbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns)
      throws IOException;

  /** Method returns a jackson serializable object that extends a StoragePluginConfig
  * @return an extension of StoragePluginConfig
  */
  StoragePluginConfig getConfig();

  /**
   * Initialize the storage plugin. The storage plugin will not be used until this method is called.
   */
  void start() throws IOException;

  SchemaMutability getMutability();

  DatasetConfig getDataset(List<String> tableSchemaPath, TableInstance tableInstance, SchemaConfig schemaConfig);

  RelNode getRel(RelOptCluster cluster, RelOptTable relOptTable, C relContext);

  List<DatasetConfig> listDatasets();

  Collection<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig);

  Iterable<String> getSubPartitions(List<String> table, List<String> partitionColumns, List<String> partitionValues, SchemaConfig schemaConfig) throws PartitionNotFoundException;

  boolean createView(List<String> tableSchemaPath, View view, SchemaConfig schemaConfig) throws IOException;

  ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig);

  void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException;

  boolean folderExists(SchemaConfig schemaConfig, List<String> folderPath) throws IOException;

  boolean supportsContains();

  SourceState getState();

  boolean refreshState();

  StoragePlugin2 getStoragePlugin2();

  Convention getStoragePluginConvention();
}
