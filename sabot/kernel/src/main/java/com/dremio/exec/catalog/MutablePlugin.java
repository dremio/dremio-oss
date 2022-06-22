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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;

public interface MutablePlugin extends StoragePlugin {

  void createEmptyTable(NamespaceKey tableSchemaPath,
                        final SchemaConfig schemaConfig,
                        BatchSchema batchSchema,
                        final WriterOptions writerOptions);

  CreateTableEntry createNewTable(final NamespaceKey tableSchemaPath,
                                  final SchemaConfig schemaConfig,
                                  final IcebergTableProps icebergTableProps,
                                  final WriterOptions writerOptions,
                                  final Map<String, Object> storageOptions,
                                  final boolean isResultsTable);

  void dropTable(NamespaceKey tableSchemaPath,
                 SchemaConfig schemaConfig,
                 TableMutationOptions tableMutationOptions);

  void alterTable(NamespaceKey tableSchemaPath, DatasetConfig datasetConfig, AlterTableOption alterTableOption, SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions);

  void truncateTable(NamespaceKey tableSchemaPath,
                     SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions);

  boolean createOrUpdateView(NamespaceKey tableSchemaPath,
                             SchemaConfig schemaConfig,
                             View view, ViewOptions viewOptions) throws IOException;

  void dropView(NamespaceKey tableSchemaPath, ViewOptions viewOptions, SchemaConfig schemaConfig) throws IOException;

  void addColumns(NamespaceKey tableSchemaPath,
                  DatasetConfig datasetConfig,
                  SchemaConfig schemaConfig,
                  List<Field> columnsToAdd,
                  TableMutationOptions tableMutationOptions);

  void dropColumn(NamespaceKey tableSchemaPath,
                  DatasetConfig datasetConfig,
                  SchemaConfig schemaConfig,
                  String columnToDrop,
                  TableMutationOptions tableMutationOptions);

  void changeColumn(NamespaceKey tableSchemaPath,
                    DatasetConfig datasetConfig,
                    SchemaConfig schemaConfig,
                    String columnToChange,
                    Field fieldFromSqlColDeclaration,
                    TableMutationOptions tableMutationOptions);

  void addPrimaryKey(NamespaceKey table,
                     DatasetConfig datasetConfig,
                     SchemaConfig schemaConfig,
                     List<Field> columns);

  void dropPrimaryKey(NamespaceKey table,
                      DatasetConfig datasetConfig,
                      SchemaConfig schemaConfig);

  StoragePluginId getId();

  Writer getWriter(PhysicalOperator child,
                   String location,
                   WriterOptions options,
                   OpProps props) throws IOException;

  boolean toggleSchemaLearning(NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning);

  default boolean isSupportUserDefinedSchema(DatasetConfig dataset) {
    return false;
  }

  /**
   * @return The default ctas format to use for the plugin.
   */
  default String getDefaultCtasFormat() {
    throw new UnsupportedOperationException("getDefaultCtasFormat is not implemented");
  }

  /**
   * @param location for the file system.
   * @param userName The userName for the file system.
   * @param operatorContext The operatorContext for creating the file system.
   * @return The filesystem .
   * @throws IOException
   */
  FileSystem createFS( String location, String userName, OperatorContext operatorContext) throws IOException;

  /**
   * @return The File system for System user.
   * @throws IOException
   */
  default FileSystem getSystemUserFS() {
    throw new UnsupportedOperationException("getSystemUserFS is not Implemented");
  }

  /**
   * @return A copy of the configuration to use for the plugin.
   */
  default Configuration getFsConfCopy() {
    throw new UnsupportedOperationException("getFsConfCopy is not Implemented");
  }

  /**
   * This provides the supplier of fs which is created in dremio class loader
   * @param path Path for which hadoop file system is being created
   * @param conf Configuration for creating hadoop file system
   * @return Supplier of hadoopFs
   */
  default Supplier<org.apache.hadoop.fs.FileSystem> getHadoopFsSupplier(String path,  Iterable<Map.Entry<String, String>> conf) {
    return getHadoopFsSupplier(path, conf, SystemUser.SYSTEM_USERNAME);
  }

  /**
   * This provides the supplier of fs which is created in dremio class loader
   * @param path Path for which hadoop file system is being created
   * @param conf Configuration for creating hadoop file system
   * @param queryUser query user using which file System will be created
   * @return Supplier of hadoopFs
   */
   Supplier<org.apache.hadoop.fs.FileSystem> getHadoopFsSupplier(String path, Iterable<Map.Entry<String, String>> conf, String queryUser);
}
