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
import org.apache.hadoop.fs.FileSystem;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.service.namespace.NamespaceKey;
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

  void truncateTable(NamespaceKey tableSchemaPath,
                     SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions);

  boolean createOrUpdateView(NamespaceKey tableSchemaPath,
                             SchemaConfig schemaConfig,
                             View view) throws IOException;

  void dropView(NamespaceKey tableSchemaPath,
                SchemaConfig schemaConfig) throws IOException;

  void addColumns(NamespaceKey tableSchemaPath,
                  SchemaConfig schemaConfig,
                  List<Field> columnsToAdd,
                  TableMutationOptions tableMutationOptions);

  void dropColumn(NamespaceKey tableSchemaPath,
                  SchemaConfig schemaConfig,
                  String columnToDrop,
                  TableMutationOptions tableMutationOptions);

  void changeColumn(NamespaceKey tableSchemaPath,
                    SchemaConfig schemaConfig,
                    String columnToChange,
                    Field fieldFromSqlColDeclaration,
                    TableMutationOptions tableMutationOptions);

  StoragePluginId getId();

  Writer getWriter(PhysicalOperator child,
                   String location,
                   WriterOptions options,
                   OpProps props) throws IOException;

  boolean toggleSchemaLearning(NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning);

  /**
   *
   * @param path Path for which hadoop file system is being created
   * @param conf Configuration for creating hadoop file system
   * @return Supplier of hadoopFs
   */
  default Supplier<org.apache.hadoop.fs.FileSystem> getHadoopFsSupplier(String path,  Iterable<Map.Entry<String, String>> conf) {
    return getHadoopFsSupplier(path, conf, SystemUser.SYSTEM_USERNAME);
  }

  /**
   *
   * @param path Path for which hadoop file system is being created
   * @param conf Configuration for creating hadoop file system
   * @param queryUser query user using which file System will be created
   * @return Supplier of hadoopFs
   */
  Supplier<FileSystem> getHadoopFsSupplier(String path, Iterable<Map.Entry<String, String>> conf, String queryUser);
}
