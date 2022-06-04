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
package com.dremio.exec.store.iceberg.hive;

import org.apache.iceberg.TableOperations;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Entry point for Hive catalog based Iceberg tables
 */
public class IcebergHiveModel extends IcebergBaseModel {

  private final SupportsIcebergRootPointer plugin;
  private final String tableName;
  private final String queryUserName;
  public static final String HIVE = "hive";

  public IcebergHiveModel(String namespace,
                          String tableName,
                          FileSystem fs,
                          String queryUserName,
                          OperatorContext context,
                          SupportsIcebergRootPointer plugin) {
    super(namespace, plugin.getFsConfCopy(), fs, context, null, (MutablePlugin) plugin);
    this.queryUserName = queryUserName;
    this.plugin = plugin;
    this.tableName = tableName;
  }

  @Override
  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    TableOperations tableOperations = plugin.createIcebergTableOperations(fs, queryUserName, tableIdentifier);
    return new IcebergHiveCommand(configuration,
      ((IcebergHiveTableIdentifier)tableIdentifier).getTableFolder(), fs, tableOperations);
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergHiveTableIdentifier(namespace, rootFolder, tableName);
  }
}
