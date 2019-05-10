/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;

public interface MutablePlugin extends StoragePlugin {
  CreateTableEntry createNewTable(
      final SchemaConfig schemaConfig,
      final NamespaceKey key,
      final WriterOptions writerOptions,
      final Map<String, Object> storageOptions);

  StoragePluginId getId();

  Writer getWriter(PhysicalOperator child, String location, WriterOptions options, OpProps props) throws IOException;

  void dropTable(List<String> tableSchemaPath, SchemaConfig schemaConfig);

  boolean createOrUpdateView(NamespaceKey key, View view, SchemaConfig schemaConfig) throws IOException;

  void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException;
}
