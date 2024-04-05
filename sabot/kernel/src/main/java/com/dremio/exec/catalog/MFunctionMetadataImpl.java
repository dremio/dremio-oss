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

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import java.util.Optional;

public abstract class MFunctionMetadataImpl implements MFunctionMetadata {

  final DatasetConfig currentConfig;
  final NamespaceKey canonicalKey;
  final ManagedStoragePlugin plugin;
  final SchemaConfig schemaConfig;
  final TableVersionContext context;

  public MFunctionMetadataImpl(
      NamespaceKey canonicalKey,
      DatasetConfig currentConfig,
      ManagedStoragePlugin plugin,
      SchemaConfig schemaConfig,
      TableVersionContext context) {
    this.currentConfig = currentConfig;
    this.plugin = plugin;
    this.schemaConfig = schemaConfig;
    this.canonicalKey = canonicalKey;
    this.context = context;
  }

  @Override
  public DatasetConfig getCurrentConfig() {
    return currentConfig;
  }

  @Override
  public Optional<DatasetHandle> getHandle() {
    Preconditions.checkNotNull(canonicalKey);
    try {
      return plugin.getDatasetHandle(canonicalKey, currentConfig, getOptions());
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  @Override
  public StoragePlugin getPlugin() {
    return plugin.getPlugin();
  }

  @Override
  public StoragePluginId getPluginId() {
    return plugin.getId();
  }

  @Override
  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }
}
