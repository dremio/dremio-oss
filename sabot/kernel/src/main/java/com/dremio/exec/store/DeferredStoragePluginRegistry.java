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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Provider;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class DeferredStoragePluginRegistry implements StoragePluginRegistry {

  private final Provider<CatalogService> registry;

  public DeferredStoragePluginRegistry(Provider<CatalogService> registry) {
    this.registry = registry;
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return registry.get().getOldRegistry().iterator();
  }

  @Override
  public void deletePlugin(String name) {
    registry.get().getOldRegistry().deletePlugin(name);
  }

  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, boolean persist)
      throws ExecutionSetupException {
    return registry.get().getOldRegistry().createOrUpdate(name, config, persist);
  }

  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, SourceConfig sourceConfig,
      boolean persist) throws ExecutionSetupException {
    return registry.get().getOldRegistry().createOrUpdate(name, config, sourceConfig, persist);
  }

  @Override
  public StoragePlugin getPlugin(String name) throws ExecutionSetupException {
    return registry.get().getOldRegistry().getPlugin(name);
  }

  @Override
  public StoragePlugin getPlugin(StoragePluginConfig config) throws ExecutionSetupException {
    return registry.get().getOldRegistry().getPlugin(config);
  }

  @Override
  public StoragePlugin getPlugin(StoragePluginId pluginId) throws ExecutionSetupException {
    return registry.get().getOldRegistry().getPlugin(pluginId);
  }

  @Override
  public void addPlugin(String name, StoragePlugin plugin) {
    registry.get().getOldRegistry().addPlugin(name, plugin);
  }

  @Override
  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig)
      throws ExecutionSetupException {
    return registry.get().getOldRegistry().getFormatPlugin(storageConfig, formatConfig);
  }

  @Override
  public void updateNamespace(Set<String> pluginNames, MetadataPolicy metadataPolicy) {
    registry.get().getOldRegistry().updateNamespace(pluginNames, metadataPolicy);
  }

}
