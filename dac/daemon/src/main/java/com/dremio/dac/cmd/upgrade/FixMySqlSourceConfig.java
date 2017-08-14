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
package com.dremio.dac.cmd.upgrade;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.exception.StoreException;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.StoragePluginRegistryImpl;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;
import com.dremio.exec.store.sys.PersistentStore;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.google.common.base.Throwables;

/**
 * Updates MySql storage plugins to use mariadb driver
 */
public class FixMySqlSourceConfig extends UpgradeTask {

  FixMySqlSourceConfig() {
    super("Fix mysql sources", VERSION_106, VERSION_108);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespace = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    final PersistentStoreProvider persistentStoreProvider = new KVPersistentStoreProvider(context.getKVStoreProvider());

    PersistentStore<StoragePluginConfig> pluginSystemTable = null;
    try {
      pluginSystemTable = persistentStoreProvider.getOrCreateStore(StoragePluginRegistry.PSTORE_NAME,
        StoragePluginRegistryImpl.StoragePluginCreator.class, new JacksonSerializer<>(context.getLpPersistence().getMapper(), StoragePluginConfig.class));
    } catch (StoreException e) {
      Throwables.propagate(e);
    }

    // find all mysql sources
    for (SourceConfig source : namespace.getSources()) {
      if (source.getType() == SourceType.MYSQL) {
        final JdbcStorageConfig config = (JdbcStorageConfig) pluginSystemTable.get(source.getName());
        if (config == null) {
          System.out.printf("  Source %s has no associated plugin, ignoring%n", source.getName());
          continue;
        }

        final JdbcStorageConfig updated = new JdbcStorageConfig(
          CompatCreator.MYSQL_DRIVER,
          config.getUrl().replace("jdbc:mysql", "jdbc:mariadb"),
          config.getUsername(),
          config.getPassword(),
          config.getFetchSize());
        System.out.println("  Updating MySQL source " + source.getName());
        pluginSystemTable.put(source.getName(), updated);
      }
    }
  }
}
