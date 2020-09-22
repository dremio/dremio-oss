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
package com.dremio.exec.store.hive;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.pf4j.PluginManager;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.serialization.kryo.serializers.SourceConfigAwareConnectionConfDeserializer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.hive.Hive2PluginCreator;
import com.dremio.exec.store.hive.HiveStoragePluginConfig;
import com.dremio.exec.store.hive.pf4j.NativeLibPluginManager;

/**
 * Hive 2.x storage plugin configuration.
 */
@SourceType(value = SourceConfigAwareConnectionConfDeserializer.HIVE2_SOURCE_TYPE, label = "Hive 2.x")
public class Hive2StoragePluginConfig extends HiveStoragePluginConfig {

  @Override
  public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    final PluginManager manager = new NativeLibPluginManager();
    manager.loadPlugins();
    manager.startPlugins();

    // Just statically create the plugin creator for this module.
    return new Hive2PluginCreator().createStoragePlugin(manager, this, context, name, pluginIdProvider);
  }

  @Override
  @Nullable
  public String getPf4jPluginId() {
    // Use null to indicate the absence of a plugin ID
    return null;
  }
}
