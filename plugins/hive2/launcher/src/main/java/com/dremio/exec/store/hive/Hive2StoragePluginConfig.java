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

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.serialization.kryo.serializers.SourceConfigAwareConnectionConfDeserializer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.plugins.pf4j.NativeLibPluginManager;
import javax.inject.Provider;
import org.pf4j.PluginManager;

/**
 * Hive 2.x storage plugin configuration: here Hive2 source type works in compatibility mode,
 * without the use of Hive2 libraries, making use of Hive3 plugin instead
 */
@SourceType(
    value = SourceConfigAwareConnectionConfDeserializer.HIVE2_SOURCE_TYPE,
    label = "Hive 2.x",
    uiConfig = "hive2-layout.json")
public class Hive2StoragePluginConfig extends HiveStoragePluginConfig {

  @Override
  public StoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    hiveMajorVersion = 2;

    final PluginManager manager = new NativeLibPluginManager(context.getOptionManager());

    manager.loadPlugins();
    manager.startPlugin(getPf4jPluginId());
    final StoragePluginCreator pluginCreator =
        manager.getExtensions(StoragePluginCreator.class, getPf4jPluginId()).get(0);

    return pluginCreator.createStoragePlugin(manager, this, context, name, pluginIdProvider);
  }

  @Override
  public String getPf4jPluginId() {
    return "hive3";
  }
}
