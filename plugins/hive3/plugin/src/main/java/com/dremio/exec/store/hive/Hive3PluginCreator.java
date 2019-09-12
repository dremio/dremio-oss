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

import javax.inject.Provider;

import org.pf4j.Extension;
import org.pf4j.PluginManager;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;

/**
 * PF4J extension for creating Hive3 StoragePlugin instances.
 */
@Extension
public class Hive3PluginCreator implements StoragePluginCreator {

  public Hive3StoragePlugin createStoragePlugin(PluginManager pf4jManager, HiveStoragePluginConfig config,
                                                SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    final HiveConfFactory confFactory = new HiveConfFactory();
    return new Hive3StoragePlugin(confFactory.createHiveConf(config), pf4jManager, context, name);
  }
}
