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
package com.dremio.exec.store.hive;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin2;

public class HiveStoragePlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private final HiveStoragePluginConfig config;
  private final SabotContext context;
  private final HiveStoragePlugin2 plugin2;

  public HiveStoragePlugin(HiveStoragePluginConfig config, SabotContext context, String name) throws ExecutionSetupException {
    this.plugin2 = new HiveStoragePlugin2(config, name, createHiveConf(config.config), context.getConfig(), context.isCoordinator());
    this.config = config;
    this.context = context;
  }

  private static HiveConf createHiveConf(final Map<String, String> hiveConfigOverride) {
    final HiveConf hiveConf = new HiveConf();
    for(Entry<String, String> config : hiveConfigOverride.entrySet()) {
      final String key = config.getKey();
      final String value = config.getValue();
      hiveConf.set(key, value);
      if(logger.isTraceEnabled()){
        logger.trace("HiveConfig Override {}={}", key, value);
      }
    }
    return hiveConf;
  }

  @Override
  public HiveStoragePluginConfig getConfig() {
    return config;
  }

  public StoragePlugin2 getStoragePlugin2() {
    return plugin2;
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List<String> folderPath) throws IOException {
    // Should never be called...
    return false;
  }

}
