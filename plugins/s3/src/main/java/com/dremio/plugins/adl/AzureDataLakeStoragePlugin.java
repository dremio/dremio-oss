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
package com.dremio.plugins.adl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;

/**
 * Storage plugin for Microsoft Azure Data Lake
 */
public class AzureDataLakeStoragePlugin extends FileSystemPlugin<AzureDataLakeConf> {

  // Corresponds to the USE_OFF_HEAP_MEMORY_KEY in the ADLS SDK's StoreOptions class.
  static final String USE_OFF_HEAP_MEMORY_KEY = "com.microsoft.azure.datalake.store.use_off_heap_memory";

  static {
    final String useDirectMemoryKey = System.getProperty(USE_OFF_HEAP_MEMORY_KEY);
    if (useDirectMemoryKey == null) {
      System.setProperty(USE_OFF_HEAP_MEMORY_KEY, "true");
    }
  }

  public AzureDataLakeStoragePlugin(AzureDataLakeConf config, SabotContext context, String name, FileSystemWrapper fs,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, fs, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    final AzureDataLakeConf config = getConfig();
    final List<Property> properties = new ArrayList<>();

    // configure hadoop fs implementation
    properties.add(new Property("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem"));
    properties.add(new Property("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl"));
    properties.add(new Property("fs.adl.impl.disable.cache", "true"));

    // configure azure properties.
    properties.add(new Property("dfs.adls.oauth2.client.id", config.clientId));

    switch(config.mode) {
    case CLIENT_KEY:
      properties.add(new Property("dfs.adls.oauth2.access.token.provider.type", "ClientCredential"));

      if(config.clientKeyPassword != null) {
        properties.add(new Property("dfs.adls.oauth2.credential", config.clientKeyPassword));
      }

      if(config.clientKeyRefreshUrl != null) {
        properties.add(new Property("dfs.adls.oauth2.refresh.url",  config.clientKeyRefreshUrl));
      }

      break;
    case REFRESH_TOKEN:
      properties.add(new Property("dfs.adls.oauth2.access.token.provider.type", "RefreshToken"));
      if(config.refreshTokenSecret != null) {
        properties.add(new Property("dfs.adls.oauth2.refresh.token", config.refreshTokenSecret));
      }

      break;
    default:
      throw new IllegalStateException("Unknown auth mode: " + config.mode);

    }

    // Properties are added in order so make sure that any hand provided properties override settings done via specific config
    List<Property> parentProperties = super.getProperties();
    if(parentProperties != null) {
      properties.addAll(parentProperties);
    }

    return properties;
  }
}
