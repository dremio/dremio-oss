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
package com.dremio.plugins.adl.store;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.DirectorySupportLackingFileSystemPlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;

/** Storage plugin for Microsoft Azure Data Lake */
public class AzureDataLakeStoragePlugin
    extends DirectorySupportLackingFileSystemPlugin<AzureDataLakeConf> {

  static {
    final String useDirectMemoryKey = System.getProperty(ADLStoreOptions.USE_OFF_HEAP_MEMORY_KEY);
    if (useDirectMemoryKey == null) {
      System.setProperty(ADLStoreOptions.USE_OFF_HEAP_MEMORY_KEY, "true");
    }
  }

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AzureDataLakeStoragePlugin.class);

  public AzureDataLakeStoragePlugin(
      AzureDataLakeConf config,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    final AzureDataLakeConf config = getConfig();
    final List<Property> properties = new ArrayList<>();

    // configure hadoop fs implementation
    properties.add(new Property("fs.dremioAdl.impl", DremioAdlFileSystem.class.getName()));
    properties.add(new Property("fs.dremioAdl.impl.disable.cache", "true"));
    properties.add(new Property("fs.AbstractFileSystem.dremioAdl.impl", DremioAdl.class.getName()));

    // configure azure properties.
    properties.add(new Property("dfs.adls.oauth2.client.id", config.clientId));

    switch (config.mode) {
      case CLIENT_KEY:
        properties.add(
            new Property("dfs.adls.oauth2.access.token.provider.type", "ClientCredential"));

        if (config.clientKeyPassword != null) {
          properties.add(
              new Property("dfs.adls.oauth2.credential", config.clientKeyPassword.get()));
        }

        if (config.clientKeyRefreshUrl != null) {
          properties.add(new Property("dfs.adls.oauth2.refresh.url", config.clientKeyRefreshUrl));
        }

        break;
      case REFRESH_TOKEN:
        properties.add(new Property("dfs.adls.oauth2.access.token.provider.type", "RefreshToken"));
        if (config.refreshTokenSecret != null) {
          properties.add(
              new Property("dfs.adls.oauth2.refresh.token", config.refreshTokenSecret.get()));
        }

        break;
      default:
        throw new IllegalStateException("Unknown auth mode: " + config.mode);
    }

    // Properties are added in order so make sure that any hand provided properties override
    // settings done via specific config
    List<Property> parentProperties = super.getProperties();
    if (parentProperties != null) {
      properties.addAll(parentProperties);
    }

    return properties;
  }

  @Override
  public boolean supportsColocatedReads() {
    return false;
  }

  @Override
  protected boolean isAsyncEnabledForQuery(OperatorContext context) {
    return context != null && context.getOptions().getOption(AzureDataLakeOptions.ASYNC_READS);
  }

  @Override
  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    return false;
  }
}
