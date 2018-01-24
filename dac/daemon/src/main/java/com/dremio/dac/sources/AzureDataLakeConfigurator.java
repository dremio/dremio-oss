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
package com.dremio.dac.sources;

import static com.dremio.dac.server.NASSourceConfigurator.getDefaultFormats;

import java.util.HashMap;
import java.util.Map;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.ADLConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.source.proto.SourceType;
import com.google.api.client.util.Preconditions;

/**
 * generates a StoragePluginConfig from an Azure Data Lake Source
 *
 */
public class AzureDataLakeConfigurator extends SingleSourceToStoragePluginConfig<ADLConfig> {

  public AzureDataLakeConfigurator() {
    super(SourceType.ADL);
  }

  @Override
  public StoragePluginConfig configureSingle(ADLConfig config) {
    Map<String, String> properties = new HashMap<>();
    Preconditions.checkNotNull(config.getAccountName(), "Account name must be set.");
    Preconditions.checkNotNull(config.getClientId(), "Client ID must be set.");
    Preconditions.checkNotNull(config.getMode(), "Authentication mode must be set.");

    // configure hadoop fs implementation
    properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
    properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
    properties.put("fs.adl.impl.disable.cache", "true");

    // configure azure properties.
    properties.put("dfs.adls.oauth2.client.id", config.getClientId());

    switch(config.getMode()) {
    case CLIENT_KEY:
      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");

      if(config.getClientKeyPassword() != null) {
        properties.put("dfs.adls.oauth2.credential", config.getClientKeyPassword());
      }

      if(config.getClientKeyRefreshUrl() != null) {
        properties.put("dfs.adls.oauth2.refresh.url",  config.getClientKeyRefreshUrl());
      }

      break;
    case REFRESH_TOKEN:
      properties.put("dfs.adls.oauth2.access.token.provider.type", "RefreshToken");
      if(config.getRefreshTokenSecret() != null) {
        properties.put("dfs.adls.oauth2.refresh.token", config.getRefreshTokenSecret());
      }

      break;
    default:
      throw new IllegalStateException("Unknown auth mode: " + config.getMode());

    }

    if(config.getPropertyList() != null) {
      for(Property p : config.getPropertyList()) {
        properties.put(p.getName(), p.getValue());
      }
    }
    return new FileSystemConfig("adl://" + config.getAccountName() + ".azuredatalakestore.net/" , "/", properties, getDefaultFormats(), /*impersonationEnabled=*/false, SchemaMutability.NONE);
  }


}
