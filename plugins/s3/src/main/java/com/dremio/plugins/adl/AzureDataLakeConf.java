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

import org.apache.hadoop.fs.Path;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Azure Data Lake (ADL)
 * https://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html
 */
@SourceType(value = "ADL", label = "Azure Data Lake Store")
public class AzureDataLakeConf extends FileSystemConf<AzureDataLakeConf, FileSystemPlugin> {

  private static final List<String> UNIQUE_CONN_PROPS = ImmutableList.of(
      "dfs.adls.oauth2.client.id",
      "dfs.adls.oauth2.credential",
      "dfs.adls.oauth2.refresh.url",
      "dfs.adls.oauth2.refresh.token"
  );

  /**
   * Type ADL Auth
   */
  public static enum ADLAuth {

    // fs.adl.oauth2.access.token.provider.type = RefreshToken
    @Tag(1)
    REFRESH_TOKEN,

    // fs.adl.oauth2.access.token.provider.type = ClientCredential
    @Tag(2)
    CLIENT_KEY
  }

  @Tag(1)
  @JsonIgnore
  public ADLAuth mode = ADLAuth.CLIENT_KEY;

  @Tag(2)
  @DisplayMetadata(label = "Data Lake Store Resource Name")
  public String accountName;

  // dfs.adls.oauth2.client.id
  @Tag(3)
  @DisplayMetadata(label = "Application ID")
  public String clientId;

  // dfs.adls.oauth2.refresh.token
  @Tag(4)
  @Secret
  @JsonIgnore
  public String refreshTokenSecret;

  // dfs.adl.oauth2.refresh.url
  @Tag(5)
  @DisplayMetadata(label = "OAuth 2.0 Token Endpoint")
  public String clientKeyRefreshUrl;

  // dfs.adl.oauth2.credential
  @Tag(6)
  @Secret
  @DisplayMetadata(label = "Access key value")
  public String clientKeyPassword;

  @Tag(7)
  public List<Property> propertyList;

  @Override
  public FileSystemPlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    Preconditions.checkNotNull(accountName, "Account name must be set.");
    Preconditions.checkNotNull(clientId, "Client ID must be set.");
    Preconditions.checkNotNull(mode, "Authentication mode must be set.");
    return new FileSystemPlugin(this, context, name, null, pluginIdProvider);
  }

  @Override
  public Path getPath() {
    return new Path("/");
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public List<Property> getProperties() {
    List<Property> properties = new ArrayList<>();

    // configure hadoop fs implementation
    properties.add(new Property("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem"));
    properties.add(new Property("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl"));
    properties.add(new Property("fs.adl.impl.disable.cache", "true"));

    // configure azure properties.
    properties.add(new Property("dfs.adls.oauth2.client.id", clientId));

    switch(mode) {
    case CLIENT_KEY:
      properties.add(new Property("dfs.adls.oauth2.access.token.provider.type", "ClientCredential"));

      if(clientKeyPassword != null) {
        properties.add(new Property("dfs.adls.oauth2.credential", clientKeyPassword));
      }

      if(clientKeyRefreshUrl != null) {
        properties.add(new Property("dfs.adls.oauth2.refresh.url",  clientKeyRefreshUrl));
      }

      break;
    case REFRESH_TOKEN:
      properties.add(new Property("dfs.adls.oauth2.access.token.provider.type", "RefreshToken"));
      if(refreshTokenSecret != null) {
        properties.add(new Property("dfs.adls.oauth2.refresh.token", refreshTokenSecret));
      }

      break;
    default:
      throw new IllegalStateException("Unknown auth mode: " + mode);

    }

    // Properties are added in order so make sure that any hand provided properties override settings done via specific config
    if(this.propertyList != null) {
      properties.addAll(this.propertyList);
    }

    return properties;
  }

  @Override
  public String getConnection() {
    return "adl://" + accountName + ".azuredatalakestore.net/";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public List<String> getConnectionUniqueProperties() {
    return UNIQUE_CONN_PROPS;
  }
}
