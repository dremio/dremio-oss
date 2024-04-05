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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.ExecConstants.METADATA_CLOUD_CACHING_ENABLED;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.proto.DataCredentials;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;
import io.protostuff.Tag;
import java.net.URI;
import java.util.List;
import javax.inject.Provider;

@SourceType(value = "METADATA", configurable = false)
public class MetadataStoragePluginConfig
    extends MayBeDistFileSystemConf<MetadataStoragePluginConfig, MetadataStoragePlugin> {

  private static final int MAX_CACHE_SPACE_PERCENT = 100;

  @Tag(1)
  public String connection;

  @Tag(2)
  public String path = "/";

  @Tag(3)
  public boolean enableAsync = true;

  @Tag(4)
  public boolean enableCaching = false;

  @Tag(5)
  public int maxCacheSpacePercent = MAX_CACHE_SPACE_PERCENT;

  @Tag(6)
  public boolean enableS3FileStatusCheck = true;

  @Tag(7)
  public String accessKey = null;

  @Tag(8)
  public String secretKey = null;

  @Tag(9)
  public String iamRole = null;

  @Tag(10)
  public String externalId = null;

  @Tag(11)
  public List<Property> propertyList;

  @Tag(12)
  public String tokenEndpoint = null;

  @Tag(13)
  public String clientId = null;

  @Tag(14)
  public String clientSecret = null;

  @Tag(15)
  public String accountName = null;

  @Tag(16)
  public String accountKind = null;

  @Tag(17)
  public String sharedAccessKey = null;

  // Tag has been deprecated please do not use.

  public MetadataStoragePluginConfig() {}

  public MetadataStoragePluginConfig(
      URI path,
      boolean enableAsync,
      boolean enableCaching,
      int maxCacheSpacePercent,
      boolean enableS3FileStatusCheck,
      DataCredentials dataCredentials) {
    if (path.getAuthority() != null) {
      connection = path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      connection = path.getScheme() + ":///";
    }
    String storagePath = path.getPath();
    if (!isNullOrEmpty(storagePath)) {
      this.path = storagePath;
    }
    this.enableAsync = enableAsync;
    this.enableCaching = enableCaching;
    this.maxCacheSpacePercent = maxCacheSpacePercent;
    this.enableS3FileStatusCheck = enableS3FileStatusCheck;
    if (dataCredentials != null) {
      if (dataCredentials.hasKeys()) {
        this.accessKey = dataCredentials.getKeys().getAccessKey();
        this.secretKey = dataCredentials.getKeys().getSecretKey();
      } else if (dataCredentials.hasDataRole()) {
        this.iamRole = dataCredentials.getDataRole().getIamRole();
        this.externalId = dataCredentials.getDataRole().getExternalId();
      } else if (dataCredentials.hasClientAccess()) {
        this.tokenEndpoint = dataCredentials.getClientAccess().getTokenEndpoint();
        this.clientId = dataCredentials.getClientAccess().getClientId();
        this.clientSecret = dataCredentials.getClientAccess().getClientSecret();
        this.accountName = dataCredentials.getClientAccess().getAccountName();
        this.accountKind = dataCredentials.getClientAccess().getAccountKind();
      }
    }
  }

  @Override
  public MetadataStoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new MetadataStoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public boolean isInternal() {
    return true;
  }

  @Override
  public Path getPath() {
    return Path.of(path);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList == null ? ImmutableList.of() : propertyList;
  }

  @Override
  public String getConnection() {
    return connection;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.SYSTEM_TABLE;
  }

  public static SourceConfig create(
      URI path,
      boolean enableAsync,
      boolean enableCaching,
      int maxCacheSpacePercent,
      boolean enableS3FileStatusCheck,
      DataCredentials dataCredentials) {
    SourceConfig conf = new SourceConfig();
    MetadataStoragePluginConfig connection =
        new MetadataStoragePluginConfig(
            path,
            enableAsync,
            enableCaching,
            maxCacheSpacePercent,
            enableS3FileStatusCheck,
            dataCredentials);
    conf.setConnectionConf(connection);
    conf.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf.setName(METADATA_STORAGE_PLUGIN_NAME);
    return conf;
  }

  @Override
  public boolean createIfMissing() {
    return true;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }

  public boolean isS3FileStatusCheckEnabled() {
    return enableS3FileStatusCheck;
  }

  @Override
  public String getAccessKey() {
    return accessKey;
  }

  @Override
  public String getSecretKey() {
    return secretKey;
  }

  @Override
  public String getIamRole() {
    return iamRole;
  }

  @Override
  public String getExternalId() {
    return externalId;
  }

  @Override
  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public String getClientSecret() {
    return clientSecret;
  }

  @Override
  public String getAccountName() {
    return accountName;
  }

  @Override
  public String getAccountKind() {
    return accountKind;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(final OptionManager optionManager) {
        if (FileSystemConf.isCloudFileSystemScheme(connection)) {
          return optionManager.getOption(METADATA_CLOUD_CACHING_ENABLED);
        }
        return enableCaching;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        maxCacheSpacePercent =
            (maxCacheSpacePercent > MAX_CACHE_SPACE_PERCENT)
                ? MAX_CACHE_SPACE_PERCENT
                : maxCacheSpacePercent;
        return maxCacheSpacePercent;
      }
    };
  }
}
