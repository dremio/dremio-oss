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
package com.dremio.service.reflection.materialization;

import static com.dremio.service.reflection.ReflectionOptions.CLOUD_CACHING_ENABLED;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.net.URI;
import java.util.List;

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.CacheProperties;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Configuration for the MaterializationStoragePluginConfigOTAS plugin
 */
@SourceType( value = "ACCELERATION", configurable = false)
public class AccelerationStoragePluginConfig extends FileSystemConf<AccelerationStoragePluginConfig, AccelerationStoragePlugin> {

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

  public AccelerationStoragePluginConfig() {
  }

  public AccelerationStoragePluginConfig(URI path, boolean enableAsync, boolean enableCaching, int maxCacheSpacePercent,
                                         boolean enableS3FileStatusCheck, String accessKey, String secretKey) {
    if(path.getAuthority() != null) {
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
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  @Override
  public AccelerationStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new AccelerationStoragePlugin(this, context, name, pluginIdProvider);
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
    return ImmutableList.of();
  }

  @Override
  public String getConnection() {
    return connection;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.SYSTEM_TABLE;
  }

  public static SourceConfig create(URI path, boolean enableAsync, boolean enableCaching, int maxCacheSpacePercent,
                                    boolean enableS3FileStatusCheck, String accessKey, String secretKey) {
    SourceConfig conf = new SourceConfig();
    AccelerationStoragePluginConfig connection = new AccelerationStoragePluginConfig(path, enableAsync,
      enableCaching, maxCacheSpacePercent, enableS3FileStatusCheck, accessKey, secretKey);
    conf.setConnectionConf(connection);
    conf.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf.setName(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);
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

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(final OptionManager optionManager) {
        if (FileSystemConf.isCloudFileSystemScheme(connection)) {
          return optionManager.getOption(CLOUD_CACHING_ENABLED) ;
        }
        return enableCaching;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        maxCacheSpacePercent = (maxCacheSpacePercent > MAX_CACHE_SPACE_PERCENT) ?
          MAX_CACHE_SPACE_PERCENT : maxCacheSpacePercent;
        return maxCacheSpacePercent;
      }
    };
  }
}
