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
package com.dremio.plugins.s3.store;

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;

import io.protostuff.Tag;

/**
 * Connection Configuration for S3.
 */
@SourceType(value = "S3", label = "Amazon S3", uiConfig = "s3-layout.json")
public class S3PluginConfig extends FileSystemConf<S3PluginConfig, S3StoragePlugin> {
  @Tag(1)
  @DisplayMetadata(label = "AWS Access Key")
  public String accessKey = "";

  @Tag(2)
  @Secret
  @DisplayMetadata(label = "AWS Access Secret")
  public String accessSecret = "";

  @Tag(3)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Encrypt connection")
  public boolean secure;

  @Tag(4)
  @DisplayMetadata(label = "Buckets")
  public List<String> externalBucketList;

  @Tag(5)
  @DisplayMetadata(label = "Connection Properties")
  public List<Property> propertyList;

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  public boolean allowCreateDrop;

  @Tag(7)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(8)
  public AWSAuthenticationType credentialType = AWSAuthenticationType.ACCESS_KEY;

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean enableAsync = true;

  @Tag(10)
  @DisplayMetadata(label = "Enable compatibility mode (experimental)")
  public boolean compatibilityMode = false;

  @Tag(11)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable local caching when possible")
  public boolean isCachingEnabled = true;

  @Tag(12)
  @NotMetadataImpacting
  @Min(value = 1, message = "Max percent of total available cache space must be between 1 and 100")
  @Max(value = 100, message = "Max percent of total available cache space must be between 1 and 100")
  @DisplayMetadata(label = "Max percent of total available cache space to use when possible")
  public int maxCacheSpacePct = 100;

  @Tag(13)
  @DisplayMetadata(label = "Whitelisted buckets")
  public List<String> whitelistedBuckets;

  @Tag(15)
  @DisplayMetadata(label = "IAM Role to Assume")
  public String assumedRoleARN;

  @Tag(16)
  @DisplayMetadata(label = "Server side encryption key ARN")
  @NotMetadataImpacting
  public String kmsKeyARN;

  @Override
  public S3StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new S3StoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public Path getPath() {
    return Path.of(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    return CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme() + ":///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return allowCreateDrop ? SchemaMutability.USER_TABLE : SchemaMutability.NONE;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }

  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(final OptionManager optionManager) {
        return isCachingEnabled;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        return maxCacheSpacePct;
      }
    };
  }
}
