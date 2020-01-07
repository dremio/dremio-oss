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

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;

import io.protostuff.Tag;

@SourceType("HDFS")
public class HDFSConf extends FileSystemConf<HDFSConf, HDFSStoragePlugin> {
  public enum ShortCircuitFlag {
    @Tag(1) @DisplayMetadata(label = "HDFS Default") SYSTEM,
    @Tag(2) @DisplayMetadata(label = "Enabled") ENABLED,
    @Tag(3) @DisplayMetadata(label = "Disabled") DISABLED;
  }

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "NameNode Host")
  public String hostname;

  @Tag(2)
  @Min(1)
  @Max(65535)
  @DisplayMetadata(label = "Port")
  public int port = 8020;

  @Tag(3)
  @DisplayMetadata(label = "Enable impersonation")
  public boolean enableImpersonation;

  @Tag(4)
  public List<Property> propertyList;

  @Tag(5)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Short-Circuit Local Reads")
  public ShortCircuitFlag shortCircuitFlag = ShortCircuitFlag.SYSTEM;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Socket Path")
  public String shortCircuitSocketPath;

  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  public boolean allowCreateDrop;

  @Tag(12)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean enableAsync = true;

  @Tag(13)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable local caching when possible")
  public boolean isCachingEnabled = false;

  @Tag(14)
  @NotMetadataImpacting
  @Min(value = 1, message = "Max percent of total available cache space must be between 1 and 100")
  @Max(value = 100, message = "Max percent of total available cache space must be between 1 and 100")
  @DisplayMetadata(label = "Max percent of total available cache space to use when possible")
  public int maxCacheSpacePct = 100;

  @Override
  public Path getPath() {
    return Path.of(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return enableImpersonation;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    return "hdfs://" + hostname + ":" + port + "/";
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

  @Override
  public SchemaMutability getSchemaMutability() {
    return allowCreateDrop ? SchemaMutability.USER_TABLE : SchemaMutability.NONE;
  }

  public ShortCircuitFlag getShortCircuitFlag() {
    return shortCircuitFlag;
  }

  public String getShortCircuitSocketPath() {
    return shortCircuitSocketPath;
  }

  @Override
  public HDFSStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    return new HDFSStoragePlugin(this, context, name, idProvider);
  }

}
