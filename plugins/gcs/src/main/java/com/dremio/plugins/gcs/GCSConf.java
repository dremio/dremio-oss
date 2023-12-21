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
package com.dremio.plugins.gcs;

import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.CacheProperties;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.protostuff.Tag;

/**
 * Connector configuration for Google Cloud Storage (GCS)
 */
@SourceType(value = "GCS", configurable = true, label = "Google Cloud Storage", uiConfig = "gcs-layout.json")
public class GCSConf extends FileSystemConf<GCSConf, GoogleStoragePlugin> {


  @Tag(1)
  public String projectId = "";

  /**
   * Authorization Mode for GCS
   */
  public enum AuthMode {
    @Tag(1) @DisplayMetadata(label = "Service Account Keys") SERVICE_ACCOUNT_KEYS,
    @Tag(2) @DisplayMetadata(label = "Automatic/Service Account") AUTO
  }

  public enum AllowlistedBucketsMode {
    @Tag(1) @DisplayMetadata(label = "List Buckets") LIST,
    @Tag(2) @DisplayMetadata(label = "Regular Expression") REGEX,
  }

  @Tag(2)
  public AuthMode authMode = AuthMode.SERVICE_ACCOUNT_KEYS;

  @Tag(4)
  public List<Property> propertyList;

  @Tag(5)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  @JsonIgnore
  public boolean allowCreateDrop;

  @Tag(7)
  public AllowlistedBucketsMode allowlistedBucketsMode = AllowlistedBucketsMode.LIST;

  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enter the name of each bucket to be included")
  public List<String> bucketWhitelist;

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enter a regular expression pattern to match bucket names. Only matching buckets will be allowed")
  public String bucketWhitelistRegexFilter = ".*";

  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean asyncEnabled = true;

  @Tag(11)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable local caching when possible")
  public boolean cachingEnable = true;

  @Tag(12)
  @NotMetadataImpacting
  @Min(value = 1, message = "Max percent of total available cache space must be between 1 and 100")
  @Max(value = 100, message = "Max percent of total available cache space must be between 1 and 100")
  @DisplayMetadata(label = "Max percent of total available cache space to use when possible")
  public int cachePercent = 70;

  @Tag(13)
  @DisplayMetadata(label = "Private Key ID")
  public String privateKeyId = "";

  @Tag(14)
  @Secret
  @DisplayMetadata(label = "Private Key")
  public String privateKey = "";

  @Tag(15)
  @DisplayMetadata(label = "Client Email")
  public String clientEmail = "";

  @Tag(16)
  @DisplayMetadata(label = "Client ID")
  public String clientId = "";


  @Tag(17)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default CTAS Format")
  public DefaultCtasFormatSelection defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;

  @Tag(18)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable partition column inference")
  public boolean isPartitionInferenceEnabled = false;

  @Override
  public Path getPath() {
    return Path.of(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    return DREMIO_GCS_SCHEME + ":///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return isPartitionInferenceEnabled;
  }

  @Override
  public GoogleStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    return new GoogleStoragePlugin(this, context, name, idProvider);
  }

  @Override
  public boolean isAsyncEnabled() {
    return asyncEnabled;
  }

  @Override
  public String getDefaultCtasFormat() {
    return defaultCtasFormat.getDefaultCtasFormat();
  }

  @Override
  public CacheProperties getCacheProperties() {
    return new CacheProperties() {
      @Override
      public boolean isCachingEnabled(final OptionManager optionManager) {
        return GCSConf.this.cachingEnable;
      }

      @Override
      public int cacheMaxSpaceLimitPct() {
        return GCSConf.this.cachePercent;
      }
    };
  }
}
