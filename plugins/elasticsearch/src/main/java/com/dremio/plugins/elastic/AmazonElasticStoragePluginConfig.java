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
package com.dremio.plugins.elastic;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import org.hibernate.validator.constraints.NotEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;

import io.protostuff.Tag;

/**
 * Configuration for Amazon Elasticsearch Service storage plugin.
 */
@SourceType(value = "AMAZONELASTIC", label = "Amazon Elasticsearch Service")
public class AmazonElasticStoragePluginConfig extends BaseElasticStoragePluginConfig<AmazonElasticStoragePluginConfig, ElasticsearchStoragePlugin> {

  //  optional string hostname = 1;
  //  optional integer port = 2; // default port should be 443
  //  optional AuthenticationType authenticationType = 3;
  //  optional string accessKey = 4;
  //  optional bool scriptsEnabled = 5 [default = true];
  //  optional bool showHiddenIndices = 6 [default = false];
  //  optional string accessSecret = 7;
  //  optional bool showIdColumn = 8 [default = false];
  //  optional int32 readTimeoutMillis = 9;
  //  optional int32 scrollTimeoutMillis = 10;
  //  optional bool usePainless = 11 [default = true];
  //  optional bool overwriteRegion = 12 [default = false];
  //  optional int32 scrollSize = 13 [default = 4000];
  //  optional bool allowPushdownOnNormalizedOrAnalyzedFields = 14 [default = false];
  //  optional bool warnOnRowCountMismatch = 15 [default = false];
  //  optional EncryptionVerificationMode sslMode = 16;
  //  optional string regionName = 17;

  @NotEmpty
  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String hostname;

  @Tag(2)
  @DisplayMetadata(label = "Port")
  public Integer port = 443;

  @Tag(3)
  public AWSAuthenticationType authenticationType = AWSAuthenticationType.ACCESS_KEY;

  @Tag(4)
  @DisplayMetadata(label = "AWS Access Key")
  public String accessKey = "";

  @Tag(7)
  @Secret
  @DisplayMetadata(label = "AWS Access Secret")
  public String accessSecret = "";

  @Tag(12)
  @DisplayMetadata(label = "Overwrite region")
  public boolean overwriteRegion = false;

  @Tag(17)
  @DisplayMetadata(label = "Region")
  public String regionName = "";

  public AmazonElasticStoragePluginConfig() {
  }

  public AmazonElasticStoragePluginConfig(
      String hostname,
      Integer port,
      String accessKey,
      String accessSecret,
      boolean overwriteRegion,
      String regionName,
      AWSAuthenticationType authenticationType,
      boolean scriptsEnabled,
      boolean showHiddenIndices,
      boolean showIdColumn,
      int readTimeoutMillis,
      int scrollTimeoutMillis,
      boolean usePainless,
      int scrollSize,
      boolean allowPushdownOnNormalizedOrAnalyzedFields,
      boolean warnOnRowCountMismatch,
      EncryptionValidationMode encryptionValidationMode) {
    super(scriptsEnabled, showHiddenIndices, showIdColumn, readTimeoutMillis, scrollTimeoutMillis, usePainless,
      scrollSize, allowPushdownOnNormalizedOrAnalyzedFields, warnOnRowCountMismatch, encryptionValidationMode);
    this.hostname = hostname;
    this.port = port;
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.overwriteRegion = overwriteRegion;
    this.regionName = regionName;
    this.authenticationType = authenticationType;
  }

  @Override
  public ElasticsearchStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new ElasticsearchStoragePlugin(createElasticsearchConf(this), context, name);
  }

  /**
   * Creates Elasticsearch configuration, shared by regular Elasticsearch and Amazon Elasticsearch, from Amazon Elasticsearch configuration.
   * @param amazonElasticStoragePluginConfig Amazon Elasticsearch configuration
   * @return Elasticsearch configuration
   */
  public static ElasticsearchConf createElasticsearchConf(AmazonElasticStoragePluginConfig amazonElasticStoragePluginConfig) {
    List<Host> hostList = new ArrayList<>();
    hostList.add(new Host(amazonElasticStoragePluginConfig.hostname, amazonElasticStoragePluginConfig.port));
    ElasticsearchConf.AuthenticationType authenticationType;
    switch (amazonElasticStoragePluginConfig.authenticationType) {
      case NONE:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
        break;
      case ACCESS_KEY:
        authenticationType = ElasticsearchConf.AuthenticationType.ACCESS_KEY;
        break;
      case EC2_METADATA:
        authenticationType = ElasticsearchConf.AuthenticationType.EC2_METADATA;
        break;
      default:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
        break;
    }
    ElasticsearchConf elasticsearchConf = new ElasticsearchConf(hostList, "",
      "", amazonElasticStoragePluginConfig.accessKey, amazonElasticStoragePluginConfig.accessSecret,
      amazonElasticStoragePluginConfig.overwriteRegion ? amazonElasticStoragePluginConfig.regionName : "",
      authenticationType, amazonElasticStoragePluginConfig.scriptsEnabled,
      amazonElasticStoragePluginConfig.showHiddenIndices, true, amazonElasticStoragePluginConfig.showIdColumn,
      amazonElasticStoragePluginConfig.readTimeoutMillis, amazonElasticStoragePluginConfig.scrollTimeoutMillis,
      amazonElasticStoragePluginConfig.usePainless, true, amazonElasticStoragePluginConfig.scrollSize,
      amazonElasticStoragePluginConfig.allowPushdownOnNormalizedOrAnalyzedFields,
      amazonElasticStoragePluginConfig.warnOnRowCountMismatch,
      amazonElasticStoragePluginConfig.encryptionValidationMode);
    return elasticsearchConf;
  }
}
