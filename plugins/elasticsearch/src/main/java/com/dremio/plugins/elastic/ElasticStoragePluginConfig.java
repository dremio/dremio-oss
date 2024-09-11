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
package com.dremio.plugins.elastic;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;
import java.util.List;
import javax.inject.Provider;
import javax.validation.constraints.NotEmpty;

/** Configuration for regular Elasticsearch storage plugin. */
@SourceType(value = "ELASTIC", label = "Elasticsearch", uiConfig = "elastic-storage-layout.json")
public class ElasticStoragePluginConfig
    extends BaseElasticStoragePluginConfig<ElasticStoragePluginConfig, ElasticsearchStoragePlugin> {

  //  repeated Host host = 1; // default port should be 9200
  //  optional string username = 2;
  //  optional string password = 3;
  //  optional AuthenticationType authenticationType = 4;
  //  optional bool scriptsEnabled = 5 [default = true];
  //  optional bool showHiddenIndices = 6 [default = false];
  //  optional bool sslEnabled = 7 [default = false];
  //  optional bool showIdColumn = 8 [default = false];
  //  optional int32 readTimeoutMillis = 9;
  //  optional int32 scrollTimeoutMillis = 10;
  //  optional bool usePainless = 11 [default = true];
  //  optional bool useWhitelist = 12 [default = false];
  //  optional int32 scrollSize = 13 [default = 4000];
  //  optional bool allowPushdownOnNormalizedOrAnalyzedFields = 14 [default = false];
  //  optional int32 warnOnRowCountMismatch = 15 [ default = false];
  //  optional EncryptionVerificationMode sslMode = 16;

  @NotEmpty
  @Tag(1)
  public List<Host> hostList;

  @Tag(2)
  public String username;

  @Tag(3)
  @Secret
  public SecretRef password;

  @Tag(4)
  public AuthenticationType authenticationType = AuthenticationType.ANONYMOUS;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Encrypt connection")
  public boolean sslEnabled = false;

  @Tag(12)
  @DisplayMetadata(label = "Managed Elasticsearch service")
  public boolean useWhitelist = false;

  public ElasticStoragePluginConfig() {
    super();
  }

  public static ElasticStoragePluginConfig newMessage() {
    final ElasticStoragePluginConfig result = new ElasticStoragePluginConfig();
    // Reset fields
    result.encryptionValidationMode = null;
    return result;
  }

  @VisibleForTesting
  ElasticStoragePluginConfig(
      List<Host> hostList,
      String username,
      SecretRef password,
      AuthenticationType authenticationType,
      boolean scriptsEnabled,
      boolean showHiddenIndices,
      boolean sslEnabled,
      boolean showIdColumn,
      int readTimeoutMillis,
      int scrollTimeoutMillis,
      boolean usePainless,
      boolean useWhitelist,
      int scrollSize,
      boolean allowPushdownOnNormalizedOrAnalyzedFields,
      boolean pushdownWithKeyword,
      boolean warnOnRowCountMismatch,
      EncryptionValidationMode encryptionValidationMode,
      boolean forceDoublePrecision) {
    super(
        scriptsEnabled,
        showHiddenIndices,
        showIdColumn,
        readTimeoutMillis,
        scrollTimeoutMillis,
        usePainless,
        scrollSize,
        allowPushdownOnNormalizedOrAnalyzedFields,
        pushdownWithKeyword,
        warnOnRowCountMismatch,
        encryptionValidationMode,
        forceDoublePrecision);
    this.hostList = hostList;
    this.username = username;
    this.password = password;
    this.authenticationType = authenticationType;
    this.sslEnabled = sslEnabled;
    this.useWhitelist = useWhitelist;
  }

  @Override
  public ElasticsearchStoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new ElasticsearchStoragePlugin(createElasticsearchConf(this), context, name);
  }

  public static ElasticsearchConf createElasticsearchConf(
      ElasticStoragePluginConfig elasticStoragePluginConfig) {
    ElasticsearchConf.AuthenticationType authenticationType;
    switch (elasticStoragePluginConfig.authenticationType) {
      case ANONYMOUS:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
        break;
      case MASTER:
        authenticationType = ElasticsearchConf.AuthenticationType.ES_ACCOUNT;
        break;
      default:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
    }
    ElasticsearchConf elasticsearchConf =
        new ElasticsearchConf(
            elasticStoragePluginConfig.hostList,
            elasticStoragePluginConfig.username,
            elasticStoragePluginConfig.password,
            "",
            null,
            "",
            "",
            authenticationType,
            elasticStoragePluginConfig.scriptsEnabled,
            elasticStoragePluginConfig.showHiddenIndices,
            elasticStoragePluginConfig.sslEnabled,
            elasticStoragePluginConfig.showIdColumn,
            elasticStoragePluginConfig.readTimeoutMillis,
            elasticStoragePluginConfig.scrollTimeoutMillis,
            elasticStoragePluginConfig.usePainless,
            elasticStoragePluginConfig.useWhitelist,
            elasticStoragePluginConfig.scrollSize,
            elasticStoragePluginConfig.allowPushdownOnNormalizedOrAnalyzedFields,
            elasticStoragePluginConfig.pushdownWithKeyword,
            elasticStoragePluginConfig.warnOnRowCountMismatch,
            elasticStoragePluginConfig.encryptionValidationMode,
            elasticStoragePluginConfig.forceDoublePrecision);
    return elasticsearchConf;
  }
}
