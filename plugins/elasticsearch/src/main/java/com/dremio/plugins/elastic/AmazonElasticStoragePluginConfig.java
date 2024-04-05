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
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import org.apache.commons.lang3.function.Suppliers;

/** Configuration for Amazon OpenSearch (formerly Elasticsearch) Service storage plugin. */
// Don't change the value of AMAZONELASTIC for backwards compat reasons.
@SourceType(
    value = "AMAZONELASTIC",
    label = "Amazon OpenSearch Service",
    uiConfig = "amazon-elastic-storage-layout.json")
public class AmazonElasticStoragePluginConfig
    extends BaseElasticStoragePluginConfig<
        AmazonElasticStoragePluginConfig, ElasticsearchStoragePlugin> {

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
  @Min(1)
  @Max(65535)
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
  public SecretRef accessSecret = SecretRef.EMPTY;

  @Tag(12)
  @DisplayMetadata(label = "Overwrite region")
  public boolean overwriteRegion = false;

  @Tag(17)
  @DisplayMetadata(label = "Region")
  public String regionName = "";

  @Tag(18)
  @DisplayMetadata(label = "AWS Profile")
  public String awsProfile;

  public AmazonElasticStoragePluginConfig() {}

  @VisibleForTesting
  AmazonElasticStoragePluginConfig(
      String hostname,
      Integer port,
      String accessKey,
      SecretRef accessSecret,
      boolean overwriteRegion,
      String regionName,
      String awsProfile,
      AWSAuthenticationType authenticationType,
      boolean scriptsEnabled,
      boolean showHiddenIndices,
      boolean showIdColumn,
      int readTimeoutMillis,
      int scrollTimeoutMillis,
      boolean usePainless,
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
    this.hostname = hostname;
    this.port = port;
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.overwriteRegion = overwriteRegion;
    this.regionName = regionName;
    this.awsProfile = awsProfile;
    this.authenticationType = authenticationType;
  }

  @Override
  public ElasticsearchStoragePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new ElasticsearchStoragePlugin(createElasticsearchConf(this), context, name);
  }

  /**
   * Creates Elasticsearch configuration, shared by regular Elasticsearch and Amazon OpenSearch,
   * from Amazon OpenSearch configuration.
   *
   * @param amazonOSStoragePluginConfig Amazon OpenSearch configuration
   * @return Elasticsearch configuration
   */
  public static ElasticsearchConf createElasticsearchConf(
      AmazonElasticStoragePluginConfig amazonOSStoragePluginConfig) {
    List<Host> hostList = new ArrayList<>();
    hostList.add(new Host(amazonOSStoragePluginConfig.hostname, amazonOSStoragePluginConfig.port));
    ElasticsearchConf.AuthenticationType authenticationType;
    switch (amazonOSStoragePluginConfig.authenticationType) {
      case NONE:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
        break;
      case ACCESS_KEY:
        authenticationType = ElasticsearchConf.AuthenticationType.ACCESS_KEY;
        break;
      case EC2_METADATA:
        authenticationType = ElasticsearchConf.AuthenticationType.EC2_METADATA;
        break;
      case AWS_PROFILE:
        authenticationType = ElasticsearchConf.AuthenticationType.AWS_PROFILE;
        break;
      default:
        authenticationType = ElasticsearchConf.AuthenticationType.NONE;
        break;
    }
    return new ElasticsearchConf(
        hostList,
        "",
        "",
        amazonOSStoragePluginConfig.accessKey,
        Suppliers.get(amazonOSStoragePluginConfig.accessSecret),
        amazonOSStoragePluginConfig.overwriteRegion ? amazonOSStoragePluginConfig.regionName : "",
        amazonOSStoragePluginConfig.awsProfile,
        authenticationType,
        amazonOSStoragePluginConfig.scriptsEnabled,
        amazonOSStoragePluginConfig.showHiddenIndices,
        true,
        amazonOSStoragePluginConfig.showIdColumn,
        amazonOSStoragePluginConfig.readTimeoutMillis,
        amazonOSStoragePluginConfig.scrollTimeoutMillis,
        amazonOSStoragePluginConfig.usePainless,
        true,
        amazonOSStoragePluginConfig.scrollSize,
        amazonOSStoragePluginConfig.allowPushdownOnNormalizedOrAnalyzedFields,
        amazonOSStoragePluginConfig.pushdownWithKeyword,
        amazonOSStoragePluginConfig.warnOnRowCountMismatch,
        amazonOSStoragePluginConfig.encryptionValidationMode,
        false);
  }
}
