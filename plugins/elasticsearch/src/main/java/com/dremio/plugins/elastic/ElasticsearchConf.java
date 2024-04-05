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

import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.Host;
import java.util.List;

/** Elasticsearch configuration for both regular Elasticsearch and Amazon Elasticsearch Service. */
public class ElasticsearchConf {

  /**
   * Authentication type to access elasticsearch. NONE to access as anonymous. ES_ACCOUNT uses
   * elasticsearch account. ACCESS_KEY uses AWS access key and secret as credentials. EC2_METADATA
   * uses EC2 metedata to get credentials. AWS_PROFILE uses AWS ProfileCredentialsProvider to get
   * credentials.
   */
  public enum AuthenticationType {
    NONE,
    ES_ACCOUNT,
    ACCESS_KEY,
    EC2_METADATA,
    AWS_PROFILE,
  }

  private final List<Host> hostList;
  private final String username;
  private final String password;
  private final String awsProfile;
  private final AuthenticationType authenticationType;
  private final boolean scriptsEnabled;
  private final boolean showHiddenIndices;
  private final boolean sslEnabled;
  private final boolean showIdColumn;
  private final int readTimeoutMillis;
  private final int scrollTimeoutMillis;
  private final boolean usePainless;
  private final boolean useWhitelist;
  private final int scrollSize;
  private final boolean allowPushdownOnNormalizedOrAnalyzedFields;
  private final boolean pushdownWithKeyword;
  private final boolean warnOnRowCountMismatch;
  private final EncryptionValidationMode encryptionValidationMode;
  private final boolean forceDoublePrecision;
  private final String accessKey;
  private final String accessSecret;
  private final String regionName;

  public ElasticsearchConf(
      List<Host> hostList,
      String username,
      String password,
      String accessKey,
      String accessSecret,
      String regionName,
      String awsProfile,
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
    this.hostList = hostList;
    this.username = username;
    this.password = password;
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.regionName = regionName;
    this.awsProfile = awsProfile;
    this.authenticationType = authenticationType;
    this.scriptsEnabled = scriptsEnabled;
    this.showHiddenIndices = showHiddenIndices;
    this.sslEnabled = sslEnabled;
    this.showIdColumn = showIdColumn;
    this.readTimeoutMillis = readTimeoutMillis;
    this.scrollTimeoutMillis = scrollTimeoutMillis;
    this.usePainless = usePainless;
    this.useWhitelist = useWhitelist;
    this.scrollSize = scrollSize;
    this.allowPushdownOnNormalizedOrAnalyzedFields =
        allowPushdownOnNormalizedOrAnalyzedFields || pushdownWithKeyword;
    this.pushdownWithKeyword = pushdownWithKeyword;
    this.warnOnRowCountMismatch = warnOnRowCountMismatch;
    this.encryptionValidationMode = encryptionValidationMode;
    this.forceDoublePrecision = forceDoublePrecision;
  }

  public List<Host> getHostList() {
    return hostList;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getAwsProfile() {
    return awsProfile;
  }

  public AuthenticationType getAuthenticationType() {
    return authenticationType;
  }

  public boolean isScriptsEnabled() {
    return scriptsEnabled;
  }

  public boolean isShowHiddenIndices() {
    return showHiddenIndices;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  public boolean isShowIdColumn() {
    return showIdColumn;
  }

  public int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  public boolean isUsePainless() {
    return usePainless;
  }

  public boolean isUseWhitelist() {
    return useWhitelist;
  }

  public int getScrollSize() {
    return scrollSize;
  }

  public boolean isAllowPushdownOnNormalizedOrAnalyzedFields() {
    return allowPushdownOnNormalizedOrAnalyzedFields;
  }

  public boolean isPushdownWithKeyword() {
    return pushdownWithKeyword;
  }

  public boolean isWarnOnRowCountMismatch() {
    return warnOnRowCountMismatch;
  }

  public EncryptionValidationMode getEncryptionValidationMode() {
    return encryptionValidationMode;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getScrollTimeoutFormatted() {
    return scrollTimeoutMillis + "ms";
  }

  public boolean isForceDoublePrecision() {
    return forceDoublePrecision;
  }

  public static ElasticsearchConf createElasticsearchConf(
      BaseElasticStoragePluginConfig elasticStoragePluginConfig) {
    if (elasticStoragePluginConfig instanceof ElasticStoragePluginConfig) {
      return ElasticStoragePluginConfig.createElasticsearchConf(
          (ElasticStoragePluginConfig) elasticStoragePluginConfig);
    } else if (elasticStoragePluginConfig instanceof AmazonElasticStoragePluginConfig) {
      return AmazonElasticStoragePluginConfig.createElasticsearchConf(
          (AmazonElasticStoragePluginConfig) elasticStoragePluginConfig);
    } else {
      throw new IllegalArgumentException("invalid elasticsearch storage plugin config");
    }
  }
}
