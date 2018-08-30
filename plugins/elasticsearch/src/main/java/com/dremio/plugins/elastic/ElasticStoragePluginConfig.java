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

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.hibernate.validator.constraints.NotEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.protostuff.Schema;
import io.protostuff.Tag;
import io.protostuff.runtime.RuntimeSchema;

@SourceType(value = "ELASTIC", label = "Elasticsearch")
public class ElasticStoragePluginConfig extends ConnectionConf<ElasticStoragePluginConfig, ElasticsearchStoragePlugin> {

  private static final Schema<ElasticStoragePluginConfig> SCHEMA = RuntimeSchema.getSchema(ElasticStoragePluginConfig.class);

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

  @NotEmpty
  @Tag(1)
  public List<Host> hostList; // default port should be 9200

  @Tag(2)
  public String username;

  @Tag(3)
  @Secret
  public String password;

  @Tag(4)
  public AuthenticationType authenticationType = AuthenticationType.ANONYMOUS;

  @Tag(5)
  @DisplayMetadata(label = "Use scripts for query pushdown")
  public boolean scriptsEnabled = true;

  @Tag(6)
  @DisplayMetadata(label = "Show hidden indices that start with a dot (.)")
  public boolean showHiddenIndices = false;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Encrypt connection")
  public boolean sslEnabled = false;

  @Tag(8)
  @DisplayMetadata(label = "Show _id columns")
  public boolean showIdColumn = false;

  @Min(1)
  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Read timeout (milliseconds)")
  public int readTimeoutMillis = 60000;

  @Min(1)
  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Scroll timeout (milliseconds)")
  public int scrollTimeoutMillis = 300000;

  @Tag(11)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Use Painless scripting with Elasticsearch 5.0+")
  public boolean usePainless = true;

  @Tag(12)
  @DisplayMetadata(label = "Managed Elasticsearch service")
  public boolean useWhitelist = false;

  @Tag(13)
  @Min(127)
  @Max(65535)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Scroll size")
  public int scrollSize = 4000;

  @Tag(14)
  @DisplayMetadata(label = "Use index/doc fields when pushing down aggregates and filters on analyzed and normalized fields (may produce unexpected results)")
  public boolean allowPushdownOnNormalizedOrAnalyzedFields = false;

  @Tag(15)
  @NotMetadataImpacting
  @JsonIgnore
  public boolean warnOnRowCountMismatch = false;

  public ElasticStoragePluginConfig() {
  }

  public ElasticStoragePluginConfig(
      List<Host> hostList,
      String username,
      String password,
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
      boolean warnOnRowCountMismatch) {
    this.hostList = hostList;
    this.username = username;
    this.password = password;
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
    this.allowPushdownOnNormalizedOrAnalyzedFields = allowPushdownOnNormalizedOrAnalyzedFields;
    this.warnOnRowCountMismatch = warnOnRowCountMismatch;
  }

  public String getReadTimeoutFormatted() {
    return readTimeoutMillis + "ms";
  }

  public String getScrollTimeoutFormatted() {
    return scrollTimeoutMillis + "ms";
  }

  @Override
  public ElasticsearchStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new ElasticsearchStoragePlugin(this, context, name);
  }

}
