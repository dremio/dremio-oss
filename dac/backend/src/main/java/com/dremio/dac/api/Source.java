/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.api;

import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.proto.model.source.HBaseConfig;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.proto.model.source.HiveConfig;
import com.dremio.dac.proto.model.source.MSSQLConfig;
import com.dremio.dac.proto.model.source.MapRFSConfig;
import com.dremio.dac.proto.model.source.MongoConfig;
import com.dremio.dac.proto.model.source.MySQLConfig;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.proto.model.source.OracleConfig;
import com.dremio.dac.proto.model.source.PostgresConfig;
import com.dremio.dac.proto.model.source.RedshiftConfig;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.proto.model.source.UnknownConfig;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Source model for the public REST API.
 */
@JsonIgnoreProperties(value={ "_type" }, allowGetters=true)
public class Source {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = NASConfig.class, name = "NAS"),
    @JsonSubTypes.Type(value = HdfsConfig.class, name = "HDFS"),
    @JsonSubTypes.Type(value = MapRFSConfig.class, name = "MAPRFS"),
    @JsonSubTypes.Type(value = S3Config.class, name = "S3"),
    @JsonSubTypes.Type(value = MongoConfig.class, name = "MONGO"),
    @JsonSubTypes.Type(value = ElasticConfig.class, name = "ELASTIC"),
    @JsonSubTypes.Type(value = OracleConfig.class, name = "ORACLE"),
    @JsonSubTypes.Type(value = MySQLConfig.class, name = "MYSQL"),
    @JsonSubTypes.Type(value = MSSQLConfig.class, name = "MSSQL"),
    @JsonSubTypes.Type(value = PostgresConfig.class, name = "POSTGRES"),
    @JsonSubTypes.Type(value = RedshiftConfig.class, name = "REDSHIFT"),
    @JsonSubTypes.Type(value = HBaseConfig.class, name = "HBASE"),
    @JsonSubTypes.Type(value = HiveConfig.class, name = "HIVE"),
    @JsonSubTypes.Type(value = DB2Config.class, name = "DB2"),
    @JsonSubTypes.Type(value = UnknownConfig.class, name = "UNKNOWN")
  })
  private com.dremio.dac.model.sources.Source config;
  private SourceState state;
  private String id;
  private String tag;
  private String type;
  private String name;
  private String description;
  @JsonISODateTime
  private long createdAt;
  private MetadataPolicy metadataPolicy;
  private long accelerationGracePeriodMs;
  private long accelerationRefreshPeriodMs;

  public Source() {
  }

  public Source(SourceConfig config) {
    this.id = config.getId().getId();
    this.tag = String.valueOf(config.getVersion());
    this.type = config.getType().name();
    this.name = config.getName();
    this.description = config.getDescription();

    if (config.getCtime() != null) {
      this.createdAt = config.getCtime();
    }

    com.dremio.service.namespace.source.proto.MetadataPolicy configMetadataPolicy = config.getMetadataPolicy();
    if (configMetadataPolicy == null) {
      this.metadataPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    } else {
      this.metadataPolicy = new MetadataPolicy(configMetadataPolicy);
    }

    this.accelerationGracePeriodMs = config.getAccelerationGracePeriod();
    this.accelerationRefreshPeriodMs = config.getAccelerationRefreshPeriod();

    // TODO: use our own config classes
    this.config = com.dremio.dac.model.sources.Source.fromByteString(config.getType(), config.getConfig());
  }

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTag() {
    return this.tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getType() { return this.type; }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() { return this.name; }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() { return this.description; }

  public void setDescription(String description) {
    this.description = description;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public com.dremio.dac.model.sources.Source getConfig() {
    return this.config;
  }

  public void setConfig(com.dremio.dac.model.sources.Source config) {
    this.config = config;
  }

  public MetadataPolicy getMetadataPolicy() {
    if (this.metadataPolicy == null) {
      return new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    }

    return this.metadataPolicy;
  }

  public void setMetadataPolicy(MetadataPolicy metadataPolicy) {
    this.metadataPolicy = metadataPolicy;
  }

  public Long getAccelerationGracePeriodMs() {
    return this.accelerationGracePeriodMs;
  }

  public void setAccelerationGracePeriodMs(long accelerationGracePeriodMs) {
    this.accelerationGracePeriodMs = accelerationGracePeriodMs;
  }

  public Long getAccelerationRefreshPeriodMs() {
    return this.accelerationRefreshPeriodMs;
  }

  public void setAccelerationRefreshPeriodMs(long accelerationRefreshPeriodMs) {
    this.accelerationRefreshPeriodMs = accelerationRefreshPeriodMs;
  }

  public SourceState getState() {
    // TODO: use our own SourceState
    return this.state;
  }

  public void setState(SourceState state) {
    this.state = state;
  }

  public SourceConfig toSourceConfig() {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setId(new EntityId(getId()));

    String tag = getTag();
    if (tag != null) {
      sourceConfig.setVersion(Long.valueOf(tag));
    }

    sourceConfig.setType(SourceType.valueOf(getType()));
    sourceConfig.setConfig(getConfig().toByteString());
    sourceConfig.setName(getName());
    sourceConfig.setDescription(getDescription());
    sourceConfig.setCtime(this.createdAt);
    sourceConfig.setMetadataPolicy(getMetadataPolicy().toMetadataPolicy());
    sourceConfig.setAccelerationGracePeriod(getAccelerationGracePeriodMs());
    sourceConfig.setAccelerationRefreshPeriod(getAccelerationRefreshPeriodMs());
    return sourceConfig;
  }

  @JsonProperty("_type")
  public String getEntityType() {
    return "source";
  }
}
