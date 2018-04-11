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
package com.dremio.dac.api;

import java.util.List;

import com.dremio.dac.server.InputValidation;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Source model for the public REST API.
 */
public class Source implements CatalogEntity {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  private ConnectionConf<?, ?> config;
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
  private List<CatalogItem> children;

  private static final InputValidation validator = new InputValidation();

  public Source() {
  }

  public Source(SourceConfig config, AccelerationSettings settings, ConnectionReader reader) {
    this.id = config.getId().getId();
    this.tag = String.valueOf(config.getVersion());
    this.type = config.getType() == null ? config.getLegacySourceTypeEnum().name() : config.getType();
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

    validator.validate(this.metadataPolicy);

    this.accelerationGracePeriodMs = settings.getGracePeriod();
    this.accelerationRefreshPeriodMs = settings.getRefreshPeriod();

    // TODO: use our own config classes
    this.config = reader.getConnectionConf(config);
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

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public ConnectionConf<?, ?> getConfig() {
    return this.config;
  }

  public void setConfig(ConnectionConf<?, ?> config) {
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

    sourceConfig.setType(getConfig().getType());
    sourceConfig.setConfig(getConfig().toBytesString());
    sourceConfig.setName(getName());
    sourceConfig.setDescription(getDescription());
    sourceConfig.setCtime(this.createdAt);
    sourceConfig.setMetadataPolicy(getMetadataPolicy().toMetadataPolicy());
    sourceConfig.setAccelerationGracePeriod(getAccelerationGracePeriodMs());
    sourceConfig.setAccelerationRefreshPeriod(getAccelerationRefreshPeriodMs());
    return sourceConfig;
  }

  public List<CatalogItem> getChildren() {
    return children;
  }

  public void setChildren(List<CatalogItem> children) {
    this.children = children;
  }
}
