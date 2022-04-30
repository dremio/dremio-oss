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
package com.dremio.dac.api;

import java.util.List;

import javax.validation.Valid;

import com.dremio.dac.server.InputValidation;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;

/**
 * Source model for the public REST API.
 */
public class Source implements CatalogEntity {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  @Valid
  private ConnectionConf<?, ?> config;
  private SourceConfig sourceConfig;
  private AccelerationSettings settings;
  private ConnectionReader reader;

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
  private Boolean accelerationNeverExpire;
  private Boolean accelerationNeverRefresh;
  private List<CatalogItem> children;
  private Boolean allowCrossSourceSelection;
  private Boolean disableMetadataValidityCheck;

  private static final InputValidation validator = new InputValidation();

  public Source() {
  }

  public Source(SourceConfig config, AccelerationSettings settings, ConnectionReader reader, List<CatalogItem> children) {
    this.sourceConfig = config;
    this.settings = settings;
    this.reader = reader;
    this.children = children;
    this.id = config.getId().getId();
    this.tag = config.getTag();
    this.type = config.getType() == null ? config.getLegacySourceTypeEnum().name() : config.getType();
    this.name = config.getName();
    this.description = config.getDescription();
    this.allowCrossSourceSelection = config.getAllowCrossSourceSelection();
    this.disableMetadataValidityCheck = config.getDisableMetadataValidityCheck();

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
    this.accelerationNeverExpire = settings.getNeverExpire();
    this.accelerationNeverRefresh = settings.getNeverRefresh();

    // TODO: use our own config classes
    this.config = reader.getConnectionConf(config);
    if (this.config != null) {
      // since this is a REST API class, clear any secrets
      this.config.clearSecrets();
    }
  }

  @JsonIgnore
  SourceConfig getSourceConfig() {
    return this.sourceConfig;
  }

  void setSourceConfig(SourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  @JsonIgnore
  AccelerationSettings getSettings() {
    return settings;
  }

  void setSettings(AccelerationSettings settings) {
    this.settings = settings;
  }

  @JsonIgnore
  ConnectionReader getReader() {
    return reader;
  }

  void setReader(ConnectionReader reader) {
    this.reader = reader;
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
    // We do NOT want to clear secrets here as this is called when receiving a REST API call which will include the
    // password during creation.
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

  public Boolean isAccelerationNeverExpire() {
    // Ensure that we always return true/false in case the setting is not passed in via the API (and thus null).
    // SourceConfig defaults to false and we want to match that behavior and not return null when the user doesn't pass
    // in the value.
    return Boolean.TRUE.equals(accelerationNeverExpire);
  }

  public void setAccelerationNeverExpire(Boolean accelerationNeverExpire) {
    this.accelerationNeverExpire = accelerationNeverExpire;
  }

  public Boolean isAccelerationNeverRefresh() {
    return Boolean.TRUE.equals(accelerationNeverRefresh);
  }

  public void setAccelerationNeverRefresh(Boolean accelerationNeverRefresh) {
    this.accelerationNeverRefresh = accelerationNeverRefresh;
  }

  public Boolean isAllowCrossSourceSelection() {
    // Ensure that we always return true/false in case the setting is not passed in via the API (and thus null).
    // SourceConfig defaults to false and we want to match that behavior and not return null when the user doesn't pass
    // in the value.
    return Boolean.TRUE.equals(allowCrossSourceSelection);
  }

  public void setAllowCrossSourceSelection(Boolean allowCrossSourceSelection) {
    this.allowCrossSourceSelection = allowCrossSourceSelection;
  }

  public Boolean isDisableMetadataValidityCheck() {
    // Ensure that we always return true/false in case the setting is not passed in via the API (and thus null).
    // SourceConfig defaults to false and we want to match that behavior and not return null when the user doesn't pass
    // in the value.
    return Boolean.TRUE.equals(disableMetadataValidityCheck);
  }

  public void setDisableMetadataValidityCheck(Boolean disableMetadataValidityCheck) {
    this.disableMetadataValidityCheck = disableMetadataValidityCheck;
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
    if (!Strings.isNullOrEmpty(getId())) {
      sourceConfig.setId(new EntityId(getId()));
    }

    String tag = getTag();
    if (tag != null) {
      sourceConfig.setTag(tag);
    }

    sourceConfig.setType(getConfig().getType());
    sourceConfig.setConfig(getConfig().toBytesString());
    sourceConfig.setName(getName());
    sourceConfig.setDescription(getDescription());
    sourceConfig.setCtime(this.createdAt);
    sourceConfig.setMetadataPolicy(getMetadataPolicy().toMetadataPolicy());
    sourceConfig.setAccelerationGracePeriod(getAccelerationGracePeriodMs());
    sourceConfig.setAccelerationRefreshPeriod(getAccelerationRefreshPeriodMs());
    sourceConfig.setAccelerationNeverExpire(isAccelerationNeverExpire());
    sourceConfig.setAccelerationNeverRefresh(isAccelerationNeverRefresh());
    sourceConfig.setAllowCrossSourceSelection(isAllowCrossSourceSelection());
    sourceConfig.setDisableMetadataValidityCheck(isDisableMetadataValidityCheck());
    return sourceConfig;
  }

  public List<CatalogItem> getChildren() {
    return children;
  }

  public void setChildren(List<CatalogItem> children) {
    this.children = children;
  }
}
