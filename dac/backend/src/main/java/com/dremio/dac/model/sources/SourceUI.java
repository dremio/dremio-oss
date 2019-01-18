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
package com.dremio.dac.model.sources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.validation.Valid;
import javax.validation.constraints.Pattern;

import com.dremio.dac.model.common.AddressableResource;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

/**
 * Source model
 */
// Ignoring the type hierarchy/Overriding DatasetContainer annotation
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = SourceUI.class)
@JsonIgnoreProperties(value={ "links", "fullPathList", "resourcePath" }, allowGetters=true, ignoreUnknown=true)
public class SourceUI implements AddressableResource, DatasetContainer {

  @Valid
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
  private ConnectionConf<?, ?> config;

  @JsonProperty
  private NamespaceTree contents;

  private String name;
  private long ctime;
  private String img;
  private String description;
  private String id;
  private UIMetadataPolicy metadataPolicy = UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY;

  private long numberOfDatasets;
  private boolean datasetCountBounded = false;
  private SourceState state;

  private String tag;
  // acceleration grace and refresh periods
  private Long accelerationGracePeriod;
  private Long accelerationRefreshPeriod;
  private Boolean accelerationNeverExpire;
  private Boolean accelerationNeverRefresh;

  public SourceUI setConfig(ConnectionConf<?, ?> sourceConfig) {
    this.config = sourceConfig;
    return this;
  }

  public ConnectionConf<?, ?> getConfig() {
    return config;
  }

  public void clearSecrets() {
    config.clearSecrets();
  }

  /**
   * For debugging purposes, the serialization for the REST API is also
   * being done with Jackson, but that is managed in the setup of the rest server.
   */
  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing object.", e);
    }
  }

  public String getId() {
    return id;
  }

  @Override
  public SourceResourcePath getResourcePath() {
    return new SourceResourcePath(new SourceName(name));
  }

  public SourceUI setNumberOfDatasets(long numberOfDatasets) {
    this.numberOfDatasets = numberOfDatasets;
    return this;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getNumberOfDatasets() {
    return numberOfDatasets;
  }

  @Pattern(regexp = "^[^.\"@]+$", message = "Source name can not contain periods, double quotes or @.")
  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public Long getCtime() {
    return ctime;
  }

  public Long getAccelerationRefreshPeriod() {
    return accelerationRefreshPeriod;
  }

  public void setAccelerationRefreshPeriod(Long accelerationRefreshPeriod) {
    this.accelerationRefreshPeriod = accelerationRefreshPeriod;
  }

  public Long getAccelerationGracePeriod() {
    return accelerationGracePeriod;
  }

  public void setAccelerationGracePeriod(Long accelerationGracePeriod) {
    this.accelerationGracePeriod = accelerationGracePeriod;
  }

  public void setCtime(long ctime) {
    this.ctime = ctime;
  }

  public SourceState getState() {
    return state;
  }

  public void setState(SourceState state) {
    this.state = state;
  }

  public UIMetadataPolicy getMetadataPolicy() {
    return metadataPolicy;
  }

  public void setMetadataPolicy(UIMetadataPolicy metadataPolicy) {
    this.metadataPolicy = MoreObjects.firstNonNull(metadataPolicy, UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY);
  }

  public String getImg() {
    return img;
  }

  public void setImg(String img) {
    this.img = img;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public NamespaceTree getContents() {
    return contents;
  }

  public void setContents(NamespaceTree contents) {
    this.contents = contents;
  }

  public boolean isDatasetCountBounded() {
    return datasetCountBounded;
  }

  public void setDatasetCountBounded(boolean datasetCountBounded) {
    this.datasetCountBounded = datasetCountBounded;
  }

  public List<String> getFullPathList() {
    return ImmutableList.of(name);
  }

  public Boolean getAccelerationNeverExpire() {
    return accelerationNeverExpire;
  }

  public void setAccelerationNeverExpire(Boolean accelerationNeverExpire) {
    this.accelerationNeverExpire = accelerationNeverExpire;
  }

  public Boolean getAccelerationNeverRefresh() {
    return accelerationNeverRefresh;
  }

  public void setAccelerationNeverRefresh(Boolean accelerationNeverRefresh) {
    this.accelerationNeverRefresh = accelerationNeverRefresh;
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    String resourcePath = new SourcePath(new SourceName(name)).toUrlPath();
    links.put("self", resourcePath);
    links.put("rename", resourcePath + "/rename");
    final JobFilters jobFilters = new JobFilters()
      .addContainsFilter(name)
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    links.put("format", resourcePath + "/folder_format");
    links.put("file_format", resourcePath + "/file_format");
    links.put("file_preview", resourcePath + "/file_preview");
    return links;
  }

  @JsonIgnore
  public SourceConfig asSourceConfig() {
    SourceConfig c = new SourceConfig();
    c.setType(config.getType());
    c.setName(name);
    c.setCtime(ctime);
    c.setImg(img);
    c.setConfig(config.toBytesString());
    c.setDescription(description);
    c.setTag(tag);
    c.setAccelerationRefreshPeriod(accelerationRefreshPeriod);
    c.setAccelerationGracePeriod(accelerationGracePeriod);
    c.setAccelerationNeverExpire(Boolean.TRUE.equals(accelerationNeverExpire));
    c.setAccelerationNeverRefresh(Boolean.TRUE.equals(accelerationNeverRefresh));
    c.setMetadataPolicy(metadataPolicy.asMetadataPolicy());
    c.setId(new EntityId(getId()));
    return c;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj != null && obj instanceof SourceUI) {
      final SourceUI s = (SourceUI) obj;
      // Name uniquely identifies a source. No need to consider the remaining properties.
      return Objects.equals(name, s.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    // Name uniquely identifies a source. No need to consider the remaining properties.
    return name.hashCode();
  }

  public static SourceUI get(SourceConfig sourceConfig, ConnectionReader reader) {
    SourceUI source = getWithPrivates(sourceConfig, reader);
    // we should not set fields that expose passwords and other private parts of the config
    source.config.clearSecrets();
    return source;
  }

  public static SourceUI getWithPrivates(SourceConfig sourceConfig, ConnectionReader reader) {
    SourceUI source = new SourceUI();
    ConnectionConf<?, ?> config = reader.getConnectionConf(sourceConfig);
    source.setConfig(config);
    source.setCtime(sourceConfig.getCtime());
    source.setDescription(sourceConfig.getDescription());
    source.setImg(sourceConfig.getImg());
    source.setName(sourceConfig.getName());
    source.setState(SourceState.GOOD);
    source.setMetadataPolicy(UIMetadataPolicy.of(sourceConfig.getMetadataPolicy()));
    source.setTag(sourceConfig.getTag());
    source.setAccelerationRefreshPeriod(sourceConfig.getAccelerationRefreshPeriod());
    source.setAccelerationGracePeriod(sourceConfig.getAccelerationGracePeriod());
    source.setAccelerationNeverExpire(sourceConfig.getAccelerationNeverExpire());
    source.setAccelerationNeverRefresh(sourceConfig.getAccelerationNeverRefresh());
    source.setId(sourceConfig.getId().getId());
    return source;
  }


  public static boolean isInternal(SourceConfig sourceConfig, ConnectionReader reader) {
    ConnectionConf<?, ?> config = reader.getConnectionConf(sourceConfig);

    return config.isInternal();
  }
}
