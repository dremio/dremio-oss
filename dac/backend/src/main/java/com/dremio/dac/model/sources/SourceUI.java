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
package com.dremio.dac.model.sources;

import com.dremio.dac.explore.DatasetResourceUtils;
import com.dremio.dac.model.common.AddressableResource;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.namespace.source.proto.SourceChangeState;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;

/** Source model */
// Ignoring the type hierarchy/Overriding DatasetContainer annotation
@JsonInclude(Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = SourceUI.class)
@JsonIgnoreProperties(
    value = {"links", "fullPathList", "resourcePath"},
    allowGetters = true,
    ignoreUnknown = true)
public class SourceUI implements AddressableResource, DatasetContainer {

  @Valid
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
      property = "type")
  private ConnectionConf<?, ?> config;

  @JsonProperty private NamespaceTree contents;

  private String name;
  private long ctime;
  private String img;
  private String description;
  private String id;
  private UIMetadataPolicy metadataPolicy = UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY;

  private Integer numberOfDatasets;
  private Boolean datasetCountBounded;
  private SourceState state;

  private String tag;
  // acceleration grace and refresh periods
  private Long accelerationGracePeriod;
  private Long accelerationRefreshPeriod;
  private RefreshPolicyType accelerationActivePolicyType;
  private String accelerationRefreshSchedule;
  private Boolean accelerationNeverExpire;
  private Boolean accelerationNeverRefresh;
  private Boolean allowCrossSourceSelection;
  private Boolean disableMetadataValidityCheck;
  private SourceChangeState sourceChangeState;

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

  public NamespaceAttribute[] getNamespaceAttributes() {
    return new NamespaceAttribute[0];
  }

  /**
   * For debugging purposes, the serialization for the REST API is also being done with Jackson, but
   * that is managed in the setup of the rest server.
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

  public SourceUI setNumberOfDatasets(Integer numberOfDatasets) {
    this.numberOfDatasets = numberOfDatasets;
    return this;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getNumberOfDatasets() {
    return numberOfDatasets;
  }

  @Pattern(
      regexp = "^[^.\"@]+$",
      message = "Source name can not contain periods, double quotes or @.")
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

  public RefreshPolicyType getAccelerationActivePolicyType() {
    return accelerationActivePolicyType;
  }

  public void setAccelerationActivePolicyType(RefreshPolicyType policyType) {
    this.accelerationActivePolicyType = policyType;
  }

  public String getAccelerationRefreshSchedule() {
    return accelerationRefreshSchedule;
  }

  public void setAccelerationRefreshSchedule(String refreshSchedule) {
    if (refreshSchedule != null && !DatasetResourceUtils.validateInputSchedule(refreshSchedule)) {
      throw new IllegalArgumentException(
          "refreshSchedule must be a cron expression only specifying one time and days of week");
    }
    this.accelerationRefreshSchedule = refreshSchedule;
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
    this.metadataPolicy =
        MoreObjects.firstNonNull(metadataPolicy, UIMetadataPolicy.DEFAULT_UIMETADATA_POLICY);
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

  public Boolean isDatasetCountBounded() {
    return datasetCountBounded;
  }

  public void setDatasetCountBounded(Boolean datasetCountBounded) {
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

  public void setAccelerationSettings(AccelerationSettings settings) {
    setAccelerationRefreshPeriod(settings.getRefreshPeriod());
    setAccelerationGracePeriod(settings.getGracePeriod());
    setAccelerationRefreshSchedule(settings.getRefreshSchedule());
    setAccelerationActivePolicyType(settings.getRefreshPolicyType());
  }

  public Boolean getAllowCrossSourceSelection() {
    return allowCrossSourceSelection;
  }

  public void setAllowCrossSourceSelection(Boolean allowCrossSourceSelection) {
    this.allowCrossSourceSelection = allowCrossSourceSelection;
  }

  public Boolean getDisableMetadataValidityCheck() {
    return disableMetadataValidityCheck;
  }

  public void setDisableMetadataValidityCheck(Boolean disableMetadataValidityCheck) {
    this.disableMetadataValidityCheck = disableMetadataValidityCheck;
  }

  public SourceChangeState getSourceChangeState() {
    return sourceChangeState;
  }

  public void setSourceChangeState(SourceChangeState sourceChangeState) {
    this.sourceChangeState = sourceChangeState;
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    String resourcePath = new SourcePath(new SourceName(name)).toUrlPath();
    links.put("self", resourcePath);
    links.put("rename", resourcePath + "/rename");
    final JobFilters jobFilters =
        new JobFilters()
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
    c.setAccelerationRefreshSchedule(accelerationRefreshSchedule);
    if (accelerationActivePolicyType != null) {
      c.setAccelerationActivePolicyType(accelerationActivePolicyType);
    }
    c.setAccelerationNeverExpire(Boolean.TRUE.equals(accelerationNeverExpire));
    c.setAccelerationNeverRefresh(Boolean.TRUE.equals(accelerationNeverRefresh));
    c.setMetadataPolicy(metadataPolicy.asMetadataPolicy());
    if (!Strings.isNullOrEmpty(getId())) {
      c.setId(new EntityId(getId()));
    }
    c.setAllowCrossSourceSelection(Boolean.TRUE.equals(allowCrossSourceSelection));
    c.setDisableMetadataValidityCheck(Boolean.TRUE.equals(disableMetadataValidityCheck));
    c.setSourceChangeState(sourceChangeState);
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
    source.setAccelerationActivePolicyType(sourceConfig.getAccelerationActivePolicyType());
    source.setAccelerationRefreshSchedule(sourceConfig.getAccelerationRefreshSchedule());
    source.setId(sourceConfig.getId().getId());
    source.setAllowCrossSourceSelection(sourceConfig.getAllowCrossSourceSelection());
    source.setDisableMetadataValidityCheck(sourceConfig.getDisableMetadataValidityCheck());
    source.setSourceChangeState(sourceConfig.getSourceChangeState());
    return source;
  }

  public static boolean isInternal(SourceConfig sourceConfig, ConnectionReader reader) {
    ConnectionConf<?, ?> config = reader.getConnectionConf(sourceConfig);

    return config.isInternal();
  }
}
