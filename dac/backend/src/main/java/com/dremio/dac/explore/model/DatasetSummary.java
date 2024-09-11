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
package com.dremio.dac.explore.model;

import static com.dremio.common.utils.PathUtils.encodeURIComponent;

import com.dremio.dac.api.JsonISODateTime;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergViewAttributes;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.users.User;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Dataset summary for overlay */
@JsonIgnoreProperties(
    value = {"links", "apiLinks"},
    allowGetters = true)
public class DatasetSummary {
  private final List<String> fullPath;
  private final int jobCount;
  private final int descendants;
  private final List<Field> fields;
  private final DatasetType datasetType;
  private final DatasetVersion datasetVersion;
  private final Map<String, VersionContextReq> references;
  private final List<String> tags;
  private final String entityId;
  private Boolean hasReflection;
  private final String ownerName;
  private final String ownerEmail;
  private final String lastModifyingUserName;
  private final String lastModifyingUserEmail;
  @JsonISODateTime private final Long createdAt;
  @JsonISODateTime private final Long lastModified;
  private final String viewSpecVersion;
  private final Boolean schemaOutdated;
  private final String viewDialect;

  public DatasetSummary(
      @JsonProperty("fullPath") List<String> fullPath,
      @JsonProperty("jobCount") int jobCount,
      @JsonProperty("descendants") int descendants,
      @JsonProperty("fields") List<Field> fields,
      @JsonProperty("datasetType") DatasetType datasetType,
      @JsonProperty("datasetVersion") DatasetVersion datasetVersion,
      @JsonProperty("tags") List<String> tags,
      @JsonProperty("references") Map<String, VersionContextReq> references,
      @JsonProperty("entityId") String entityId,
      @JsonProperty("hasReflection") Boolean hasReflection,
      @JsonProperty("ownerName") String ownerName,
      @JsonProperty("ownerEmail") String ownerEmail,
      @JsonProperty("lastModifyingUserName") String lastModifyingUserName,
      @JsonProperty("lastModifyingUserEmail") String lastModifyingUserEmail,
      @JsonProperty("createdAt") Long createdAt,
      @JsonProperty("lastModified") Long lastModified,
      @JsonProperty("viewSpecVersion") String viewSpecVersion,
      @JsonProperty("schemaOutdated") Boolean schemaOutdated,
      @JsonProperty("viewDialect") String viewDialect) {
    this.fullPath = fullPath;
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.fields = fields;
    this.datasetType = datasetType;
    this.datasetVersion = datasetVersion;
    this.tags = tags;
    this.references = references;
    this.entityId = entityId;
    this.hasReflection = hasReflection;
    this.ownerName = ownerName;
    this.ownerEmail = ownerEmail;
    this.lastModifyingUserName = lastModifyingUserName;
    this.lastModifyingUserEmail = lastModifyingUserEmail;
    this.createdAt = createdAt;
    this.lastModified = lastModified;
    this.viewSpecVersion = viewSpecVersion;
    this.schemaOutdated = schemaOutdated;
    this.viewDialect = viewDialect;
  }

  public static DatasetSummary newInstance(
      DatasetConfig datasetConfig,
      int jobCount,
      int descendants,
      Map<String, VersionContextReq> references,
      List<String> tags,
      Boolean hasReflection,
      User owner,
      User lastModifyingUser) {
    List<String> fullPath = datasetConfig.getFullPathList();

    DatasetType datasetType = datasetConfig.getType();
    List<Field> fields; // here
    DatasetVersion datasetVersion;
    String viewSpecVersion = null;
    String viewDialect = null;

    List<com.dremio.dac.model.common.Field> fieldList =
        DatasetsUtil.getFieldsFromDatasetConfig(datasetConfig);
    if (fieldList == null) {
      fields = null;
    } else {
      final Set<String> partitionedColumnsSet = DatasetsUtil.getPartitionedColumns(datasetConfig);
      final Set<String> sortedColumns = DatasetsUtil.getSortedColumns(datasetConfig);

      fields =
          Lists.transform(
              fieldList,
              new Function<com.dremio.dac.model.common.Field, Field>() {
                @Override
                public Field apply(com.dremio.dac.model.common.Field input) {
                  return new Field(
                      input.getName(),
                      input.getType().name(),
                      partitionedColumnsSet.contains(input.getName()),
                      sortedColumns.contains(input.getName()));
                }
              });
    }

    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();
      datasetVersion = virtualDataset.getVersion();
      IcebergViewAttributes icebergViewAttributes =
          datasetConfig.getVirtualDataset().getIcebergViewAttributes();
      viewSpecVersion =
          icebergViewAttributes != null ? icebergViewAttributes.getViewSpecVersion() : null;
      viewDialect = icebergViewAttributes != null ? icebergViewAttributes.getViewDialect() : null;
    } else {
      datasetVersion = null;
    }

    final String entityId = datasetConfig.getId() == null ? null : datasetConfig.getId().getId();
    final String ownerName = owner != null ? owner.getUserName() : null;
    final String ownerEmail = owner != null ? owner.getEmail() : null;
    final String lastModifyingUserName =
        lastModifyingUser != null ? lastModifyingUser.getUserName() : null;
    final String lastModifyingUserEmail =
        lastModifyingUser != null ? lastModifyingUser.getEmail() : null;
    final Long createdAt = datasetConfig.getCreatedAt();
    final Long lastModified = datasetConfig.getLastModified();

    return new DatasetSummary(
        fullPath,
        jobCount,
        descendants,
        fields,
        datasetType,
        datasetVersion,
        tags,
        references,
        entityId,
        hasReflection,
        ownerName,
        ownerEmail,
        lastModifyingUserName,
        lastModifyingUserEmail,
        createdAt,
        lastModified,
        viewSpecVersion,
        NamespaceUtils.isSchemaOutdated(datasetConfig),
        viewDialect);
  }

  public DatasetVersion getDatasetVersion() {
    return datasetVersion;
  }

  public List<String> getFullPath() {
    return fullPath;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  public Integer getJobCount() {
    return jobCount;
  }

  public Integer getDescendants() {
    return descendants;
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<String> getTags() {
    return tags;
  }

  public Map<String, VersionContextReq> getReferences() {
    return references;
  }

  public String getEntityId() {
    return entityId;
  }

  public Boolean getHasReflection() {
    return hasReflection;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public String getOwnerEmail() {
    return ownerEmail;
  }

  public String getLastModifyingUserName() {
    return lastModifyingUserName;
  }

  public String getLastModifyingUserEmail() {
    return lastModifyingUserEmail;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public Long getLastModified() {
    return lastModified;
  }

  public String getViewSpecVersion() {
    return viewSpecVersion;
  }

  public String getViewDialect() {
    return viewDialect;
  }

  public Boolean getSchemaOutdated() {
    return schemaOutdated;
  }

  // links
  // TODO make this consistent with DatasetUI.createLinks. In ideal case, both methods should use
  // the same util method
  public Map<String, String> getLinks() {
    DatasetPath datasetPath = new DatasetPath(fullPath);
    Map<String, String> links = new HashMap<>();

    links.put("self", datasetPath.toUrlPath());
    links.put("query", datasetPath.getQueryUrlPath());
    links.put("jobs", this.getJobsUrl());
    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      links.put(
          "edit",
          datasetPath.getQueryUrlPath()
              + "?mode=edit&version="
              + (datasetVersion == null
                  ? datasetVersion
                  : encodeURIComponent(datasetVersion.toString())));
    }
    return links;
  }

  private String getJobsUrl() {
    final NamespaceKey datasetPath = new NamespaceKey(fullPath);
    final JobFilters jobFilters =
        new JobFilters()
            .addFilter(JobIndexKeys.ALL_DATASETS, datasetPath.toString())
            .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    return jobFilters.toUrl();
  }
}
