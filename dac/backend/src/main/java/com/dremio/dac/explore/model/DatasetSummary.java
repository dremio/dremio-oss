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
package com.dremio.dac.explore.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Dataset summary for overlay
 */
@JsonIgnoreProperties(value={"links", "apiLinks"}, allowGetters=true)
public class DatasetSummary {
  private final List<String> fullPath;
  private final int jobCount;
  private final int descendants;
  private final List<Field> fields;
  private final DatasetType datasetType;
  private final DatasetVersion datasetVersion;

  public DatasetSummary(@JsonProperty("fullPath") List<String> fullPath,
                        @JsonProperty("jobCount") int jobCount,
                        @JsonProperty("descendants") int descendants,
                        @JsonProperty("fields") List<Field> fields,
                        @JsonProperty("datasetType") DatasetType datasetType,
                        @JsonProperty("datasetVersion") DatasetVersion datasetVersion) {
    this.fullPath = fullPath;
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.fields = fields;
    this.datasetType = datasetType;
    this.datasetVersion = datasetVersion;
  }

  public static DatasetSummary newInstance(DatasetConfig datasetConfig, int jobCount, int descendants)
      throws NamespaceException {
    List<String> fullPath = datasetConfig.getFullPathList();

    DatasetType datasetType = datasetConfig.getType();
    List<Field> fields;
    DatasetVersion datasetVersion;

    List<com.dremio.dac.model.common.Field> fieldList = DatasetsUtil.getFieldsFromDatasetConfig(datasetConfig);
    if (fieldList == null) {
      fields = null;
    } else {
      fields = Lists.transform(fieldList, new Function<com.dremio.dac.model.common.Field, Field>() {
        @Override
        public Field apply(com.dremio.dac.model.common.Field input) {
          return new Field(input.getName(), input.getType().name());
        }
      });
    }

    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();
      datasetVersion = virtualDataset.getVersion();
    } else {
      datasetVersion = null;
    }

    return new DatasetSummary(fullPath, jobCount, descendants, fields, datasetType, datasetVersion);
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

  // links
  public Map<String, String> getLinks() {
    DatasetPath datasetPath = new DatasetPath(fullPath);
    Map<String, String> links = new HashMap<>();

    links.put("self", datasetPath.toUrlPath());
    links.put("query", datasetPath.getQueryUrlPath());
    links.put("jobs", this.getJobsUrl());
    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      links.put("edit", datasetPath.toUrlPath() + "?mode=edit&version=" + datasetVersion);
    }
    return links;
  }

  // api links
  public Map<String, String> getApiLinks() {
    final Map<String, String> links = Maps.newHashMap();
    final NamespaceKey datasetPath = new NamespaceKey(fullPath);
    final String dottedFullPath = datasetPath.toUrlEncodedString();
    final String fullPathString = PathUtils.toFSPath(fullPath).toString();

    links.put("jobs", this.getJobsUrl());
    switch (datasetType) {
      case VIRTUAL_DATASET:
        links.put("edit", "/dataset/" + dottedFullPath + "/version/" + datasetVersion + "/preview"); // edit dataset
        links.put("run", "/datasets/new_untitled?parentDataset=" + dottedFullPath + "&newVersion=" + DatasetVersion.newVersion()); //create new dataset
        break;
      case PHYSICAL_DATASET_HOME_FILE:
        links.put("run", "/home/" + fullPath.get(0) + "new_untitled_from_file" + fullPathString);
        break;
      case PHYSICAL_DATASET_HOME_FOLDER:
        // Folder not supported yet
        break;
      case PHYSICAL_DATASET_SOURCE_FILE:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_file" + fullPathString);
        break;
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_folder" + fullPathString);
        break;
      case PHYSICAL_DATASET:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_physical_dataset" + fullPathString);
        break;
      default:
        break;
    }
    return links;
  }

  private String getJobsUrl() {
    final NamespaceKey datasetPath = new NamespaceKey(fullPath);
    final JobFilters jobFilters = new JobFilters()
      .addFilter(JobIndexKeys.ALL_DATASETS, datasetPath.toString())
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    return jobFilters.toUrl();
  }

}
