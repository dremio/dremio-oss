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

import com.dremio.dac.model.common.AddressableResource;
import com.dremio.dac.model.common.ResourcePath;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raw dataset/table
 */
@JsonIgnoreProperties(value={"links"}, allowGetters=true)
public class PhysicalDataset implements AddressableResource {

  private final PhysicalDatasetConfig datasetConfig;
  private final PhysicalDatasetResourcePath resourcePath;
  private final PhysicalDatasetName datasetName;
  private final Integer jobCount;
  private final Integer descendants;

  @JsonCreator
  public PhysicalDataset(
    @JsonProperty("resourcePath") PhysicalDatasetResourcePath resourcePath,
    @JsonProperty("datasetName") PhysicalDatasetName datasetName,
    @JsonProperty("datasetConfig") PhysicalDatasetConfig datasetConfig,
    @JsonProperty("jobCount") Integer jobCount,
    @JsonProperty("descendants") Integer descendants) {
    this.resourcePath = resourcePath;
    this.datasetName = datasetName;
    this.datasetConfig = datasetConfig;
    this.jobCount = jobCount;
    this.descendants = descendants;
  }

  @Override
  public ResourcePath getResourcePath() {
    return resourcePath;
  }

  public PhysicalDatasetConfig getDatasetConfig() {
    return datasetConfig;
  }

  public PhysicalDatasetName getDatasetName() {
    return datasetName;
  }

  public Integer getJobCount() {
    return jobCount;
  }

  public Integer getDescendants() {
    return descendants;
  }

  public Map<String, String> getLinks() {
    List<String> fullPathList = datasetConfig.getFullPathList();
    PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(fullPathList);

    Map<String, String> links = new HashMap<>();
    links.put("self", datasetPath.toUrlPath());
    links.put("query", datasetPath.getQueryUrlPath());
    final JobFilters jobFilters = new JobFilters()
      .addFilter(JobIndexKeys.ALL_DATASETS, datasetPath.toString())
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    return links;
  }
}
