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
package com.dremio.dac.model.spaces;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Home space.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = Home.class)
@JsonIgnoreProperties(value={"fullPathList","links", "name", "owner", "ctime", "description", "id"}, allowGetters=true)
public class Home implements DatasetContainer {
  private HomePath homePath;

  private HomeConfig homeConfig;

  @JsonProperty
  private NamespaceTree contents;

  @JsonCreator
  public Home(
    @JsonProperty("homeConfig") HomeConfig homeConfig) {
    this.homePath = new HomePath(new HomeName(HomeName.HOME_PREFIX + homeConfig.getOwner()));
    this.homeConfig = homeConfig;
  }

  public Home(HomePath homePath,
              HomeConfig homeConfig) {
    this.homePath = homePath;
    this.homeConfig = homeConfig;
  }

  public HomeConfig getHomeConfig() {
    return homeConfig;
  }

  public NamespaceTree getContents() {
    return contents;
  }

  public void setContents(NamespaceTree contents) {
    this.contents = contents;
  }

  public String getId() {
    return homeConfig.getId().getId();
  }

  public List<String> getFullPathList() {
    return homePath.toPathList();
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    final String resourcePath = homePath.toUrlPath();
    links.put("self", resourcePath);
    final JobFilters jobFilters = new JobFilters()
      .addContainsFilter(homePath.getHomeName().toString())
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    links.put("file_format", resourcePath + "/file_format");
    links.put("file_prefix", resourcePath + "/file");
    links.put("upload_start", resourcePath + "/upload_start");
    return links;
  }

  @Override
  public String getName() {
    return "@" + homeConfig.getOwner();
  }

  public String getOwner() {
    return homeConfig.getOwner();
  }

  @Override
  public Long getCtime() {
    return homeConfig.getCtime();
  }

  @Override
  public String getDescription() {
    return "Personal space for user " + homeConfig.getOwner();
  }
}
