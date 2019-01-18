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
package com.dremio.dac.model.spaces;

import static java.lang.String.format;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Strings.isNullOrEmpty;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.Pattern;
import javax.ws.rs.DefaultValue;

import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Space model.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = Space.class)
@JsonIgnoreProperties(value={ "links", "fullPathList"}, allowGetters=true)
public class Space implements DatasetContainer {
  private final String id;
  private final String name;
  private final String description;
  private final int datasetCount;
  private final String version;
  private final NamespaceTree contents;
  private final Long ctime;

  @JsonCreator
  public Space(
    @JsonProperty("id") @DefaultValue("null") String id, // default is null for new spaces
    @JsonProperty("name") String name,
    @JsonProperty("description") String description,
    @JsonProperty("version") String version,
    @JsonProperty("contents") NamespaceTree contents,
    @JsonProperty("datasetCount") int datasetCount,
    @JsonProperty("ctime") Long ctime
  ) {
    checkArgument(!isNullOrEmpty(name), "space name can not be empty");
    this.id = id;
    this.name = name;
    this.description = description;
    this.version = version;
    this.contents = contents;
    this.datasetCount = datasetCount;
    this.ctime = ctime;
  }

  public String getId() {
    return id;
  }

  public NamespaceTree getContents() {
    return contents;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  public long getDatasetCount() {
    return datasetCount;
  }

  public List<String> getFullPathList() {
    return ImmutableList.of(name);
  }

  public Map<String, String> getLinks() {
    final SpacePath spacePath = new SpacePath(new SpaceName(name));
    final String resourcePath = spacePath.toUrlPath();
    return ImmutableMap.of("self", resourcePath,
      "jobs", format("/jobs?filter=(*=contains=%s);(qt==UI,qt==EXTERNAL)", spacePath.getSpaceName()),
      "rename", resourcePath + "/rename");
  }

  @Pattern(regexp = "^[^.\"@]+$", message = "Space name can not contain periods, double quotes or @.")
  @Override
  public String getName() {
    return name;
  }

  @Override
  public Long getCtime() {
    return ctime;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public String getVersion() {
    return version;
  }

  public static Space newInstance(SpaceConfig spaceConfig, NamespaceTree contents, int datasetCount) {
    String id = spaceConfig.getId() != null ? spaceConfig.getId().getId() : null;
    return new Space(id, spaceConfig.getName(), spaceConfig.getDescription(), spaceConfig.getTag(), contents, datasetCount, spaceConfig.getCtime());
  }
}
