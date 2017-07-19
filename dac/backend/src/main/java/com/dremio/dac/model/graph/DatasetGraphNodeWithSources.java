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
package com.dremio.dac.model.graph;

import java.util.List;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Graph model for parent dataset carries information about source mappings.
 */
public class DatasetGraphNodeWithSources extends DatasetGraphNode {

  private final List<String> sources;

  @JsonCreator
  public DatasetGraphNodeWithSources(@JsonProperty("name") String name,
                                     @JsonProperty("fullPath") List<String> fullPath,
                                     @JsonProperty("jobCount") int jobCount,
                                     @JsonProperty("descendants") int descendants,
                                     @JsonProperty("fields") List<Field> fields,
                                     @JsonProperty("owner") String owner,
                                     @JsonProperty("missing") boolean missing,
                                     @JsonProperty("datasetType") DatasetType datasetType,
                                     @JsonProperty("sources") List<String> sources) {
    super(name, fullPath, jobCount, descendants, fields, owner, missing, datasetType);
    this.sources = sources;
  }

  public DatasetGraphNodeWithSources(DatasetPath origin, DatasetVersion originVersion, DatasetConfig datasetConfig, int jobCount, int descendants) {
    super(origin, originVersion, datasetConfig, jobCount, descendants);
    this.sources = QueryMetadata.getSources(datasetConfig);
  }

  public List<String> getSources() {
    return sources;
  }
}
