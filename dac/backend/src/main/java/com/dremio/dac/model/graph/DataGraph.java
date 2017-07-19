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

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.source.proto.SourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * Dataset dependency graph.
 * For V1 we only track children, parents and sources. Field level tracking is not supported.
 *
 * For V1 datagraph consist of adjacent vertices in both directions.
 * Parents: list of parents current dataset is derived from.
 * Children: list of children who depend on current dataset.
 * Sources: list of sources dataset depend on.
 *
 * Each parent contains list of sources parent depends on. (mapping of parent to sources)
 */
@JsonIgnoreProperties(value={"origin", "originVersion", "links"}, allowGetters=true)
public class DataGraph {

  // used to generate links
  private final DatasetPath origin;
  private final DatasetVersion originVersion;

  // sent to client
  private final DatasetGraphNode dataset;
  private final List<DatasetGraphNodeWithSources>  parents;
  private final List<DatasetGraphNode> children;
  private final List<SourceGraphUI> sources;

  @JsonCreator
  public DataGraph(@JsonProperty("dataset") DatasetGraphNode dataset,
                   @JsonProperty("parents") List<DatasetGraphNodeWithSources> parents,
                   @JsonProperty("children") List<DatasetGraphNode> children,
                   @JsonProperty("sources") List<SourceGraphUI> sources) {
    this.dataset = dataset;
    this.parents = parents;
    this.children = children;
    this.sources = sources;
    this.origin = null;
    this.originVersion  = null;
  }

  public DataGraph(DatasetPath origin, DatasetVersion originVersion, DatasetConfig currentDataset, int jobCount, int descendants) {
    // if this is a datagraph centered at physical dataset add link to sources
    if (currentDataset.getType() == DatasetType.VIRTUAL_DATASET) {
      this.dataset = new DatasetGraphNode(origin, originVersion, currentDataset, jobCount, descendants);
    } else {
      this.dataset = new DatasetGraphNodeWithSources(origin, originVersion, currentDataset, jobCount, descendants);
    }
    this.parents = Lists.newArrayList();
    this.children = Lists.newArrayList();
    this.sources = Lists.newArrayList();
    this.origin = origin;
    this.originVersion = originVersion;
  }

  public DatasetGraphNode getDataset() {
    return dataset;
  }

  @JsonIgnore
  public void addChild(DatasetConfig childDataset, int jobCount, int descendants) {
    children.add(new DatasetGraphNode(origin, originVersion, childDataset, jobCount, descendants));
  }

  @JsonIgnore
  public void addSources(List<SourceGraphUI> sourcesList) {
    sources.addAll(sourcesList);
  }

  @JsonIgnore
  public void addParent(DatasetConfig parentDataset, int jobCount, int descendants) {
    parents.add(new DatasetGraphNodeWithSources(origin, originVersion, parentDataset, jobCount, descendants));
  }

  @JsonIgnore
  public DatasetVersion getOriginVersion() {
    return originVersion;
  }

  @JsonIgnore
  public DatasetPath getOrigin() {
    return origin;
  }

  public List<DatasetGraphNodeWithSources> getParents() {
    return parents;
  }

  public List<DatasetGraphNode> getChildren() {
    return children;
  }

  public List<SourceGraphUI> getSources() {
    return sources;
  }

  /**
   * source for data graph
   */
  public static class SourceGraphUI {
    private final String name;
    private final SourceType sourceType;
    private final boolean missing;

    public SourceGraphUI(String name, SourceType type) {
      this(name, type, false);
    }

    @JsonCreator
    public SourceGraphUI(@JsonProperty("name") String name,
                         @JsonProperty("sourceType") SourceType sourceType,
                         @JsonProperty("missing") Boolean missing) {
      this.name = name;
      this.sourceType = sourceType;
      this.missing = missing;
    }

    public String getName() {
      return name;
    }

    public boolean isMissing() {
      return missing;
    }

    public SourceType getSourceType() {
      return sourceType;
    }
  }

  public Map<String, String> getLinks() throws UnsupportedEncodingException {
    return dataset.getLinks();
  }
}
