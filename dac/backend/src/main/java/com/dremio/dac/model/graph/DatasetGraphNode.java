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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.dac.explore.DataTypeUtil;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.NamespacePathUtils;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Dataset properties for graph ui.
 */
@JsonIgnoreProperties(value={"origin", "originVersion", "links"}, allowGetters=true)
public class DatasetGraphNode {

  // used to generate links
  private final DatasetPath origin;
  private final DatasetVersion originVersion;

  private final String name;
  private final List<String> fullPath;
  private final int jobCount;
  private final int descendants;
  private final List<Field> fields;
  private final String owner;
  private final boolean missing;
  private final DatasetType datasetType;

  @JsonCreator
  public DatasetGraphNode(@JsonProperty("name") String name,
                          @JsonProperty("fullPath") List<String> fullPath,
                          @JsonProperty("jobCount") int jobCount,
                          @JsonProperty("descendants") int descendants,
                          @JsonProperty("fields") List<Field> fields,
                          @JsonProperty("owner") String owner,
                          @JsonProperty("missing") boolean missing,
                          @JsonProperty("datasetType") DatasetType datasetType) {
    this.name = name;
    this.fullPath = fullPath;
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.fields = fields;
    this.owner = owner;
    this.missing = missing;
    this.datasetType = datasetType;
    this.origin = null;
    this.originVersion = null;
  }

  public DatasetGraphNode(DatasetPath origin, DatasetVersion originVersion,
                          DatasetConfig datasetConfig, int jobCount, int descendants) {
    this.name = datasetConfig.getName();
    this.origin = origin;
    this.originVersion = originVersion;
    this.fullPath = datasetConfig.getFullPathList();
    this.owner = datasetConfig.getOwner();
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.datasetType = datasetConfig.getType();
    this.missing = (datasetType == null);
    final List<ViewFieldType> viewFieldTypes = ViewFieldsHelper.getViewFields(datasetConfig);
    if (viewFieldTypes != null) {
      this.fields = Lists.transform(viewFieldTypes, new Function<ViewFieldType, Field>() {
        @Nullable
        @Override
        public Field apply(@Nullable ViewFieldType input) {
          return new Field(input);
        }
      });
    } else {
      this.fields = null;
    }
  }

  public String getName() {
    return name;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  public List<String> getFullPath() {
    return fullPath;
  }

  public boolean isMissing() {
    return missing;
  }

  public int getJobCount() {
    return jobCount;
  }

  public int getDescendants() {
    return descendants;
  }

  public List<Field> getFields() {
    return fields;
  }

  public String getOwner() {
    return owner;
  }

  @JsonIgnore
  public DatasetPath getOrigin() {
    return origin;
  }

  @JsonIgnore
  public DatasetVersion getOriginVersion() {
    return originVersion;
  }

  /**
   * column info
   */
  public static class Field {
    private final String name;
    private final String type;

    public Field(ViewFieldType field) {
      this.name = field.getName();
      this.type = DataTypeUtil.getDataType(SqlTypeName.get(field.getType())).name();
    }

    @JsonCreator
    public Field(@JsonProperty("name") String name, @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  public Map<String, String> getLinks() throws UnsupportedEncodingException {
    Map<String, String> links = new HashMap<String, String>();
    final NamespaceKey currentPath = new NamespaceKey(fullPath);
    final NamespacePath namespacePath = NamespacePathUtils.getNamespacePathForDataType(datasetType, fullPath);

    links.put("self", namespacePath.toUrlPath());
    links.put("query", namespacePath.getQueryUrlPath());

    final JobFilters jobFilters = new JobFilters()
      .addFilter(JobIndexKeys.ALL_DATASETS, currentPath.toString())
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());


    return links;
  }
}
