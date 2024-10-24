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
package com.dremio.dac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchCatalogObject {
  private final List<String> path;
  private final String branch;
  private final String type;
  private final List<String> labels;
  private final String wiki;
  private final String createdAt;
  private final String lastModifiedAt;
  private final List<String> columns;
  private final String functionSql;

  @JsonCreator
  SearchCatalogObject(
      @JsonProperty("path") List<String> path,
      @JsonProperty("branch") String branch,
      @JsonProperty("type") String type,
      @JsonProperty("labels") List<String> labels,
      @JsonProperty("wiki") String wiki,
      @JsonProperty("createdAt") String createdAt,
      @JsonProperty("lastModifiedAt") String lastModifiedAt,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("functionSql") String functionSql) {
    this.path = path;
    this.branch = branch;
    this.type = type;
    this.labels = labels;
    this.wiki = wiki;
    this.createdAt = createdAt;
    this.lastModifiedAt = lastModifiedAt;
    this.columns = columns;
    this.functionSql = functionSql;
  }

  public List<String> getPath() {
    return path;
  }

  public String getBranch() {
    return branch;
  }

  public String getType() {
    return type;
  }

  public List<String> getLabels() {
    return labels;
  }

  public String getWiki() {
    return wiki;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getLastModifiedAt() {
    return lastModifiedAt;
  }

  public List<String> getColumns() {
    return columns;
  }

  public String getFunctionSql() {
    return functionSql;
  }
}
