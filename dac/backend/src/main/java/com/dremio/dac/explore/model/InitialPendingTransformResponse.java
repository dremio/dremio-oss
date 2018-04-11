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
package com.dremio.dac.explore.model;

import java.util.List;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.resource.JobResource;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * The result of a transform preview
 */
public class InitialPendingTransformResponse {

  private final String sql;
  private final JobDataFragment data;
  private final String paginationUrl;
  private final List<String> highlightedColumns;
  private final List<String> deletedColumns;
  private final List<String> rowDeletionMarkerColumns;

  @JsonCreator
  public InitialPendingTransformResponse(
      @JsonProperty("sql") String sql,
      @JsonProperty("data") JobDataFragment data,
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("highlightedColumns") List<String> highlightedColumns,
      @JsonProperty("deletedColumns") List<String> deletedColumns,
      @JsonProperty("rowDeletionMarkerColumns") List<String> rowDeletionMarkerColumns) {
    this.sql = sql;
    this.data = data;
    this.paginationUrl = paginationUrl;
    this.highlightedColumns = highlightedColumns != null ? ImmutableList.copyOf(highlightedColumns) : null;
    this.deletedColumns = deletedColumns != null ? ImmutableList.copyOf(deletedColumns) : null;
    this.rowDeletionMarkerColumns =
        rowDeletionMarkerColumns != null ? ImmutableList.copyOf(rowDeletionMarkerColumns) : null;
  }

  public static InitialPendingTransformResponse of(String sql, JobDataFragment data,
      List<String> highlightedColumns, List<String> deletedColumns, List<String> rowDeletionMarkerColumns) {

    return new InitialPendingTransformResponse(sql, data, JobResource.getPaginationURL(data.getJobId()), highlightedColumns, deletedColumns, rowDeletionMarkerColumns);
  }

  public String getSql() {
    return sql;
  }

  public JobDataFragment getData() {
    return data;
  }

  public String getPaginationUrl() {
    return paginationUrl;
  }

  public List<String> getHighlightedColumns() {
    return highlightedColumns;
  }

  public List<String> getDeletedColumns() {
    return deletedColumns;
  }

  public List<String> getRowDeletionMarkerColumns() {
    return rowDeletionMarkerColumns;
  }
}
