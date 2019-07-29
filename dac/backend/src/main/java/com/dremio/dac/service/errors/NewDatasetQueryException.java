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
package com.dremio.dac.service.errors;

import java.util.List;

import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * Base exception for resources not found
 *
 */
public class NewDatasetQueryException extends Exception {
  private static final long serialVersionUID = 1L;

  private final ExplorePageInfo details;

  public NewDatasetQueryException(ExplorePageInfo details, Exception error) {
    super(error.getMessage(), error);
    this.details = details;
  }

  public ExplorePageInfo getDetails() {
    return details;
  }

  /**
   * Basic information needed to populate the explore page to retry a failed initial preview.
   */
  public static class ExplorePageInfo {
    private final List<String> displayFullPath;
    private final String sql;
    private final List<String> context;
    private final DatasetType datasetType;
    private final DatasetSummary datasetSummary;

    public ExplorePageInfo(List<String> path, String sql, List<String> context, DatasetType datasetType, DatasetSummary datasetSummary) {
      this.displayFullPath = path;
      this.sql = sql;
      this.context = context;
      this.datasetType = datasetType;
      this.datasetSummary = datasetSummary;
    }

    public List<String> getDisplayFullPath() {
      return displayFullPath;
    }

    public String getSql() {
      return sql;
    }

    public List<String> getContext() {
      return context;
    }

    public DatasetType getDatasetType() {
      return datasetType;
    }

    public DatasetSummary getDatasetSummary() {
      return datasetSummary;
    }
  }
}
