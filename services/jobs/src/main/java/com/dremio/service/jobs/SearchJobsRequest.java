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
package com.dremio.service.jobs;

import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.ImmutableList;

/**
 * Request to search jobs.
 */
public final class SearchJobsRequest {

  // Sort by descending order of start time. (recently submitted jobs come on top)
  static final List<SearchFieldSorting> DEFAULT_SORTER = ImmutableList.of(
      JobIndexKeys.START_TIME.toSortField(SortOrder.DESCENDING),
      JobIndexKeys.END_TIME.toSortField(SortOrder.DESCENDING),
      JobIndexKeys.JOBID.toSortField(SortOrder.DESCENDING));

  private final String username;
  private final FindByCondition condition;

  private SearchJobsRequest(String username, FindByCondition condition) {
    this.username = username;
    this.condition = condition;
  }

  FindByCondition getCondition() {
    return condition;
  }

  String getUsername() {
    return username;
  }

  /**
   * Search jobs request builder.
   */
  public static final class Builder {

    private NamespaceKey datasetPath;
    private DatasetVersion datasetVersion;

    private String filterString;

    private String username;

    private int offset = -1;
    private int limit = -1;

    private String sortColumn;
    private ResultOrder resultOrder = ResultOrder.ASCENDING;

    private Builder() {
    }

    public Builder setDatasetPath(NamespaceKey datasetPath) {
      this.datasetPath = datasetPath;
      return this;
    }

    public Builder setDatasetVersion(DatasetVersion datasetVersion) {
      this.datasetVersion = datasetVersion;
      return this;
    }

    public Builder setFilterString(String filterString) {
      this.filterString = filterString;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setOffset(int offset) {
      this.offset = offset;
      return this;
    }

    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder setSortColumn(String sortColumn) {
      this.sortColumn = sortColumn;
      return this;
    }

    public Builder setResultOrder(ResultOrder resultOrder) {
      this.resultOrder = resultOrder;
      return this;
    }

    public SearchJobsRequest build() {

      final FindByCondition condition = new FindByCondition();
      if (datasetPath != null) {
        condition.setCondition(getDatasetFilter());
      } else {
        condition.setCondition(filterString, JobIndexKeys.MAPPING);
      }

      if (offset > 0) {
        condition.setOffset(offset);
      }

      if (limit > 0) {
        condition.setLimit(limit);
      }

      if (sortColumn != null) {
        condition.addSortings(buildSorter(sortColumn, resultOrder.toSortOrder()));
      }

      return new SearchJobsRequest(username, condition);
    }

    private SearchTypes.SearchQuery getDatasetFilter() {
      final ImmutableList.Builder<SearchTypes.SearchQuery> builder =
          ImmutableList.<SearchTypes.SearchQuery>builder()
              .add(SearchQueryUtils.newTermQuery(JobIndexKeys.ALL_DATASETS, datasetPath.toString()))
              .add(JobIndexKeys.UI_EXTERNAL_JOBS_FILTER);

      if (datasetVersion != null) {
        builder.add(SearchQueryUtils.newTermQuery(JobIndexKeys.DATASET_VERSION, datasetVersion.getVersion()));
      }

      // TODO(DX-17909): this must be provided for authorization purposes
      if (username != null) {
        builder.add((SearchQueryUtils.newTermQuery(JobIndexKeys.USER, username)));
      }

      return SearchQueryUtils.and(builder.build());
    }
  }

  private static List<SearchFieldSorting> buildSorter(final String shortKey, final SortOrder order) {
    if (shortKey != null) {
      final IndexKey key = JobIndexKeys.MAPPING.getKey(shortKey);
      if(key == null || !key.isSorted()){
        throw UserException.functionError()
            .message("Unable to sort by field {}.", shortKey)
            .buildSilently();
      }
      return ImmutableList.of(key.toSortField(order));
    }

    return DEFAULT_SORTER;
  }

  /**
   * Create a new search jobs request builder.
   *
   * @return new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Order of results.
   */
  public enum ResultOrder {
    ASCENDING {
      @Override
      SortOrder toSortOrder() {
        return SortOrder.ASCENDING;
      }
    },

    DESCENDING {
      @Override
      SortOrder toSortOrder() {
        return SortOrder.DESCENDING;
      }
    };

    abstract SortOrder toSortOrder();
  }
}
