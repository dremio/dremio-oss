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
package com.dremio.exec.catalog;

import java.util.Objects;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkMetadata;

/**
 * Pointer to a set of splits for a dataset, which has been filtered further using a search query.
 * May be loaded lazily.
 */
final class FilteredSplitsPointer extends LazySplitsPointer {
  private final FindByCondition splitFilter;

  FilteredSplitsPointer(
      NamespaceService namespaceService,
      long splitVersion,
      SearchQuery partitionFilterQuery,
      int totalSplitCount) {
    super(namespaceService, splitVersion, totalSplitCount);
    this.splitFilter = new FindByCondition().setCondition(partitionFilterQuery);
  }

  @Override
  protected SearchQuery getPartitionQuery(SearchQuery partitionFilterQuery) {
    return SearchQueryUtils.and(splitFilter.getCondition(), partitionFilterQuery);
  }

  @Override
  protected Iterable<PartitionChunkMetadata> findSplits() {
    return getNamespaceService().findSplits(splitFilter);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof FilteredSplitsPointer)) {
      return false;
    }

    FilteredSplitsPointer castOther = (FilteredSplitsPointer) other;
    // No need to compare anything else: the filter is based on the dataset id, split version
    // and some specific conditions. Result of the filter should always be the same
    return this.getSplitVersion() == castOther.getSplitVersion()
        && Objects.equals(splitFilter, castOther.splitFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSplitVersion(), splitFilter);
  }
}
