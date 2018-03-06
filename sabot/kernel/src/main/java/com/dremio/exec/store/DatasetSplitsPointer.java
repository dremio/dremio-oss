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
package com.dremio.exec.store;


import java.util.Map.Entry;
import java.util.Objects;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;

/**
 * Pointer to a set of splits for a given dataset config/split version.
 *
 * May be loaded lazily
 */
public final class DatasetSplitsPointer extends LazySplitsPointer {
  private final EntityId datasetId;
  private final long splitVersion;

  private final int splitsCount;

  private DatasetSplitsPointer(NamespaceService namespaceService, EntityId datasetId, long splitVersion, int splitsCount) {
    super(namespaceService, splitsCount);
    this.datasetId = datasetId;
    this.splitVersion = splitVersion;

    this.splitsCount = splitsCount;
  }

  public static SplitsPointer of(NamespaceService namespaceService, DatasetConfig datasetConfig) {
    final EntityId datasetId = Preconditions.checkNotNull(datasetConfig.getId());
    final ReadDefinition readDefinition = Preconditions.checkNotNull(datasetConfig.getReadDefinition());
    final long splitVersion = readDefinition.getSplitVersion();

    int splitsCount = namespaceService.getSplitCount(new FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)));
    return new DatasetSplitsPointer(namespaceService, datasetId, splitVersion, splitsCount);
  }

  @Override
  protected SearchQuery getPartitionQuery(SearchQuery partitionFilterQuery) {
    FindByCondition splitFilter = new FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetId, splitVersion));

    return SearchQueryUtils.and(splitFilter.getCondition(), partitionFilterQuery);
  }

  @Override
  protected Iterable<Entry<DatasetSplitId, DatasetSplit>> findSplits() {
    FindByRange<DatasetSplitId> filter = DatasetSplitId.getSplitsRange(datasetId, splitVersion);
    return getNamespaceService().findSplits(filter);
  }

  @Override
  protected int computeSplitsCount() {
    return splitsCount;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof DatasetSplitsPointer)) {
      return false;
    }
    DatasetSplitsPointer that = (DatasetSplitsPointer) other;
    return Objects.equals(this.datasetId, that.datasetId)
        && Objects.equals(this.splitVersion, that.splitVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetId, splitVersion);
  }
}
