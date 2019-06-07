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
package com.dremio.exec.catalog;

import java.util.Objects;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
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

  private DatasetSplitsPointer(NamespaceService namespaceService, EntityId datasetId, long splitVersion, int splitsCount) {
    super(namespaceService, splitVersion, splitsCount);
    this.datasetId = datasetId;
  }

  public static SplitsPointer of(NamespaceService namespaceService, DatasetConfig datasetConfig) {
    final EntityId datasetId = Preconditions.checkNotNull(datasetConfig.getId());
    final ReadDefinition readDefinition = Preconditions.checkNotNull(datasetConfig.getReadDefinition(),
        "extended metadata (read definition) is not available");
    final long splitVersion = readDefinition.getSplitVersion();

    final int splitsCount;
    if (datasetConfig.getTotalNumSplits() != null) {
      splitsCount = datasetConfig.getTotalNumSplits();
    } else {
      // Backwards compatibility: if the total number of splits is not set, then this datasetConfig must be from
      // before the connector metadata API. At that time, each PartitionChunk represented a single split
      splitsCount = namespaceService.getPartitionChunkCount(new FindByCondition().setCondition(PartitionChunkId.getSplitsQuery(datasetConfig)));
    }
    return new DatasetSplitsPointer(namespaceService, datasetId, splitVersion, splitsCount);
  }

  @Override
  protected SearchQuery getPartitionQuery(SearchQuery partitionFilterQuery) {
    FindByCondition splitFilter = new FindByCondition().setCondition(PartitionChunkId.getSplitsQuery(datasetId, getSplitVersion()));

    return SearchQueryUtils.and(splitFilter.getCondition(), partitionFilterQuery);
  }

  @Override
  protected Iterable<PartitionChunkMetadata> findSplits() {
    FindByRange<PartitionChunkId> filter = PartitionChunkId.getSplitsRange(datasetId, getSplitVersion());
    return getNamespaceService().findSplits(filter);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof DatasetSplitsPointer)) {
      return false;
    }
    DatasetSplitsPointer that = (DatasetSplitsPointer) other;
    return Objects.equals(this.datasetId, that.datasetId)
        && this.getSplitVersion() == that.getSplitVersion();
  }

  @Override
  public double getSplitRatio() {
    return 1.0d;
  }

  @Override
  public int getSplitsCount() {
    return getTotalSplitsCount();
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetId, getSplitVersion());
  }
}
