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

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkMetadata;

/**
 * Base class to {@code SplitPointer} types whose data is loaded lazily
 * until {@code LazySplitsPointer#materialize()} method is called.
 */
abstract class LazySplitsPointer extends AbstractSplitsPointer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LazySplitsPointer.class);

  private final NamespaceService namespaceService;
  private final long splitVersion;
  private final int totalSplitCount;

  protected LazySplitsPointer(NamespaceService namespaceService, long splitVersion, int totalSplitCount) {
    this.namespaceService = namespaceService;
    this.splitVersion = splitVersion;
    this.totalSplitCount = totalSplitCount;
  }

  protected NamespaceService getNamespaceService() {
    return namespaceService;
  }

  @Override
  public long getSplitVersion() {
    return splitVersion;
  }

  protected abstract SearchQuery getPartitionQuery(SearchQuery partitionFilterQuery);

  @Override
  public SplitsPointer prune(SearchQuery partitionFilterQuery) {
    if (partitionFilterQuery == null) {
      return this;
    }

    final SearchQuery query = getPartitionQuery(partitionFilterQuery);
    final int lastSplits = getSplitsCount();
    SplitsPointer newSplits = new FilteredSplitsPointer(namespaceService, splitVersion, query, totalSplitCount);

    if (newSplits.getSplitsCount() < lastSplits) {
      return newSplits;
    }
    return this;
  }

  protected abstract Iterable<PartitionChunkMetadata> findSplits();

  @Override
  public Iterable<PartitionChunkMetadata> getPartitionChunks() {
    return findSplits();
  }

  @Override
  public int getTotalSplitsCount() {
    return totalSplitCount;
  }
}
