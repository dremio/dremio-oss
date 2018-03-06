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


import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Base class to {@code SplitPointer} types whose data is loaded lazily
 * until {@code LazySplitsPointer#materialize()} method is called.
 */
abstract class LazySplitsPointer extends AbstractSplitsPointer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LazySplitsPointer.class);

  private final NamespaceService namespaceService;
  private final int totalSplitCount;

  private List<DatasetSplit> materializedSplits;
  private volatile boolean splitsMaterialized;
  private volatile Integer splitCount;


  protected LazySplitsPointer(NamespaceService namespaceService, int totalSplitCount) {
    this.namespaceService = namespaceService;
    this.totalSplitCount = totalSplitCount;
  }

  protected NamespaceService getNamespaceService() {
    return namespaceService;
  }

  @Override
  public void materialize(){
    if(!splitsMaterialized){
      Stopwatch stopwatch = Stopwatch.createStarted();
      materializedSplits = ImmutableList.copyOf(getSplitIterable());
      splitsMaterialized = true;
      stopwatch.stop();
      logger.debug("materializing {} splits took {} ms", materializedSplits.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  protected abstract SearchQuery getPartitionQuery(SearchQuery partitionFilterQuery);

  @Override
  public SplitsPointer prune(SearchQuery partitionFilterQuery) {
    if (splitsMaterialized || partitionFilterQuery == null) {
      return this;
    }

    final SearchQuery query = getPartitionQuery(partitionFilterQuery);
    final int lastSplits = getSplitsCount();
    SplitsPointer newSplits = new FilteredSplitsPointer(namespaceService, query, totalSplitCount);

    if (newSplits.getSplitsCount() < lastSplits) {
      return newSplits;
    }
    return this;
  }

  protected abstract Iterable<Map.Entry<DatasetSplitId, DatasetSplit>> findSplits();

  @Override
  public Iterable<DatasetSplit> getSplitIterable() {
    if (splitsMaterialized) {
      return materializedSplits;
    }

    return Iterables.transform(findSplits(), SPLIT_VALUES);
  }

  protected abstract int computeSplitsCount();

  @Override
  public int getSplitsCount() {
    if (splitsMaterialized) {
      return materializedSplits.size();
    }
    if(splitCount == null){
      splitCount = computeSplitsCount();
    }
    return splitCount;
  }

  @Override
  public int getTotalSplitsCount() {
    return totalSplitCount;
  }
}
