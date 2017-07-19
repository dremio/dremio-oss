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
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Pointer to a set of splits. May be loaded lazily.
 */
public class SplitsPointerImpl implements SplitsPointer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitsPointerImpl.class);

  private static Function<Entry<DatasetSplitId, DatasetSplit>, DatasetSplit> SPLIT_VALUES = new Function<Entry<DatasetSplitId, DatasetSplit>, DatasetSplit>() {
    @Nullable
    @Override
    public DatasetSplit apply(@Nullable Entry<DatasetSplitId, DatasetSplit> input) {
      return input.getValue();
    }
  };

  private final FindByCondition splitFilter;
  private final NamespaceService namespaceService;
  private final int totalSplitCount;
  private List<DatasetSplit> materializedSplits;
  private volatile boolean splitsMaterialized;
  private volatile Integer splitCount;
  private final boolean usingAllSplits;
  private final FindByRange<DatasetSplitId> defaultRangeFilter;

  SplitsPointerImpl(List<DatasetSplit> splits, int totalSplitCount){
    this.splitFilter = null;
    this.splitsMaterialized = true;
    this.materializedSplits = ImmutableList.copyOf(splits);
    this.namespaceService = null;
    this.totalSplitCount = totalSplitCount;
    this.usingAllSplits = true;
    this.defaultRangeFilter = null;
  }

  private SplitsPointerImpl(
      SearchQuery partitionFilterQuery,
      NamespaceService namespaceService,
      int totalSplitCount,
      boolean usingAllSplits,
      FindByRange<DatasetSplitId> defaultRangeFilter
      ) {
    super();
    this.splitFilter = new FindByCondition().setCondition(partitionFilterQuery);
    this.materializedSplits = null;
    this.namespaceService = namespaceService;
    this.totalSplitCount = totalSplitCount;
    this.usingAllSplits = usingAllSplits;
    this.defaultRangeFilter = defaultRangeFilter;

  }

  public SplitsPointerImpl(
      DatasetConfig datasetConfig,
      NamespaceService namespaceService
      ) {
    this(DatasetSplitId.getSplitsQuery(datasetConfig),
        namespaceService,
        namespaceService.getSplitCount(new FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))),
        true,
        DatasetSplitId.getSplitsRange(datasetConfig)
      );
  }

  public void materialize(){
    if(!splitsMaterialized){
      Stopwatch stopwatch = Stopwatch.createStarted();
      materializedSplits = ImmutableList.copyOf(getSplitIterable());
      splitsMaterialized = true;
      stopwatch.stop();
      logger.debug("materializing {} splits took {} ms" + materializedSplits.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }



  @Override
  public double getSplitRatio() throws NamespaceException{
    return ((double) getSplitsCount())/totalSplitCount;
  }

  @Override
  public SplitsPointer prune(SearchQuery partitionFilterQuery) throws NamespaceException {
    if (!splitsMaterialized && partitionFilterQuery != null) {
      final SearchQuery query = SearchQueryUtils.and(splitFilter.getCondition(), partitionFilterQuery);
      final int lastSplits = getSplitsCount();
      SplitsPointer newSplits = new SplitsPointerImpl(query, namespaceService, totalSplitCount, false, defaultRangeFilter);

      if (newSplits.getSplitsCount() < lastSplits) {
        return newSplits;
      }
    }
    return this;
  }

  @Override
  public Iterable<DatasetSplit> getSplitIterable() {
    if (splitsMaterialized) {
      return materializedSplits;
    }
    if (usingAllSplits) {
      return Iterables.transform(namespaceService.findSplits(defaultRangeFilter), SPLIT_VALUES);
    }
    return Iterables.transform(namespaceService.findSplits(splitFilter), SPLIT_VALUES);
  }

  @Override
  public String computeDigest(){
    return Integer.toString(hashCode());
  }

  @Override
  public int getSplitsCount() throws NamespaceException {
    if (splitsMaterialized) {
      return materializedSplits.size();
    }
    if(splitCount == null){
      splitCount = namespaceService.getSplitCount(splitFilter);
    }
    return splitCount;
  }

  @Override
  public int getTotalSplitsCount() {
    return totalSplitCount;
  }

  public boolean isPruned() throws NamespaceException {
    return totalSplitCount != getSplitsCount();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SplitsPointerImpl)) {
      return false;
    }
    SplitsPointerImpl castOther = (SplitsPointerImpl) other;
    return Objects.equals(splitFilter, castOther.splitFilter)
        && Objects.equals(materializedSplits, castOther.materializedSplits)
        && Objects.equals(totalSplitCount, castOther.totalSplitCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitFilter, materializedSplits, totalSplitCount);
  }




}
