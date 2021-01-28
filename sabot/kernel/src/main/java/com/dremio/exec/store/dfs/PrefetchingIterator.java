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
package com.dremio.exec.store.dfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.RuntimeFilterEvaluator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A split, separates initialization of the input reader from actually constructing the reader to allow prefetching.
 */
public class PrefetchingIterator<T extends SplitReaderCreator> implements RecordReaderIterator {
  private static final Logger logger = LoggerFactory.getLogger(PrefetchingIterator.class);

  // The next split to create. This will change as intermediate splits are pruned due to
  // runtime partition pruning
  private SplitReaderCreator nextSplit;
  // Number of splits to prefetch
  private final int numSplitsToPrefetch;
  // All the unused, but initialised creators are closed as part of close on the PrefetchingIterator
  private final List<T> creators;
  private InputStreamProvider inputStreamProvider;
  private MutableParquetMetadata footer;
  private final OperatorContext context;
  private final CompositeReaderConfig readerConfig;
  private List<RuntimeFilterEvaluator> runtimeFilterEvaluators;

  public PrefetchingIterator(OperatorContext context, CompositeReaderConfig readerConfig, List<T> creators, int numSplitsToPrefetch) {
    this.creators = creators;
    this.nextSplit = creators.size() > 0 ? creators.get(0) : null;
    this.context = context;
    this.readerConfig = readerConfig;
    this.runtimeFilterEvaluators = new ArrayList<>();
    this.numSplitsToPrefetch = numSplitsToPrefetch;
  }

  @Override
  public boolean hasNext() {
    return nextSplit != null;
  }

  @Override
  public RecordReader next() {
    Preconditions.checkArgument(hasNext());
    final SplitReaderCreator current = nextSplit;
    applyRuntimeFilters(current, numSplitsToPrefetch);
    nextSplit = current.next;
    current.createInputStreamProvider(inputStreamProvider, footer);
    this.inputStreamProvider = current.getInputStreamProvider();
    this.footer = current.getFooter();
    return current.createRecordReader(this.footer);
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (runtimeFilter.getPartitionColumnFilter() != null) {
      final RuntimeFilterEvaluator filterEvaluator =
              new RuntimeFilterEvaluator(context.getAllocator(), context.getStats(), runtimeFilter);
      this.runtimeFilterEvaluators.add(filterEvaluator);
      logger.debug("Runtime filter added to the iterator [{}]", runtimeFilter);
    }
  }

  private boolean canSkipSplit(final SplitAndPartitionInfo split) {
    final List<NameValuePair<?>> nameValuePairs = this.readerConfig.getPartitionNVPairs(this.context.getAllocator(), split);
    try {
      for (RuntimeFilterEvaluator runtimeFilterEvaluator : runtimeFilterEvaluators) {
        if (runtimeFilterEvaluator.canBeSkipped(split, nameValuePairs)) {
          return true;
        }
      }
    } finally {
      AutoCloseables.close(RuntimeException.class, nameValuePairs);
    }

    return false;
  }

  // Starting from base, returns the next valid split
  // Returns null, if there is no valid split
  private SplitReaderCreator getNextValidSplit(SplitReaderCreator base) {
    Preconditions.checkArgument(base instanceof GetSplitAndPartitionInfo, "Unexpected argument passed to getNextValidSplit");
    if (runtimeFilterEvaluators.isEmpty()) {
      return base.next;
    }

    // there are runtime filters and next
    SplitReaderCreator nextLoc = base.next;
    while (nextLoc != null) {
      final SplitAndPartitionInfo split = ((GetSplitAndPartitionInfo)nextLoc).getSplit();
      if (!canSkipSplit(split)) {
        // this split cannot be skipped
        return nextLoc;
      }

      nextLoc = nextLoc.next;
    }

    return nextLoc;
  }

  private void applyRuntimeFilters(SplitReaderCreator baseSplit, int numSplitsToPrefetch) {
    SplitReaderCreator current = baseSplit;
    if (!(current instanceof GetSplitAndPartitionInfo)) {
      return;
    }

    for(int i = 0; (i < numSplitsToPrefetch) && (current != null); i++) {
      SplitReaderCreator nextSplit = getNextValidSplit(current);
      current.setNext(nextSplit);
      current = nextSplit;
    }
  }

  @Override
  public void close() throws Exception {
    // this is for cleanup if we prematurely exit.
    AutoCloseables.close(creators);
  }
}
