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
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A split, separates initialization of the input reader from actually constructing the reader to allow prefetching.
 */
public class PrefetchingIterator<T extends SplitReaderCreator> implements RecordReaderIterator {
  private static final Logger logger = LoggerFactory.getLogger(PrefetchingIterator.class);

  private int location = -1;
  private int nextLocation = 0;
  private final List<T> creators;
  private Path path;
  private MutableParquetMetadata footer;
  private final OperatorContext context;
  private final CompositeReaderConfig readerConfig;
  private List<RuntimeFilterEvaluator> runtimeFilterEvaluators;

  public PrefetchingIterator(OperatorContext context, CompositeReaderConfig readerConfig, List<T> creators) {
    this.creators = creators;
    this.context = context;
    this.readerConfig = readerConfig;
    this.runtimeFilterEvaluators = new ArrayList<>();
  }

  @Override
  public boolean hasNext() {
    return nextLocation < creators.size();
  }

  @Override
  public RecordReader next() {
    Preconditions.checkArgument(hasNext());
    location = nextLocation;
    setNextLocation(location + 1);
    final SplitReaderCreator current = creators.get(location);
    current.createInputStreamProvider(path, footer);
    this.path = current.getPath();
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

  private void setNextLocation(int baseNext) {
    this.nextLocation = baseNext;
    if (runtimeFilterEvaluators.isEmpty() || !hasNext()) {
      return;
    }
    boolean skipPartition;
    do {
      skipPartition = false;
      final SplitAndPartitionInfo split = creators.get(nextLocation).getSplit();
      if (split == null) {
        return;
      }

      final List<NameValuePair<?>> nameValuePairs = this.readerConfig.getPartitionNVPairs(this.context.getAllocator(), split);
      try {
        for (RuntimeFilterEvaluator runtimeFilterEvaluator : runtimeFilterEvaluators) {
          if (runtimeFilterEvaluator.canBeSkipped(split, nameValuePairs)) {
            skipPartition = true;
            this.nextLocation++;
            break;
          }
        }
      } finally {
        AutoCloseables.close(RuntimeException.class, nameValuePairs);
      }
    } while (skipPartition && hasNext());

    // Reset pre-fetching next
    if (location >= 0 && nextLocation > location + 1) {
      final SplitReaderCreator nextReaderCreator = nextLocation < creators.size() ? creators.get(nextLocation) : null;
      creators.get(location).setNext(nextReaderCreator);
    }
  }

  @Override
  public void close() throws Exception {
    // this is for cleanup if we prematurely exit.
    AutoCloseables.close(creators);
  }
}
