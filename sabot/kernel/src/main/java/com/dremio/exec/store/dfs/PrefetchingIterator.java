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

import java.util.List;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.SplitReaderCreatorIterator;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;

/**
 * A split, separates initialization of the input reader from actually constructing the reader to allow prefetching.
 */
public class PrefetchingIterator implements RecordReaderIterator {
  private static final Logger logger = LoggerFactory.getLogger(PrefetchingIterator.class);

  private final SplitReaderCreatorIterator creators;
  private InputStreamProvider inputStreamProvider;
  private MutableParquetMetadata footer;
  private SplitReaderCreator current;

  public PrefetchingIterator(SplitReaderCreatorIterator creatorIterator) {
    this.creators = creatorIterator;
  }

  @Override
  public boolean hasNext() {
    return creators.hasNext();
  }

  @Override
  public RecordReader next() {
    try {
      Preconditions.checkArgument(hasNext());
      if (current != null) {
        current.clearLocalFields();
      }
      current = creators.next();
      current.createInputStreamProvider(inputStreamProvider, footer);
      this.inputStreamProvider = current.getInputStreamProvider();
      this.footer = current.getFooter();
      return current.createRecordReader(this.footer);
    } catch (Exception ex) {
      if (current != null) {
        try {
          AutoCloseables.close(current);
        } catch (Exception ignoredEx) {
          // ignore the exception due to close of current record reader
          // since we are going to throw the source exception anyway
        }
      }
      throw ex;
    }
  }

  @Override
  public ParquetProtobuf.ParquetDatasetSplitScanXAttr getCurrentSplitXAttr() {
    return current.getSplitXAttr();
  }


  @Override
  public SplitAndPartitionInfo getCurrentSplitAndPartitionInfo() {
    return current.getSplit();
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    creators.addRuntimeFilter(runtimeFilter);
  }

  @Override
  public List<RuntimeFilter> getRuntimeFilters() {
    return creators.getRuntimeFilters();
  }

  @Override
  public void produceFromBuffered(boolean toProduce) {
    creators.produceFromBufferedSplits(toProduce);
  }

  @Override
  public void close() throws Exception {
    // this is for cleanup if we prematurely exit.
    AutoCloseables.close(creators);
  }
}
