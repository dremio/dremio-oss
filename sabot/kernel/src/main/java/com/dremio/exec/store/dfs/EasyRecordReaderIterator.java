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

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.SplitReaderCreatorIterator;
import com.google.common.base.Preconditions;

/**
 * A split, separates initialization of the input reader from actually constructing the reader.
 */
public class EasyRecordReaderIterator implements RecordReaderIterator {

  private final SplitReaderCreatorIterator creators;
  private SplitReaderCreator current;

  public EasyRecordReaderIterator(SplitReaderCreatorIterator creatorIterator) {
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
      return current.createRecordReader(null);
    } catch (Exception ex) {
      if (current != null) {
          AutoCloseables.close(RuntimeException.class, current);
      }
      throw ex;
    }
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
