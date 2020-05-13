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

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.exec.store.RecordReader;
import com.dremio.exec.util.CloseableIterator;
import com.dremio.io.file.Path;

/**
 * A split, separates initialization of the input reader from actually constructing the reader to allow prefetching.
 */
public class PrefetchingIterator<T extends SplitReaderCreator> implements CloseableIterator<RecordReader> {

  private int location = -1;
  private final List<T> creators;
  private Path path;
  private ParquetMetadata footer;

  public PrefetchingIterator(List<T> creators) {
    this.creators = creators;
  }

  @Override
  public boolean hasNext() {
    return location < creators.size() - 1;
  }

  @Override
  public RecordReader next() {
    Preconditions.checkArgument(hasNext());
    location++;
    final SplitReaderCreator current = creators.get(location);
    current.createInputStreamProvider(path, footer);
    this.path = current.getPath();
    this.footer = current.getFooter();
    return current.createRecordReader();
  }

  @Override
  public void close() throws Exception {
    // this is for cleanup if we prematurely exit.
    AutoCloseables.close(creators);
  }

}
