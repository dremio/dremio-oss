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

import java.util.Set;

import org.apache.parquet.Preconditions;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.io.file.Path;

/**
 * Reader creator which creates {@link EmptyRecordReader}
 */
public class EmptySplitReaderCreator extends SplitReaderCreator {

  public EmptySplitReaderCreator(Path path, InputStreamProvider inputStreamProvider) {
    super(inputStreamProvider);
    this.path = path;
  }

  @Override
  public void createInputStreamProvider(InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {
    Preconditions.checkNotNull(inputStreamProvider, "InputStreamProvider is not initialized");
  }

  @Override
  public void addRowGroupsToRead(Set<Integer> rowGroupsToRead) {
  }

  @Override
  public MutableParquetMetadata getFooter() {
    return null;
  }

  @Override
  public RecordReader createRecordReader(MutableParquetMetadata footer) {
    return new EmptyRecordReader();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(inputStreamProvider);
  }
}
