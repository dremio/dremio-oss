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
package com.dremio.exec.store.parquet.columnreaders;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.SeekableInputStream;

import com.dremio.common.exceptions.ExecutionSetupException;

/**
 * Single stream page reader that saves position in input stream between each read call
 */
class DeprecatedSingleStreamPageReader extends PageReader {
  private final SeekableInputStream inputStream;
  private long lastPosition;

  DeprecatedSingleStreamPageReader(ColumnReader<?> parentStatus, SeekableInputStream inputStream, Path path, ColumnChunkMetaData columnChunkMetaData) throws ExecutionSetupException {
    super(parentStatus, inputStream, path, columnChunkMetaData);
    try {
      lastPosition = inputStream.getPos();
    } catch (IOException e) {
      throw new ExecutionSetupException("Error in getting current position for parquet file at location: " + path, e);
    }
    this.inputStream = inputStream;
  }

  @Override
  public boolean next() throws IOException {
    inputStream.seek(lastPosition);
    final boolean res = super.next();
    lastPosition = inputStream.getPos();
    return res;
  }

}
