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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.common.collections.Tuple;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.Path;

/**
 * Provides input stream(s) for reading a parquet file
 */
public interface InputStreamProvider extends AutoCloseable {
  /**
   * Obtains a stream for the given column.
   * Please note, the provider is allowed to reuse a single stream for all columns, if and only if the
   * {@link #isSingleStream()} method below returns true
   */
  BulkInputStream getStream(ColumnChunkMetaData column) throws IOException;

  /**
   * Returns the path corresponding to this stream
   * @return
   */
  Path getStreamPath();

  /**
   * Reads the footer -- or returns the cached one
   */
  MutableParquetMetadata getFooter() throws IOException;

  /**
   * Is this provider reusing the same stream for all columns
   */
  boolean isSingleStream();

  /**
   * Returns the AsyncByteReader associated with this object
   */
  default AsyncByteReader getAsyncByteReader() {
    return null;
  }

  /**
   * Obtains the boosted input stream for the given column.
   * @param column Given column
   * @return The boosted input stream + Size of the InputStream.
   * @throws IOException
   */
  default Tuple<FSInputStream, Long> getBoostedStream(ColumnChunkMetaData column) throws IOException { return null; }


  /**
   * getOffsetIndexProvider.
   */

  OffsetIndexProvider getOffsetIndexProvider(List<ColumnChunkMetaData> columns);

  /**
   * getColumnIndexProvider.
   */

  ColumnIndexProvider getColumnIndexProvider(List<ColumnChunkMetaData> columns);

  /**
   * Enable reading with column index
   * @param selectedColumns
   * @throws IOException
   */
  void enableColumnIndices(List<ColumnChunkMetaData> selectedColumns) throws IOException;

}
