/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

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
   * Reads the footer -- or returns the cached one
   */
  ParquetMetadata getFooter() throws IOException;

  /**
   * Is this provider reusing the same stream for all columns
   */
  boolean isSingleStream();
}
