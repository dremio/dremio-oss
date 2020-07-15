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
package com.dremio.io.file;

import com.dremio.io.AsyncByteReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This interface used to boost access to Parquet files
 */
public interface BoostedFileSystem {
  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> open a Stream to write CachedFile.
   */
  OutputStream createBoostFile(AsyncByteReader.FileKey fileKey, long offset, String columnName) throws IOException;

  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> open a Stream to read CachedFile.
   */
  InputStream getBoostFile(AsyncByteReader.FileKey fileKey, long offset, String columnName) throws IOException;
}
