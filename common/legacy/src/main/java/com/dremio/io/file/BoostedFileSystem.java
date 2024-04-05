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

import com.dremio.common.collections.Tuple;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;

/**
 * This interface used to boost access to Parquet files
 *
 * <p>For Creating a Boosted Stream/File, the algorithm to follow: 1. createBoostFile: This returns
 * a FSOutputStream. 2. write data using the FSOutputStream. 3. on completion or failure, close the
 * FSOutputStream. 4. if successfully written : commit using commitBoostFile(//args same as used for
 * createBoostFile). else on failure: abort using abortBoostFile(//args same as used for
 * createBoostFile).
 *
 * <p>For Reading from a Boosted File do: 1. getBoostFile(): It returns either a null or a
 * FSInputStream. If FSInputStream is received use it to do the reads.
 */
public interface BoostedFileSystem {
  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> open a Stream to write
   * CachedFile.
   */
  FSOutputStream createBoostFile(AsyncByteReader.FileKey fileKey, long offset, String columnName)
      throws IOException;

  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> open a Stream to read
   * CachedFile.
   *
   * @return
   */
  Tuple<FSInputStream, Long> getBoostFile(
      AsyncByteReader.FileKey fileKey,
      long offset,
      String columnName,
      List<AsyncByteReader.ReaderStat> stats,
      BufferAllocator allocator)
      throws IOException;

  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> commit any inFlight
   * instances.
   */
  void commitBoostFile(AsyncByteReader.FileKey fileKey, long offset, String columnName)
      throws IOException;

  /**
   * For a given tuple <ParquetFilePath, RowGroupID, ColumnName, Version> abort any inFlight
   * instances.
   */
  void abortBoostFile(AsyncByteReader.FileKey fileKey, long offset, String columnName)
      throws IOException;
}
