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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;

/**
 * An InputStreamProvider that uses a single stream.
 * It is OK to reuse this stream for reading several columns, in which case the user must handle the repositioning of the stream
 */
public class SingleStreamProvider implements InputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private BulkInputStream stream;
  private final long fileLength;
  private ParquetMetadata footer;

  public SingleStreamProvider(FileSystem fs, Path path, long fileLength) {
    this.fs = fs;
    this.path = path;
    this.fileLength = fileLength;
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    if(stream == null) {
      stream = BulkInputStream.wrap(HadoopStreams.wrap(fs.open(path)));
    }
    return stream;
  }

  @Override
  public ParquetMetadata getFooter() throws IOException {
    if(footer == null) {
      SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();
      footer = footerCache.getFooter(getStream(null), path.toString(), fileLength, fs);
    }
    return footer;
  }

  @Override
  public boolean isSingleStream() {
    return true;
  }

  @Override
  public void close() throws IOException {
    if(stream != null) {
      stream.close();
    }
  }
}
