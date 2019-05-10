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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;

import com.dremio.common.AutoCloseables;

/**
 * An InputStreamProvider that opens a separate stream for each column.
 */
public class StreamPerColumnProvider implements InputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private final long length;
  private ParquetMetadata footer;

  private final List<BulkInputStream> streams = new ArrayList<>();

  public StreamPerColumnProvider(FileSystem fs, Path path, long length) {
    this.fs = fs;
    this.path = path;
    this.length = length;
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    BulkInputStream stream = BulkInputStream.wrap(HadoopStreams.wrap(fs.open(path)));
    streams.add(stream);
    return stream;
  }

  @Override
  public boolean isSingleStream() {
    return false;
  }

  @Override
  public ParquetMetadata getFooter() throws IOException {
    if(footer == null) {
      SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();
      footer = footerCache.getFooter(getStream(null), path.toString(), length, fs);
    }
    return footer;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(streams);
    } catch (Exception ex) {
      if(ex instanceof IOException) {
        throw (IOException) ex;
      }
      throw new IOException(ex);
    }
  }

}
