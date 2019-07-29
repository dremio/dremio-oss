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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * An InputStreamProvider that opens a separate stream for each column.
 */
public class StreamPerColumnProvider extends FSDataStreamInputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private final long length;
  private ParquetMetadata footer;
  private final long maxFooterLen;

  private final List<FSDataInputStream> inputStreams = new ArrayList<>();
  private final List<BulkInputStream> streams = new ArrayList<>();

  public StreamPerColumnProvider(FileSystem fs, Path path, long length, long maxFooterLen, OperatorStats stats) {
    super(stats);
    this.fs = fs;
    this.path = path;
    this.length = length;
    this.maxFooterLen = maxFooterLen;
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    FSDataInputStream is = fs.open(path);
    BulkInputStream stream = BulkInputStream.wrap(HadoopStreams.wrap(is));
    inputStreams.add(is);
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
      footer = footerCache.getFooter(getStream(null), path.toString(), length, fs, maxFooterLen);
    }
    return footer;
  }

  @Override
  public void close() throws IOException {
    populateStats(inputStreams);

    try {
      AutoCloseables.close(streams);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
