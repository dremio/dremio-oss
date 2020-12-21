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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.common.AutoCloseables;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * An InputStreamProvider that opens a separate stream for each column.
 */

public class StreamPerColumnProvider implements InputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private final long length;
  private MutableParquetMetadata footer;
  private final long maxFooterLen;
  private boolean readColumnOffsetIndexes;
  private final BufferAllocator allocator;
  private final OperatorContext context;

  private final List<BulkInputStream> streams = new ArrayList<>();

  public StreamPerColumnProvider(FileSystem fs, Path path, long length, long maxFooterLen, MutableParquetMetadata footer, OperatorContext context, boolean readColumnOffsetIndexes) {
    this.fs = fs;
    this.path = path;
    this.length = length;
    this.maxFooterLen = maxFooterLen;
    this.footer = footer;
    this.readColumnOffsetIndexes = readColumnOffsetIndexes;
    if (context != null) {
      this.allocator = context.getAllocator();
    } else {
      this.allocator = null;
    }
    this.context = context;
  }

  @Override
  public Path getStreamPath() {
    return path;
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    FSInputStream is = fs.open(path);
    BulkInputStream stream = BulkInputStream.wrap(Streams.wrap(is));
    streams.add(stream);
    return stream;
  }

  @Override
  public boolean isSingleStream() {
    return false;
  }

  @Override
  public void enableColumnIndices(List<ColumnChunkMetaData> selectedColumns) throws IOException {
    this.readColumnOffsetIndexes = true;
  }

  @Override
  public OffsetIndexProvider getOffsetIndexProvider(List<ColumnChunkMetaData> columns) {
    if (readColumnOffsetIndexes) {
      if ((columns.size() == 0) || (columns.get(0).getOffsetIndexReference() == null)) {
          return null;
      }
      OffsetIndexProvider offsetIndexProvider;
      Preconditions.checkState(allocator != null, "Allocator null when trying to getOffsetIndexProvider");
      try (BulkInputStream inputStream = BulkInputStream.wrap(Streams.wrap(fs.open(path)))) {
        offsetIndexProvider = new OffsetIndexProvider(inputStream, allocator, columns);
        if ((context != null) && (context.getStats() != null)) {
          context.getStats().addLongStat(com.dremio.sabot.op.scan.ScanOperator.Metric.OFFSET_INDEX_READ, 1);
        }
        return offsetIndexProvider;
      } catch (IOException ex) {
        //Ignore IOException.
      }
    }
    return null;
  }

  @Override
  public ColumnIndexProvider getColumnIndexProvider(List<ColumnChunkMetaData> columns) {
    if (readColumnOffsetIndexes) {
      if ((columns.size() == 0) || (columns.get(0).getColumnIndexReference() == null)) {
        return null;
      }
      Preconditions.checkState(allocator != null, "Allocator null when trying to getColumnIndexProvider");
      try (BulkInputStream inputStream = BulkInputStream.wrap(Streams.wrap(fs.open(path)))) {
        ColumnIndexProvider columnIndexProvider;
        columnIndexProvider = new ColumnIndexProvider(inputStream, allocator, columns);
        if ((context != null) && (context.getStats() != null)) {
          context.getStats().addLongStat(com.dremio.sabot.op.scan.ScanOperator.Metric.COLUMN_INDEX_READ, 1);
        }
        return columnIndexProvider;
      } catch (IOException ex) {
        //Ignore IOException.
      }
    }
    return null;
  }

  @Override
  public MutableParquetMetadata getFooter() throws IOException {
    if(footer == null) {
      SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();
      footer = new MutableParquetMetadata(footerCache.getFooter(getStream(null), path.toString(), length, fs, maxFooterLen));
    }
    return footer;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(streams);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
