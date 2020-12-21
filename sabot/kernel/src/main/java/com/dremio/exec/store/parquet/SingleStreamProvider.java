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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.SeekableInputStream;

import com.dremio.io.ArrowBufFSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * An InputStreamProvider that uses a single stream.
 * It is OK to reuse this stream for reading several columns, in which case the user must handle the repositioning of the stream
 */
public class SingleStreamProvider implements InputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private final BufferAllocator allocator;
  private final long fileLength;
  private final long maxFooterLen;
  private final boolean readFullFile;
  private BulkInputStream stream;
  private OperatorContext context;

  private MutableParquetMetadata footer;
  private boolean readColumnOffsetIndices;

  public SingleStreamProvider(FileSystem fs, Path path, long fileLength, long maxFooterLen, boolean readFullFile, MutableParquetMetadata footer, OperatorContext context, boolean readColumnOffsetIndices) {
    this.fs = fs;
    this.path = path;
    this.fileLength = fileLength;
    this.maxFooterLen = maxFooterLen;
    this.readFullFile = readFullFile;
    this.footer = footer;
    this.context = context;
    if (context != null) {
      this.allocator = context.getAllocator();
    } else {
      this.allocator = null;
    }
    this.readColumnOffsetIndices = readColumnOffsetIndices;
  }

  @Override
  public Path getStreamPath() {
    return path;
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    if(stream == null) {
      stream = initStream();
    }
    return stream;
  }

  private BulkInputStream initStream() throws IOException {
    if (!readFullFile) {
      return BulkInputStream.wrap(Streams.wrap(fs.open(path)));
    }

    try (SeekableInputStream is = Streams.wrap(fs.open(path))) {
      int len = (int) fileLength;
      ArrowBuf buf = allocator.buffer(len);
      if (buf == null) {
        throw new OutOfMemoryException("Unable to allocate memory for reading parquet file");
      }
      try {
        ByteBuffer buffer = buf.nioBuffer(0, len);
        is.readFully(buffer);
        buf.writerIndex(len);
        return BulkInputStream.wrap(Streams.wrap(new ArrowBufFSInputStream(buf)));
      } catch (Throwable t) {
        buf.close();
        throw t;
      }
    }
  }

  @Override
  public void enableColumnIndices(List<ColumnChunkMetaData> selectedColumns) throws IOException {
    this.readColumnOffsetIndices = true;
  }

  @Override
  public OffsetIndexProvider getOffsetIndexProvider(List<ColumnChunkMetaData> columns) {
    if (readColumnOffsetIndices) {
      if ((columns.size() == 0) || (columns.get(0).getOffsetIndexReference() == null)) {
        return null;
      }
      try (BulkInputStream inputStream = BulkInputStream.wrap(Streams.wrap(fs.open(path)))) {
        OffsetIndexProvider offsetIndexProvider;
        offsetIndexProvider = new OffsetIndexProvider(inputStream, allocator, columns);
        if ((context != null) && (context.getStats() != null)) {
          context.getStats().addLongStat(com.dremio.sabot.op.scan.ScanOperator.Metric.OFFSET_INDEX_READ, 1);
        }
        return offsetIndexProvider;
      } catch (IOException ex) {
        //Ignore error and return null;
      }
    }
    return null;
  }

  @Override
  public ColumnIndexProvider getColumnIndexProvider(List<ColumnChunkMetaData> columns) {
    if (readColumnOffsetIndices) {
      if ((columns.size() == 0) || (columns.get(0).getColumnIndexReference() == null)) {
        return null;
      }
      try (BulkInputStream inputStream = BulkInputStream.wrap(Streams.wrap(fs.open(path)))) {
        ColumnIndexProvider columnIndexProvider;
        columnIndexProvider = new ColumnIndexProvider(inputStream, allocator, columns);
        if ((context != null) && (context.getStats() != null)) {
          context.getStats().addLongStat(com.dremio.sabot.op.scan.ScanOperator.Metric.COLUMN_INDEX_READ, 1);
        }
        return columnIndexProvider;
      } catch (IOException ex) {
        //Ignore error and return null;
      }
    }
    return null;
  }

  @Override
  public MutableParquetMetadata getFooter() throws IOException {
    if(footer == null) {
      SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();
      footer = new MutableParquetMetadata(footerCache.getFooter(getStream(null), path.toString(), fileLength, fs, maxFooterLen));
    }
    return footer;
  }

  @Override
  public boolean isSingleStream() {
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(stream);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
