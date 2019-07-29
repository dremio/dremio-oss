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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;

import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/**
 * An InputStreamProvider that uses a single stream.
 * It is OK to reuse this stream for reading several columns, in which case the user must handle the repositioning of the stream
 */
public class SingleStreamProvider extends FSDataStreamInputStreamProvider {
  private final FileSystem fs;
  private final Path path;
  private final BufferAllocator allocator;
  private final long fileLength;
  private final long maxFooterLen;
  private final boolean readFullFile;
  private BulkInputStream stream;
  private ParquetMetadata footer;
  private FSDataInputStream fsDataInputStream;

  public SingleStreamProvider(FileSystem fs, Path path, long fileLength, long maxFooterLen, boolean readFullFile, OperatorContext context) {
    super(context == null ? null : context.getStats());
    this.fs = fs;
    this.path = path;
    this.fileLength = fileLength;
    this.maxFooterLen = maxFooterLen;
    this.readFullFile = readFullFile;
    if (context != null) {
      this.allocator = context.getAllocator();
    } else {
      this.allocator = null;
    }
  }

  @Override
  public BulkInputStream getStream(ColumnChunkMetaData column) throws IOException {
    if(stream == null) {
      this.fsDataInputStream = fs.open(path);
      stream = initStream();
    }
    return stream;
  }

  private BulkInputStream initStream() throws IOException {
    if (!readFullFile) {
      return BulkInputStream.wrap(HadoopStreams.wrap(fsDataInputStream));
    }

    try (SeekableInputStream is = HadoopStreams.wrap(fsDataInputStream)) {
      int len = (int) fileLength;
      ArrowBuf buf = allocator.buffer(len);
      if (buf == null) {
        throw new OutOfMemoryException("Unable to allocate memory for reading parquet file");
      }
      try {
        ByteBuffer buffer = buf.nioBuffer(0, len);
        is.readFully(buffer);
        buf.writerIndex(len);
        if (stats != null) {
          populateStats(Lists.newArrayList(fsDataInputStream));
          stats.addLongStat(ScanOperator.Metric.PRELOADED_BYTES, len);
        }
        return BulkInputStream.wrap(HadoopStreams.wrap(new FSDataInputStream(new ByteBufferFSDataInputStream(buf.asNettyBuffer()))));
      } catch (Throwable t) {
        buf.close();
        throw t;
      } finally {
        this.fsDataInputStream = null;
      }
    }
  }

  @Override
  public ParquetMetadata getFooter() throws IOException {
    if(footer == null) {
      SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();
      footer = footerCache.getFooter(getStream(null), path.toString(), fileLength, fs, maxFooterLen);
    }
    return footer;
  }

  @Override
  public boolean isSingleStream() {
    return true;
  }

  @Override
  public void close() throws IOException {
    if (fsDataInputStream != null) {
      populateStats(Lists.newArrayList(fsDataInputStream));
    }

    try {
      AutoCloseables.close(stream);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
