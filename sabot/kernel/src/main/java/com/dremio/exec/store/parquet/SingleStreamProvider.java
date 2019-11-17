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
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.SeekableInputStream;

import com.dremio.io.ArrowBufFSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

import io.netty.buffer.ArrowBuf;

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
  private ParquetMetadata footer;

  public SingleStreamProvider(FileSystem fs, Path path, long fileLength, long maxFooterLen, boolean readFullFile, OperatorContext context) {
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
    try {
      AutoCloseables.close(stream);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
