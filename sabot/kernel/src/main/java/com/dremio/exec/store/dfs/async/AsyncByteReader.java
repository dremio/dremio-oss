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
package com.dremio.exec.store.dfs.async;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A simplified asynchronous data reading interface.
 */
public interface AsyncByteReader {
  /**
   * Read data into the provided dst buffer. Attempts to do so offheap.
   * @param offset The offset in the underlying data.
   * @param dst The ArrowBuf to read into
   * @param dstOffset The offset to read into.
   * @param len The amount of bytes to read.
   * @return A CompletableFuture that will be informed when the read is completed.
   */
  CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len);

  /**
   * Read data and return as a byte array.
   * @param offset File offset to read from
   * @param len Number of bytes to read
   * @return A CompletableFuture that will be carry the byte[] result when the read is completed
   */
  default CompletableFuture<byte[]> readFully(long offset, int len) {
    final ByteBuf buf = Unpooled.directBuffer(len);
    CompletableFuture<Void> innerFuture = readFully(offset, buf, 0, len);
    return innerFuture.thenApply((v) -> {
      byte[] bytes = new byte[len];
      buf.getBytes(0, bytes, 0, len);
      return bytes;
    }).whenComplete((a,b) -> buf.release());
  }

  /**
   * An addon interface for FileSystems that support an async reader stream.
   */
  interface MayProvideAsyncStream {

    /**
     * Whether this FileSystem may support async reads.
     * @return true if async reads are supported for the given file.
     */
    boolean supportsAsync();

    /**
     * For a given path, get an AsyncByteReader.
     * @param path Path to read
     * @return Reader
     */
    AsyncByteReader getAsyncByteReader(Path path) throws IOException;
  }
}
