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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.ByteBufferPool;

import com.google.common.base.Preconditions;

/**
 * Copy of hadoop's org.apache.hadoop.fs.ByteBufferUtil, with a bugfix for HADOOP-15911
 */
public final class ByteBufferUtil {
  static final int MAX_BUFFERED_READ = 256 * 1024;
  private static final ThreadLocal<byte[]> heapBuffer = ThreadLocal.withInitial(() -> new byte[MAX_BUFFERED_READ]);

  /**
   * Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
   */
  private static boolean streamHasByteBufferRead(InputStream stream) {
    if (!(stream instanceof ByteBufferReadable)) {
      return false;
    }
    if (!(stream instanceof FSDataInputStream)) {
      return true;
    }
    return ((FSDataInputStream)stream).getWrappedStream()
      instanceof ByteBufferReadable;
  }

  /**
   * Perform a fallback read.
   */
  public static ByteBuffer fallbackRead(
    InputStream stream, ByteBufferPool bufferPool, int maxLength)
    throws IOException {
    if (bufferPool == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
        "were not available, and you did not provide a fallback " +
        "ByteBufferPool.");
    }
    final boolean useDirect = streamHasByteBufferRead(stream);
    ByteBuffer buffer = bufferPool.getBuffer(true, maxLength);
    if (buffer == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
        "were not available, and the ByteBufferPool did not provide " +
        "us with a direct buffer.");
    }
    Preconditions.checkState(buffer.capacity() > 0);
    Preconditions.checkState(buffer.isDirect() == true);
    maxLength = Math.min(maxLength, buffer.capacity());
    boolean success = false;
    try {
      if (useDirect) {
        buffer.clear();
        buffer.limit(maxLength);
        ByteBufferReadable readable = (ByteBufferReadable)stream;
        int totalRead = 0;
        while (true) {
          if (totalRead >= maxLength) {
            success = true;
            break;
          }
          int nRead = readable.read(buffer);
          if (nRead < 0) {
            if (totalRead > 0) {
              success = true;
            }
            break;
          }
          totalRead += nRead;
        }
        buffer.flip();
      } else {
        buffer.clear();
        byte[] heapReadBuffer = heapBuffer.get();
        int totalRead = 0;
        while (totalRead < maxLength) {
          final int sizeToRead = Math.min(maxLength - totalRead, MAX_BUFFERED_READ);
          final int nRead = stream.read(heapReadBuffer, 0, sizeToRead);
          if (nRead < 0) {
            break;
          }
          buffer.put(heapReadBuffer, 0, nRead);
          totalRead += nRead;
        }
        if (totalRead >= 0) {
          buffer.limit(totalRead);
          buffer.flip();
          success = true;
        }
      }
    } finally {
      if (!success) {
        // If we got an error while reading, or if we are at EOF, we
        // don't need the buffer any more.  We can give it back to the
        // bufferPool.
        bufferPool.putBuffer(buffer);
        buffer = null;
      }
    }
    return buffer;
  }
}
