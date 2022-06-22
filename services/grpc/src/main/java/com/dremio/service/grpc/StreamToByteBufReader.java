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
package com.dremio.service.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.SuppressForbidden;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import io.grpc.internal.ReadableBuffer;

/**
 * Enable access to ReadableBuffer directly to copy data from a GRPC InputStream into
 * arrow buffers.
 */
@SuppressForbidden
public class StreamToByteBufReader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.
          getLogger(StreamToByteBufReader.class);
  private static AtomicInteger NON_OPTIMAL_READ = new AtomicInteger(0);
  private static final Field READABLE_BUFFER;
  private static final Class<?> BUFFER_INPUT_STREAM;

  static {
    Field tmpField = null;
    Class<?> tmpClazz = null;
    try {
      Class<?> clazz = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");

      Field f = clazz.getDeclaredField("buffer");
      f.setAccessible(true);
      // don't set until we've gotten past all exception cases.
      tmpField = f;
      tmpClazz = clazz;
    } catch (Exception e) {
      LOGGER.warn("Unable to setup optimal read path.", e);
    }
    READABLE_BUFFER = tmpField;
    BUFFER_INPUT_STREAM = tmpClazz;
  }

  /**
   * Extracts the ReadableBuffer for the given input stream.
   *
   * @param is Must be an instance of io.grpc.internal.ReadableBuffers$BufferInputStream or
   *     null will be returned.
   */
  public static ReadableBuffer getReadableBuffer(InputStream is) {

    if (BUFFER_INPUT_STREAM == null || !is.getClass().equals(BUFFER_INPUT_STREAM)) {
      return null;
    }

    try {
      return (ReadableBuffer) READABLE_BUFFER.get(is);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Helper method to read a gRPC-provided InputStream into an Direct Buffer.
   * @param stream The stream to read from. Should be an instance of {@link #BUFFER_INPUT_STREAM}.
   * @param buf The buffer to read into.
   * @param size The number of bytes to read.
   * @throws IOException if there is an error reading form the stream
   */
  public static void readIntoBuffer(final InputStream stream, final ArrowBuf buf, final int size)
          throws IOException {

    ReadableBuffer readableBuffer = getReadableBuffer(stream);
    if (readableBuffer != null) {
      readableBuffer.readBytes(buf.nioBuffer(0, size));
    } else {
      LOGGER.warn("Entered non optimal read path {} number of times", NON_OPTIMAL_READ.incrementAndGet());
      byte[] heapBytes = new byte[size];
      ByteStreams.readFully(stream, heapBytes);
      buf.writeBytes(heapBytes);
    }
    buf.writerIndex(size);
  }
}
