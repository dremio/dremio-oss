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
package org.apache.arrow.flight.grpc;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Compatibility patch for Arrow Flight's gRPC fast path.
 *
 * <p>Arrow Flight 18.1.1 may try to call ReadableBuffer.readBytes(ByteBuffer), but the gRPC core
 * bundled in this runtime does not expose that method. When that happens we fall back to the safe
 * heap-buffer copy path instead of throwing NoSuchMethodError while reading query results.
 */
public final class GetReadableBuffer {
  private static final Field READABLE_BUFFER;
  private static final Class<?> BUFFER_INPUT_STREAM;
  private static final Method READ_BYTES_METHOD;

  static {
    Field readableBufferField = null;
    Class<?> bufferInputStreamClass = null;
    Method readBytesMethod = null;
    try {
      Class<?> inputStreamClass = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");
      Field bufferField = inputStreamClass.getDeclaredField("buffer");
      bufferField.setAccessible(true);

      Class<?> readableBufferClass = Class.forName("io.grpc.internal.ReadableBuffer");
      readBytesMethod = readableBufferClass.getDeclaredMethod("readBytes", ByteBuffer.class);

      readableBufferField = bufferField;
      bufferInputStreamClass = inputStreamClass;
    } catch (Exception e) {
      readableBufferField = null;
      bufferInputStreamClass = null;
      readBytesMethod = null;
    }

    READABLE_BUFFER = readableBufferField;
    BUFFER_INPUT_STREAM = bufferInputStreamClass;
    READ_BYTES_METHOD = readBytesMethod;
  }

  private GetReadableBuffer() {}

  public static io.grpc.internal.ReadableBuffer getReadableBuffer(InputStream inputStream) {
    if (BUFFER_INPUT_STREAM == null || !inputStream.getClass().equals(BUFFER_INPUT_STREAM)) {
      return null;
    }

    try {
      return (io.grpc.internal.ReadableBuffer) READABLE_BUFFER.get(inputStream);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static void readIntoBuffer(
      InputStream stream, ArrowBuf buffer, int size, boolean tryZeroCopy) throws IOException {
    io.grpc.internal.ReadableBuffer readableBuffer =
        tryZeroCopy ? getReadableBuffer(stream) : null;

    if (readableBuffer != null && READ_BYTES_METHOD != null) {
      try {
        READ_BYTES_METHOD.invoke(readableBuffer, buffer.nioBuffer(0, size));
        buffer.writerIndex(size);
        return;
      } catch (ReflectiveOperationException | RuntimeException e) {
        // Fall through to the compatible heap-copy path.
      }
    }

    byte[] heapBytes = new byte[size];
    ByteStreams.readFully(stream, heapBytes);
    buffer.writeBytes(heapBytes);
    buffer.writerIndex(size);
  }
}
