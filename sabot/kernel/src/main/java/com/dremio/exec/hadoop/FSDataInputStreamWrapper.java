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
package com.dremio.exec.hadoop;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.io.FSInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSError;

/** Wrapper around FSDataInputStream to capture {@code FSError}. */
@NotThreadSafe
class FSDataInputStreamWrapper extends FSInputStream {
  private static final String BYTE_BUFFER_POSITIONED_READ_METHOD_NAME = "read";
  private static final Method BYTE_BUFFER_POSITIONED_READ_METHOD =
      getByteBufferPositionedReadMethod();

  /** Wrapper when {@code FSDataInputStream#read(ByteBuffer)} is not supported */
  private static final class ByteArrayFSInputStream extends FSDataInputStreamWrapper {
    private final byte[] temp = new byte[8192];

    private ByteArrayFSInputStream(FSDataInputStream in) throws IOException {
      super(in);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (dst.hasArray()) {
        return readHeapBuffer(dst);
      } else {
        return readDirectBuffer(dst);
      }
    }

    private int readHeapBuffer(ByteBuffer dst) throws IOException {
      int result = read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
      if (result < 0) {
        return result;
      }
      dst.position(dst.position() + result);
      return result;
    }

    private int readDirectBuffer(ByteBuffer dst) throws IOException {
      int toRead = Math.min(temp.length, dst.remaining());
      int result = read(temp, 0, toRead);
      if (result < 0) {
        return result;
      }

      dst.put(temp, 0, result);
      return result;
    }
  }

  private final FSDataInputStream underlyingIs;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private FSDataInputStreamWrapper(FSDataInputStream in) throws IOException {
    underlyingIs = in;
  }

  public static FSInputStream of(FSDataInputStream in) throws IOException {
    if (in.getWrappedStream() instanceof ByteBufferReadable) {
      return new FSDataInputStreamWrapper(in);
    }

    return new ByteArrayFSInputStream(in);
  }

  @Override
  public int read() throws IOException {
    try {
      return underlyingIs.read();
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    try {
      return underlyingIs.read(b);
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    try {
      return underlyingIs.read(b, off, len);
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    try {
      return underlyingIs.read(dst);
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public int read(long position, ByteBuffer dst) throws IOException {
    try {
      if (BYTE_BUFFER_POSITIONED_READ_METHOD == null) {
        throw new UnsupportedOperationException("ByteBuffer positioned read not supported");
      }
      return (int) BYTE_BUFFER_POSITIONED_READ_METHOD.invoke(underlyingIs, position, dst);
    } catch (InvocationTargetException e) {
      IOException ioException = ErrorHelper.findWrappedCause(e, IOException.class);
      if (ioException != null) {
        throw ioException;
      } else {
        throw new IOException(e.getCause());
      }
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException("ByteBuffer positioned read not supported");
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public long getPosition() throws IOException {
    try {
      return underlyingIs.getPos();
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public void setPosition(long position) throws IOException {
    try {
      underlyingIs.seek(position);
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try {
      return underlyingIs.skip(n);
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public int available() throws IOException {
    try {
      return underlyingIs.available();
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      underlyingIs.close();
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public void mark(int readlimit) {
    underlyingIs.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    try {
      underlyingIs.reset();
    } catch (FSError e) {
      throw HadoopFileSystem.propagateFSError(e);
    }
  }

  @Override
  public boolean markSupported() {
    return underlyingIs.markSupported();
  }

  /*
   * MapR uses hadoop 2.x distribution, and does not have this method.
   */
  private static Method getByteBufferPositionedReadMethod() {
    Class[] types = {long.class, ByteBuffer.class};
    try {
      return FSDataInputStream.class.getMethod(BYTE_BUFFER_POSITIONED_READ_METHOD_NAME, types);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
