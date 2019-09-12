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

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.io.compress.CompressionInputStream;

import com.dremio.io.CompressedFSInputStream;

/**
 * Wrapper around {@code CompressionInputStream}
 */
@NotThreadSafe
public class CompressionInputStreamWrapper extends CompressedFSInputStream {
  private final CompressionInputStream underlyingIs;
  private final byte[] temp = new byte[8192];

  public CompressionInputStreamWrapper(CompressionInputStream in) throws IOException {
    underlyingIs = in;
  }

  @Override
  public int read() throws IOException {
    return underlyingIs.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return underlyingIs.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return underlyingIs.read(b, off, len);
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

  @Override
  public long getPosition() throws IOException {
    return underlyingIs.getPos();
  }

  @Override
  public void setPosition(long position) throws IOException {
    underlyingIs.seek(position);
  }

  @Override
  public long skip(long n) throws IOException {
    return underlyingIs.skip(n);
  }

  @Override
  public int available() throws IOException {
    return underlyingIs.available();
  }

  @Override
  public void close() throws IOException {
    underlyingIs.close();
  }

  @Override
  public void mark(int readlimit) {
    underlyingIs.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    underlyingIs.reset();
  }

  @Override
  public boolean markSupported() {
    return underlyingIs.markSupported();
  }
}
