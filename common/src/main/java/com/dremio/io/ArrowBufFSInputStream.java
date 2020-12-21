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
package com.dremio.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.ArrowBuf;

import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * An implementation of {@code FSInputStream} using a {@code ArrowBuf}
 * as its backend.
 *
 * The buffer will be closed when the stream is closed
 */
@NotThreadSafe
public class ArrowBufFSInputStream extends FSInputStream {
  private final ByteBuf nettyBuf;
  private final int startIndex;
  private final int endIndex;
  private boolean closed = false;

  /**
   * Creates a new stream
   *
   * The current position and limit will be used as the start and
   * end indices.
   * @param buf
   */
  public ArrowBufFSInputStream(ArrowBuf buf) {
    this.nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
    this.startIndex = nettyBuf.readerIndex();
    this.endIndex = this.startIndex + nettyBuf.readableBytes();
  }

  @Override
  public int read() throws IOException {
    if (available() <= 0) {
      return -1;
    }
    return nettyBuf.readByte();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int available = available();
    if (available <= 0) {
      return -1;
    }
    int toRead = Math.min(len, available);
    nettyBuf.readBytes(b, off, toRead);
    return toRead;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int available = available();
    if (available <= 0) {
      return -1;
    }
    int toRead = Math.min(available, dst.remaining());
    ByteBuffer tmpBuf = (ByteBuffer) dst.slice().limit(toRead);
    nettyBuf.readBytes(tmpBuf);
    dst.position(dst.position() + toRead);
    return toRead;
  }

  @Override
  public int read(long position, ByteBuffer dst) throws IOException {
    setPosition(position);
    return read(dst);
  }

  @Override
  public long getPosition() throws IOException {
    return nettyBuf.readerIndex() - startIndex;
  }

  @Override
  public void setPosition(long position) throws IOException {
    long newPosition = startIndex + position;
    if (newPosition > endIndex) {
      throw new EOFException("Cannot move past end of buffer");
    }
    // Cast is safe as newPosition less than endIndex
    nettyBuf.readerIndex((int) newPosition);
  }

  @Override
  public long skip(long n) throws IOException {
    int toSkip = Ints.saturatedCast(Math.min(available(), n));
    if (toSkip <= 0) {
      return 0;
    }

    nettyBuf.skipBytes(toSkip);
    return toSkip;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public synchronized void mark(int readlimit) {
    nettyBuf.markReaderIndex();
  }

  @Override
  public synchronized void reset() throws IOException {
    nettyBuf.resetReaderIndex();
  }

  @Override
  public int available() throws IOException {
    return endIndex - nettyBuf.readerIndex();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    nettyBuf.release();
  }
}
