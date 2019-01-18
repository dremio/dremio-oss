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
package com.dremio.exec.store.easy.excel.xls;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/**
 * Off-heap buffered InputStream that keeps all read buffers in memory until the stream is closed.
 * <br><br>
 * This is not a thread-safe implementation.
 */
public class XlsInputStream extends InputStream {

  public interface BufferManager {
    ArrowBuf allocate(int size);
  }

  private final static int DEFAULT_BUFFER_SIZE = 4096*8;

  private final BufferManager bufferManager;
  private final InputStream in;
  private final int size;
  private final List<ArrowBuf> blocks = Lists.newArrayList();

  private long total;
  private long pos; // position in stream
  private ArrowBuf current;

  private long mark = -1; // marked position

  public XlsInputStream(final BufferManager bufferManager, final InputStream in) {
    this(bufferManager, in, DEFAULT_BUFFER_SIZE);
  }

  public XlsInputStream(final BufferManager bufferManager, final InputStream in, int size) {
    Preconditions.checkArgument(size > 0, "block size should never be 0");

    this.bufferManager = bufferManager;
    this.in = in;
    this.size = size;

    pos = 0;
    current = fetchCurrentBlock();
  }

  private ArrowBuf fetchCurrentBlock() {
    final int block = (int) (pos / size);

    while (blocks.size() <= block) {
      ArrowBuf buf = bufferManager.allocate(size);
      try {
        total += buf.setBytes(0, in, size);
      } catch (IOException e) {
        // we should never hit an IOException for a well formatted XLS file
        throw new IllegalStateException("Couldn't read a block from the input stream");
      }
      blocks.add(buf);
    }
    return blocks.get(block);
  }

  @Override
  public int read() {
    if (pos == total) {
      return -1; // EOF
    }

    int res = current.getByte((int) (pos % size)) & 0xff;

    pos++; //move read index
    if ((pos % size) == 0) {
      current = fetchCurrentBlock();
    }

    return res;
  }

  @Override
  public void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public void reset() {
    assert mark != -1 : "mark() should be called before reset()";
    pos = mark;
    current = fetchCurrentBlock();
  }

  public void seek(long offset) {
    pos = offset;
    current = fetchCurrentBlock();
  }

  @Override
  public int available() {
    return (int)(total - pos);
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() throws IOException {
    in.close();
    blocks.clear();
  }
}
