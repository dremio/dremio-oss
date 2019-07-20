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
package com.dremio.exec.store.parquet;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class ByteBufferFSDataInputStream extends ByteBufInputStream implements Seekable, PositionedReadable {

  public ByteBufferFSDataInputStream(ByteBuf buffer) {
    super(buffer, true);
  }


  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    int l = read(position, buffer, 0, buffer.length);
    if (l < buffer.length) {
      throw new EOFException();
    }
  }

  @Override
  public int read(long position, byte[] b, int off, int len) throws IOException {
    seek(position);
    return read(b, off, len);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int l = read(position, buffer, offset, length);
    if (l < length) {
      throw new EOFException();
    }
  }

  @Override
  public long getPos() throws IOException {
    return readBytes();
  }

  @Override
  public boolean seekToNewSource(long position) throws IOException {
    seek(position);
    return true;
  }

  @Override
  public void seek(long position) throws IOException {
    long toSkip;
    long curPos = getPos();
    if (curPos <= position) {
      toSkip = position - curPos;
    } else {
      reset();
      toSkip = position;
    }
    while (toSkip > 0) {
      toSkip = toSkip - skip(toSkip);
    }
  }
}
