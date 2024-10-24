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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.dremio.io.FSInputStream;

/**
 * Wraps the FSInputStream so that it can be used to construct a FSDataInputStream
 * which needs a seekable stream as source.
 */
public class FSInputStreamWrapper extends InputStream implements Seekable, PositionedReadable {
  private final FSInputStream fsInputStream;

  public FSInputStreamWrapper(FSInputStream fsInputStream) {
    Preconditions.checkNotNull(fsInputStream);
    this.fsInputStream = fsInputStream;
  }

  @Override
  public int read() throws IOException {
    return fsInputStream.read();
  }

  @Override
  public int read(long position, byte[] bytes, int offset, int length) throws IOException {
    fsInputStream.setPosition(position);
    return fsInputStream.read(bytes, offset, length);
  }

  @Override
  public void readFully(long position, byte[] bytes, int offset, int length) throws IOException {
    fsInputStream.setPosition(position);
    fsInputStream.read(bytes, offset, length);
  }

  @Override
  public void readFully(long position, byte[] bytes) throws IOException {
    fsInputStream.setPosition(position);
    fsInputStream.read(bytes);
  }

  @Override
  public void seek(long position) throws IOException {
    fsInputStream.setPosition(position);
  }

  @Override
  public long getPos() throws IOException {
    return fsInputStream.getPosition();
  }

  @Override
  public boolean seekToNewSource(long position) throws IOException {
    fsInputStream.setPosition(position);
    return true;
  }

  @Override
  public void close() throws IOException {
    fsInputStream.close();
  }
}
