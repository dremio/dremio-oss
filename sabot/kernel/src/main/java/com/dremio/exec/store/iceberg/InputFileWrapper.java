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
package com.dremio.exec.store.iceberg;

import java.io.IOException;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * A wrapper InputFile that uses dremio fs wrappers to do IO.
 */
public class InputFileWrapper implements InputFile {
  private FileSystem fs;
  private Path path;
  private long length;

  public InputFileWrapper(FileSystem fs, Path path, long length) {
    this.fs = fs;
    this.path = path;
    this.length = length;
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return wrap(fs.open(path));
    } catch (IOException e) {
      throw new RuntimeException("open failed for " + path, e);
    }
  }

  private SeekableInputStream wrap(FSInputStream fsInputStream) {
    return new SeekableInputStream() {
      @Override
      public long getPos() throws IOException {
        return fsInputStream.getPosition();
      }

      @Override
      public void seek(long position) throws IOException {
        fsInputStream.setPosition(position);
      }

      @Override
      public int read() throws IOException {
        return fsInputStream.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return fsInputStream.read(b, off, len);
      }
    };
  }
}
