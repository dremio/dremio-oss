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
package com.dremio.exec.store.parquet;

import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import java.io.IOException;
import org.apache.parquet.io.PositionOutputStream;

public final class OutputFile implements org.apache.parquet.io.OutputFile {
  private final FileSystem fs;
  private final Path path;

  private OutputFile(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  public static OutputFile of(FileSystem fs, Path path) {
    return new OutputFile(fs, path);
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new PositionOutputStreamWrapper(fs.create(path));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new PositionOutputStreamWrapper(fs.create(path, true));
  }

  @Override
  public boolean supportsBlockSize() {
    return defaultBlockSize() > 0;
  }

  @Override
  public long defaultBlockSize() {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  private static final class PositionOutputStreamWrapper extends PositionOutputStream {
    private final FSOutputStream os;

    public PositionOutputStreamWrapper(FSOutputStream os) {
      this.os = os;
    }

    @Override
    public long getPos() throws IOException {
      return os.getPosition();
    }

    @Override
    public void write(int b) throws IOException {
      os.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      os.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      os.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      os.flush();
    }

    @Override
    public void close() throws IOException {
      os.close();
    }
  }
}
