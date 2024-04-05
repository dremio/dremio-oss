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

import com.dremio.exec.store.dfs.FileSystemConfigurationAdapter;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.Path;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/** DremioOutputFile. used in DremioFileIO to output iceberg metadata file. */
public class DremioOutputFile implements OutputFile {

  private final DremioFileIO io;

  private final Path path;
  private final String locationWithScheme;

  public DremioOutputFile(DremioFileIO io, Path path, FileSystemConfigurationAdapter conf) {
    this.io = io;
    this.path = path;
    this.locationWithScheme =
        IcebergUtils.getValidIcebergPath(path.toString(), conf, io.getFs().getScheme());
  }

  @Override
  public PositionOutputStream create() {
    return create(false);
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return create(true);
  }

  private PositionOutputStream create(boolean overwrite) {
    try {
      return new PositionOutputStreamWrapper(io.getFs().create(path, overwrite));
    } catch (IOException ex) {
      throw new UncheckedIOException(String.format("Failed to create file: %s", path), ex);
    }
  }

  @Override
  public String location() {
    return locationWithScheme;
  }

  @Override
  public InputFile toInputFile() {
    return io.newInputFile(path.toString());
  }

  @Override
  public String toString() {
    return location();
  }

  private static class PositionOutputStreamWrapper extends PositionOutputStream {

    private final FSOutputStream inner;

    public PositionOutputStreamWrapper(FSOutputStream inner) {
      this.inner = inner;
    }

    @Override
    public long getPos() throws IOException {
      return inner.getPosition();
    }

    @Override
    public void write(int b) throws IOException {
      inner.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      inner.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      inner.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      inner.flush();
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }
  }
}
