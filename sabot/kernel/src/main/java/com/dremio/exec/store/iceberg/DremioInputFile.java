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
import java.io.UncheckedIOException;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import com.dremio.exec.store.dfs.FileSystemConfigurationAdapter;
import com.dremio.io.file.Path;

/**
 * DremioInputFile is a dremio implementation of Iceberg InputFile interface.
 * It uses Dremio implementation of FileSystem to leverage several existing
 * functionalities like tracking IO time, Caching, Async read aheads where necessary.
 */
public class DremioInputFile implements InputFile {

  private final DremioFileIO io;

  private final Path path;
  private Long fileSize;
  private final Long mtime;
  private final String locationWithScheme;

  public DremioInputFile(DremioFileIO io, Path path, Long fileSize, Long mtime, FileSystemConfigurationAdapter conf) {
    this.io = io;
    this.path = path;
    this.fileSize = fileSize;
    this.mtime = mtime;
    this.locationWithScheme = IcebergUtils.getValidIcebergPath(path.toString(), conf, io.getFs().getScheme());
  }

  @Override
  public long getLength() {
    if (fileSize == null) {
      try {
        fileSize = io.getFs().getFileAttributes(path).size();
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Failed to get file attributes for file: %s", path), e);
      }
    }

    return fileSize;
  }

  public long getVersion() {
    return mtime != null ? mtime : 0;
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      SeekableInputStreamFactory factory = io.getContext() == null || io.getDataset() == null ?
          SeekableInputStreamFactory.DEFAULT :
          io.getContext().getConfig().getInstance(SeekableInputStreamFactory.KEY, SeekableInputStreamFactory.class,
              SeekableInputStreamFactory.DEFAULT);
      return factory.getStream(io.getFs(), io.getContext(),
          path, fileSize, mtime, io.getDataset(), io.getDatasourcePluginUID());
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to create new input stream for file: %s", path), e);
    }
  }

  @Override
  public String location() {
    return locationWithScheme;
  }

  @Override
  public boolean exists() {
    try {
      return io.getFs().exists(path);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to check existence of file: %s", path), e);
    }
  }
}
