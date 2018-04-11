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
package com.dremio.exec.store.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A file system that throws exceptions if used.
 */
class NoopFileSystem extends FileSystem {

  private static final String ERROR = "Local PDFs access isn't supported on a coordination node.";

  private static final URI NAME = URI.create("noop:///");

  public NoopFileSystem(){

  }
  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    throw new UnsupportedOperationException(ERROR);
  }
}
