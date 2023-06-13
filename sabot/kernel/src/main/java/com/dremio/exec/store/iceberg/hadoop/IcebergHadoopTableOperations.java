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
package com.dremio.exec.store.iceberg.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LockManagers;

import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileAttributes;

/**
 * Hadoop based iceberg table operations
 */
public class IcebergHadoopTableOperations extends HadoopTableOperations {
  private final Configuration conf;
  private final com.dremio.io.file.FileSystem fs;

  public IcebergHadoopTableOperations(Path location, Configuration conf, com.dremio.io.file.FileSystem fs,
      FileIO fileIO) {
    super(location, fileIO, conf, LockManagers.defaultLockManager());
    this.conf = conf;
    this.fs = fs;
  }

  @Override
  protected org.apache.hadoop.fs.FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    return new DremioToHadoopFileSystemProxy();
  }

  /**
   * Proxy class for exposing a Dremio FileSystem as a Hadoop FileSystem.
   */
  private class DremioToHadoopFileSystemProxy extends FileSystem {

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public URI getUri() {
      return fs.getUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      FSInputStream fsInputStream = fs.open(com.dremio.io.file.Path.of(f.toUri()));
      return new FSDataInputStream(new SeekableFSInputStream(fsInputStream));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
        short replication, long blockSize, Progressable progress) throws IOException {
      FSOutputStream fsOutputStream = fs.create(com.dremio.io.file.Path.of(f.toUri()), overwrite);
      return new FSDataOutputStream(fsOutputStream, null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return fs.rename(com.dremio.io.file.Path.of(src.toUri()), com.dremio.io.file.Path.of(dst.toUri()));
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return fs.delete(com.dremio.io.file.Path.of(f.toUri()), recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      final List<FileStatus> fileStatusList = new ArrayList<>();
      com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(f.toUri());
      DirectoryStream<FileAttributes> attributes = fs.list(dremioPath);
      long defaultBlockSize = fs.getDefaultBlockSize(dremioPath);
      attributes.forEach(attribute -> fileStatusList.add(getFileStatusFromAttributes(attribute, defaultBlockSize)));
      return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      String fsPerms = permission.toString();
      Set<PosixFilePermission> posixFilePermission =
          PosixFilePermissions.fromString(fsPerms.substring(1));
      return fs.mkdirs(com.dremio.io.file.Path.of(f.toUri()), posixFilePermission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(f.toUri());
      FileAttributes attributes = fs.getFileAttributes(dremioPath);
      long defaultBlockSize = fs.getDefaultBlockSize(dremioPath);

      return getFileStatusFromAttributes(attributes, defaultBlockSize);
    }

    private FileStatus getFileStatusFromAttributes(FileAttributes attributes, long defaultBlockSize) {
      return new FileStatus(attributes.size(), attributes.isDirectory(), 1,
          defaultBlockSize, attributes.lastModifiedTime().toMillis(), new Path(String.valueOf(attributes.getPath())));
    }

    @Override
    public String getScheme() {
      return fs.getScheme();
    }
  }

  private class SeekableFSInputStream extends InputStream implements Seekable, PositionedReadable {
    private final FSInputStream fsInputStream;

    public SeekableFSInputStream(FSInputStream fsInputStream) {
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
}
