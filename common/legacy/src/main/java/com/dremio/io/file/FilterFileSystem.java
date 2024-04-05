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
package com.dremio.io.file;

import com.dremio.io.AsyncByteReader;
import com.dremio.io.AsyncByteReader.FileKey;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessControlException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** A filter file system delegating calls to another instance */
public class FilterFileSystem implements FileSystem {
  private final FileSystem fs;

  public FilterFileSystem(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }

  @Override
  public Path canonicalizePath(Path p) throws IOException {
    return fs.canonicalizePath(p);
  }

  @Override
  public FSInputStream open(Path f) throws FileNotFoundException, IOException {
    return fs.open(f);
  }

  @Override
  public String getScheme() {
    return fs.getScheme();
  }

  @Override
  public FSOutputStream create(Path f) throws FileNotFoundException, IOException {
    return fs.create(f);
  }

  @Override
  public FSOutputStream create(Path f, boolean overwrite)
      throws FileAlreadyExistsException, IOException {
    return fs.create(f, overwrite);
  }

  @Override
  public FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException {
    return fs.getFileAttributes(f);
  }

  @Override
  public void setPermission(Path p, Set<PosixFilePermission> permissions)
      throws FileNotFoundException, IOException {
    fs.setPermission(p, permissions);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return fs.unwrap(clazz);
  }

  @Override
  public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
    return fs.mkdirs(f, permissions);
  }

  @Override
  public boolean supportsAsync() {
    return fs.supportsAsync();
  }

  @Override
  public boolean supportsPathsWithScheme() {
    return fs.supportsPathsWithScheme();
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public AsyncByteReader getAsyncByteReader(FileKey fileKey, Map<String, String> options)
      throws IOException {
    return fs.getAsyncByteReader(fileKey, options);
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
    return fs.list(f);
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    return fs.list(f, filter);
  }

  @Override
  public DirectoryStream<FileAttributes> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    return fs.listFiles(f, recursive);
  }

  @Override
  public DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    return fs.glob(pattern, filter);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return fs.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return fs.exists(f);
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return fs.isDirectory(f);
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return fs.isFile(f);
  }

  @Override
  public URI getUri() {
    return fs.getUri();
  }

  @Override
  public Path makeQualified(Path path) {
    return fs.makeQualified(path);
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(
      FileAttributes file, long start, long len) throws IOException {
    return fs.getFileBlockLocations(file, start, len);
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    return fs.getFileBlockLocations(p, start, len);
  }

  @Override
  public void access(Path path, Set<AccessMode> mode)
      throws AccessControlException, FileNotFoundException, IOException {
    fs.access(path, mode);
  }

  @Override
  public boolean isPdfs() {
    return fs.isPdfs();
  }

  @Override
  public boolean isMapRfs() {
    return fs.isMapRfs();
  }

  @Override
  public boolean supportsBlockAffinity() {
    return fs.supportsBlockAffinity();
  }

  @Override
  public boolean supportsPath(Path path) {
    return fs.supportsPath(path);
  }

  @Override
  public long getDefaultBlockSize(Path path) {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public boolean preserveBlockLocationsOrder() {
    return fs.preserveBlockLocationsOrder();
  }

  @Override
  public boolean supportsBoosting() {
    return fs.supportsBoosting();
  }

  @Override
  public BoostedFileSystem getBoostedFilesystem() {
    return fs.getBoostedFilesystem();
  }
}
