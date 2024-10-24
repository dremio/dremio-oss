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

import static com.dremio.io.file.UriSchemes.AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.io.file.UriSchemes.GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.S3_SCHEME;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.hive.ContextClassLoaderAware;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Wrapper file system used to work around class loader issues in Hive.
 *
 * The solution is to redirect S3/ADLS/Azure tables to this file system
 * and to create the actual fs(S3FileSystem etc) from here.
 *
 * Delegates all operations to the actual fs impl.
 *
 * Replaces class loader before any action to delegate all class loading to default
 * class loaders. This is to avoid loading non-hive/hadoop related classes here.
 */
public class DremioFileSystem extends FileSystem implements ContextClassLoaderAware {

  private com.dremio.io.file.FileSystem underLyingFs;
  private Path workingDir;
  private String scheme;
  private URI originalURI;
  private static final Map<String, Set<String>> SUPPORTED_SCHEME_MAP = ImmutableMap.of(
    DREMIO_S3_SCHEME, ImmutableSet.of("s3a", S3_SCHEME,"s3n", DREMIO_S3_SCHEME),
    DREMIO_GCS_SCHEME, ImmutableSet.of(GCS_SCHEME, DREMIO_GCS_SCHEME),
    DREMIO_AZURE_SCHEME, ImmutableSet.of(AZURE_SCHEME, "wasb", "abfs", "abfss"));

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    originalURI = name;
    FileSystemConfUtil.initializeConfiguration(name, conf);
    try (Closeable swapper = swapClassLoader()) {
      underLyingFs = HadoopFileSystem.get(name, conf.iterator(), true);
    }
    workingDir = new Path(name).getParent();
    scheme = name.getScheme();
  }

  @Override
  public URI getUri() {
    if (scheme.equals(DREMIO_AZURE_SCHEME) || scheme.equals(DREMIO_S3_SCHEME) || scheme.equals(DREMIO_GCS_SCHEME)) {
      // GCS, Azure File System and S3 File system have modified URIs.
      return originalURI;
    }
    return underLyingFs.getUri();
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    FSInputStream fsInputStream = null;
    try (Closeable swapper = swapClassLoader()) {
      fsInputStream = underLyingFs.open(com.dremio.io.file.Path.of(path.toUri()));
    }
    return new FSDataInputStream(new FSInputStreamWrapper(fsInputStream));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int i, short i1, long l, Progressable progressable) throws IOException {
    FSOutputStream fsOutputStream = null;
    try (Closeable swapper = swapClassLoader()) {
      fsOutputStream = underLyingFs.create(com.dremio.io.file.Path.of(path.toUri()),
        overwrite);
    }
    return new FSDataOutputStream(fsOutputStream, null);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException("Append to a file not supported.");
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.rename(com.dremio.io.file.Path.of(path.toUri()), com.dremio.io.file.Path.of(path1.toUri()));
    }
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.delete(com.dremio.io.file.Path.of(path.toUri()), recursive);
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(path.toUri());
    try (DirectoryStream<FileAttributes> attributes = underLyingFs.list(dremioPath)) {
      long defaultBlockSize = underLyingFs.getDefaultBlockSize(dremioPath);
      return StreamSupport.stream(attributes.spliterator(), false)
          .map(attribute -> getFileStatusFromAttributes(attribute, defaultBlockSize))
          .toArray(FileStatus[]::new);
    } catch (DirectoryIteratorException ex) {
      // I/O error encountered during the iteration, the cause is an IOException
      throw ex.getCause();
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    this.workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    String fsPerms = fsPermission.toString();
    Set<PosixFilePermission> posixFilePermission =
      PosixFilePermissions.fromString(fsPerms.substring(1, fsPerms.length()));
    try (Closeable swapper = swapClassLoader()) {
      return underLyingFs.mkdirs(com.dremio.io.file.Path.of(path.toUri()), posixFilePermission);
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    com.dremio.io.file.Path dremioPath = com.dremio.io.file.Path.of(path.toUri());
    FileAttributes attributes = null;
    long defaultBlockSize;
    try (Closeable swapper = swapClassLoader()) {
      attributes = underLyingFs.getFileAttributes(dremioPath);
      defaultBlockSize = underLyingFs.getDefaultBlockSize(dremioPath);
    }
    return getFileStatusFromAttributes(attributes, defaultBlockSize);
  }

  @Override
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String thatScheme = uri.getScheme();
    if (thatScheme == null) {              // fs is relative
      return;
    }
    URI thisUri = getCanonicalUri();
    String thisScheme = thisUri.getScheme();
    //authority and scheme are not case sensitive
    if (thisScheme.equalsIgnoreCase(thatScheme) ||
      (SUPPORTED_SCHEME_MAP.containsKey(thisScheme)
        && SUPPORTED_SCHEME_MAP.get(thisScheme).contains(thatScheme))) {// schemes match
      String thisAuthority = thisUri.getAuthority();
      String thatAuthority = uri.getAuthority();
      if (thatAuthority == null &&                // path's authority is null
        thisAuthority != null) {                // fs has an authority
        URI defaultUri = getDefaultUri(getConf());
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())) {
          uri = defaultUri; // schemes match, so use this uri instead
        } else {
          uri = null; // can't determine auth of the path
        }
      }
      if (uri != null) {
        // canonicalize uri before comparing with this fs
        uri = canonicalizeUri(uri);
        thatAuthority = uri.getAuthority();
        if (Objects.equals(thisAuthority, thatAuthority) ||       // authorities match
          (thisAuthority != null &&
            thisAuthority.equalsIgnoreCase(thatAuthority))) {
          return;
        }
      }
    }
    throw new IllegalArgumentException("Wrong FS: " + path +
      ", expected: " + this.getUri());
  }

  private FileStatus getFileStatusFromAttributes(FileAttributes attributes,
                                                 long defaultBlockSize) {
    return new FileStatus(attributes.size(), attributes.isDirectory(), 1,
            defaultBlockSize, attributes.lastModifiedTime().toMillis(), new Path(String.valueOf(attributes.getPath())));
  }

  public boolean supportsAsync() {
    return underLyingFs.supportsAsync();
  }

  public AsyncByteReader getAsyncByteReader(AsyncByteReader.FileKey fileKey, OperatorStats operatorStats, Map<String, String> options) throws IOException {
    try (Closeable swapper = swapClassLoader()) {
      if (underLyingFs instanceof HadoopFileSystem) {
        return ((HadoopFileSystem) underLyingFs).getAsyncByteReader(fileKey, operatorStats, options);
      } else {
        return underLyingFs.getAsyncByteReader(fileKey,options);
      }
    }
  }

  @Override
  public String getScheme() {
    return underLyingFs.getScheme();
  }

  /**
   * swaps current threads class loader to use application class loader
   * @return
   */
  private Closeable swapClassLoader() {
    return ContextClassLoaderSwapper.swapClassLoader(HadoopFileSystem.class);
  }

}
