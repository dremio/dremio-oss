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

import com.dremio.common.exceptions.UserException;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessControlException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * FileSystem implementation responsible for overriding all metadataStorage Plugin instances for
 * FilterFileSystem methods that take a Path as input. All override methods are designed to support
 * Dynamic metadata Path rewriting. The purpose of Path rewriting is to map an iceberg's metadata
 * Path to the latest configured distributed storage metadata directory.
 */
public class DistStorageMetadataPathRewritingFileSystem extends FilterFileSystem {

  // path of distributed storage. Set via dremio.conf
  private final Path distStoragePath;
  private final FileSystem fs;

  // set of supported file types written to the storage's metadata folder
  public static final Set<String> METADATA_FILE_EXTENSIONS =
      new HashSet<String>() {
        {
          add(".json");
          add(".avro");
          add(".crc");
        }
      };
  private static final String METADATA_SUBDIRECTORY = "metadata";
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DistStorageMetadataPathRewritingFileSystem.class);

  public DistStorageMetadataPathRewritingFileSystem(FileSystem fs, Path distStoragePath) {
    super(fs);
    Preconditions.checkArgument(distStoragePath != null, "distributed storage should exist");
    this.distStoragePath = distStoragePath;
    this.fs = fs;
  }

  public Path getDistStoragePath() {
    return this.distStoragePath;
  }

  @Override
  public Path canonicalizePath(Path p) throws IOException {
    Path updatedPath = rewritePathIfNecessary(p);
    return super.canonicalizePath(updatedPath);
  }

  @Override
  public FSInputStream open(Path f) throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.open(updatedPath);
  }

  @Override
  public FSOutputStream create(Path f) throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.create(updatedPath);
  }

  @Override
  public FSOutputStream create(Path f, boolean overwrite)
      throws FileAlreadyExistsException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.create(updatedPath, overwrite);
  }

  @Override
  public FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.getFileAttributes(updatedPath);
  }

  @Override
  public void setPermission(Path p, Set<PosixFilePermission> permissions)
      throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(p);
    super.setPermission(updatedPath, permissions);
  }

  @Override
  public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.mkdirs(updatedPath, permissions);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.mkdirs(updatedPath);
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.list(updatedPath);
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.list(updatedPath, filter);
  }

  @Override
  public AsyncByteReader getAsyncByteReader(
      AsyncByteReader.FileKey fileKey, Map<String, String> options) throws IOException {
    // TODO: DX-65934... will this work for cross functionality of distributed storages? (example:
    // Moving from S3 --> Azure)
    Path updatedPath = rewritePathIfNecessary(fileKey.getPath());
    AsyncByteReader.FileKey updatedFileKey =
        AsyncByteReader.FileKey.of(
            updatedPath,
            fileKey.getVersion(),
            fileKey.getFileType(),
            fileKey.getDatasetKey(),
            fileKey.getPluginUID().get());
    return super.getAsyncByteReader(updatedFileKey, options);
  }

  @Override
  public DirectoryStream<FileAttributes> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.listFiles(updatedPath, recursive);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path updatedPath = rewritePathIfNecessary(src);
    return super.rename(updatedPath, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.delete(updatedPath, recursive);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.exists(updatedPath);
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.isDirectory(updatedPath);
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    Path updatedPath = rewritePathIfNecessary(f);
    return super.isFile(updatedPath);
  }

  @Override
  public Path makeQualified(Path path) {
    Path updatedPath = rewritePathIfNecessary(path);
    return super.makeQualified(updatedPath);
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    Path updatedPath = rewritePathIfNecessary(p);
    return super.getFileBlockLocations(updatedPath, start, len);
  }

  @Override
  public void access(Path path, Set<AccessMode> mode)
      throws AccessControlException, FileNotFoundException, IOException {
    Path updatedPath = rewritePathIfNecessary(path);
    super.access(updatedPath, mode);
  }

  /**
   * rewrites the filepath of files within the given distributed-storage metadata root directory.
   * The old metadata root directory is replaced with the new distributed storage metadata root
   * directory. The new location is based on the user's distributed storage configuration in
   * dremio.conf. If we need to conduct a path rewrite on a file, it is guaranteed to follow the
   * following format:
   * "[distributedStorageRootMetadata]/[metadataguid]/"metadata"/[iceberg-md-file-name]" We will
   * utilize this guarantee to construct our path re-write.
   *
   * @param originalPath = the path contained in the original metadata directory when dataset was
   *     initially promoted
   * @return the new location of the metadata contents.
   */
  public Path rewritePathIfNecessary(Path originalPath) {
    // If the original path is null, has no parent, or is already equal to the destination storage
    // path,
    // no rewriting is necessary, so return the original path.
    if (originalPath == null
        || originalPath.getParent() == null
        || originalPath.toURI().getPath().equals(distStoragePath.toString())) {
      return originalPath;
    }
    String filename = originalPath.getName().toLowerCase();
    String parentName = originalPath.getParent().getName();
    // If the filename has no extension, is not within a "metadata" folder, and is not one of the
    // specified file types,
    // no rewriting is necessary, so return the original path.
    if (filename.lastIndexOf(".") < 0
        || !METADATA_SUBDIRECTORY.equals(parentName)
        || !METADATA_FILE_EXTENSIONS.contains(filename.substring(filename.lastIndexOf(".")))) {
      return originalPath;
    }

    Path metadataRootFolder = originalPath.getParent().getParent();
    // If the metadata root folder is null, indicating that the original path doesn't have a
    // metadata root,
    // no rewriting is necessary, so return the original path.
    if (metadataRootFolder == null) {
      return originalPath;
    }

    // merge the original path to the metadata root subfolder. Then merge with the new distributed
    // storage root
    Path relativeOriginalPath =
        Objects.requireNonNull(metadataRootFolder.getParent()).relativize(originalPath);
    Path mergedPath = Path.mergePaths(distStoragePath, relativeOriginalPath);

    // create the new path by building new uri. We do this to ensure the connection's scheme +
    // authority are included if present
    URI updatedUri;
    URI distStoreUri = fs.getUri();
    try {
      updatedUri =
          new URI(
              distStoreUri.getScheme(),
              distStoreUri.getAuthority(),
              mergedPath.toURI().getPath(),
              distStoreUri.getQuery(),
              distStoreUri.getFragment());
    } catch (URISyntaxException e) {
      logger.warn("Metadata Path Relocation Failed", e);
      throw UserException.invalidMetadataError(e)
          .message("Metadata Path Relocation Failed")
          .buildSilently();
    }
    return Path.of(updatedUri);
  }
}
