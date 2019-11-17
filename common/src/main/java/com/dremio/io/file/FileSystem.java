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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessControlException;
import java.util.Set;
import java.util.function.Predicate;

import com.dremio.io.AsyncByteReader;
import com.dremio.io.AsyncByteReader.FileKey;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;

/**
 * FileSystem interface used by storage plugins.
 *
 */
public interface FileSystem extends Closeable {

  /**
   * Gets an input stream for the file designated by path {@code f}
   *
   * @param f the file to read
   * @throws FileNotFoundException if file doesn't exist
   * @throws IOException if an error occurs when accessing file information
   */
  FSInputStream open(Path f) throws FileNotFoundException, IOException;

  /**
   * Gets the protocol scheme for the filesystem
   * @return the protocol schema
   */
  String getScheme();

  /**
   * Creates a new file designated by path {@code f}
   *
   * Note that if {@code f} already exists, it will be overwritten
   * @param f the file to create
   * @return an output stream to write file content
   * @throws IOException if an error occurs during creation
   */
  FSOutputStream create(Path f) throws FileNotFoundException, IOException;

  /**
   * Creates a new file designated by path {@code f}
   *
   * Note that if {@code f} already exists, it will be overwritten
   * @param f the file to create
   * @param overwrite if {@code true}, an existing file will be overwritten.
   * @return an output stream to write file content
   * @throws java.nio.file.FileAlreadyExistsException if file already exists
   * and {@code overwrite} is set to {@code false}
   * @throws IOException if an error occurs during creation
   */
  FSOutputStream create(Path f, boolean overwrite) throws FileAlreadyExistsException, IOException;

  /**
   * Gets file information for file designated by path {@code f}
   *
   * @param f the file to get information
   * @return file information
   * @throws FileNotFoundException if file doesn't exist
   * @throws IOException if an error occurs when accessing file information
   */
  FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException;

  /**
   * Sets permission for file designated by path {@code f}
   *
   * @param f the file to update permission
   * @param permissions the new set of permissions
   * @throws FileNotFoundException if file doesn't exist
   * @throws IOException if an error occurs when accessing file information
   */
  void setPermission(Path p, Set<PosixFilePermission> permissions) throws FileNotFoundException, IOException;

  <T> T unwrap(Class<T> clazz);

  /**
   * Creates a new directory designated by {@code f} with the given {@code permission}
   *
   * If parent directories do not exist, they are created too.
   *
   * @param f the directory to create
   * @param permissions the directory permission
   * @return {@code true} if directory was successfully created,
   *  {@code false} if the directory already exists
   * @throws IOException if an error occurs when creating the directory
   */
  boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException;

  /**
   * Creates a new directory designated by {@code f}
   *
   * If parent directories do not exist, they are created too.
   *
   * @param f the directory to create
   * @return {@code true} if directory was successfully created,
   *  {@code false} if the directory already exists
   * @throws IOException if an error occurs when creating the directory
   */
  boolean mkdirs(Path f) throws IOException;

  /**
   * Lists content of directory designated by {@code f}
   *
   * @param f the directory path
   * @return a list of file attributes for each entry in the directory
   * @throws FileNotFoundException if the directory does not exist
   * @throws IOException if an error occurs during listing
   */
  DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException;

  /**
   * Lists content of directory designated by {@code f}
   *
   * @param f the directory path
   * @param filter a filter to filter out directory entries from the result
   * @return a list of file attributes for each entry in the directory
   * @throws FileNotFoundException if the directory does not exist
   * @throws IOException if an error occurs during listing
   */
  DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter) throws FileNotFoundException, IOException;

  /**
   * Lists content of directory designated by glob pattern {@code p}
   *
   * @param the glob pattern
   * @param filter a filter to filter out directory entries from the result
   * @return a list of file attributes for each entry in the directory
   * @throws FileNotFoundException if the directory does not exist
   * @throws IOException if an error occurs during listing
   */
  DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter) throws FileNotFoundException, IOException;

  /**
   * Renames the file designated by {@code src} to {@code dst}
   *
   * @param src path to the source file
   * @param dst the new path for the file
   * @return {@code true} if rename is successful.
   * @throws IOException if an error occurs during renaming
   */
  boolean rename(Path src, Path dst) throws IOException;

  /**
   * Deletes the file designated by {@code f}.
   *
   * If {@code f} designates a directory and {@code recursive} is set to {@code true},
   * all deletes each directory children recursively.
   *
   * @param f path to delete
   * @param recursive if delete should be done recursively
   * @return {@code true} if rename is successful.
   * @throws IOException if an error occurs during deletion
   */
  boolean delete(Path f, boolean recursive) throws IOException;

  /**
   * Checks if path designated by {@code f} exists
   *
   * @param f the path to check
   * @return {@code} true if the path exists, {@code false} otherwise
   * @throws IOException if an error occurs while checking
   */
  boolean exists(Path f) throws IOException;

  /**
   * Checks if path designated by {@code f} exists an d is a directory
   *
   * @param f the path to check
   * @return {@code} true if the path exists and is a directory, {@code false} otherwise
   * @throws IOException if an error occurs while checking
   */
  boolean isDirectory(Path f) throws IOException;

  /**
   * Checks if path designated by {@code f} exists an d is a regular file
   *
   * @param f the path to check
   * @return {@code} true if the path exists and is a regular file, {@code false} otherwise
   * @throws IOException if an error occurs while checking
   */
  boolean isFile(Path f) throws IOException;

  /**
   * Gets the filesystem URI
   * @return the filesystem URI
   */
  URI getUri();

  /**
   * Makes the path qualified by adding the filesystem URI to it (if not present already).
   * @param path the path
   * @return the qualified path
   */
  Path makeQualified(Path path);

  /**
   * Lists block locations for file designated by {@code file}.
   *
   * Only block locations for region designated by {@code start} and {@code len}
   * are returned
   *
   * @param file
   *          the file status of the path to get block locations from
   * @param start
   *          the start offset of the region
   * @param len
   *          the length of the region
   * @return an array of block locations. The array might be empty if
   *         {@code len} is 0, or start exceeds the file size, otherwise it
   *         contains at least one element
   * @throws IOException if an error occurs while checking
   */
  Iterable<FileBlockLocation> getFileBlockLocations(FileAttributes file, long start, long len) throws IOException;

  /**
   * Lists block locations for file designated by {@code p}.
   *
   * Only block locations for region designated by {@code start} and {@code len}
   * are returned
   *
   * @param p
   *          the file to get block locations from
   * @param start
   *          the start offset of the region
   * @param len
   *          the length of the region
   * @return an array of block locations. The array might be empty if
   *         {@code len} is 0, or start exceeds the file size, otherwise it
   *         contains at least one element
   * @throws IOException if an error occurs while checking
   */
  Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len) throws IOException;

  /**
   * Checks if the path designated by {@code path} is accessible for the
   * given {@code mode}.
   *
   * @param path the path to check access
   * @param mode the access mode
   * @throws AccessControlException if the current user is not allowed to access the patg.
   * @throws FileNotFoundException the the path doesn't exist
   * @throws IOException if an error occurs while checking
   */
  void access(final Path path, final Set<AccessMode> mode) throws AccessControlException, FileNotFoundException, IOException;

  /**
   * Checks if the filesystem is PDFS-based
   * @return {@code true} if PDFS, {@code false} otherwise
   */
  boolean isPdfs();

  /**
   * Checks if the filesystem is MapR-based
   * @return {@code true} if MapR, {@code false} otherwise
   */
  boolean isMapRfs();

  boolean supportsPath(Path path);

  /**
   * Gets filesystem default block size for {@code path}
   *
   * @param path
   *          the default block size filesystem would use to write {@code path}
   * @return the size in bytes, -1 if the filesystem doesn't know/do not provide
   *         to directly access based on blocks
   */
  long getDefaultBlockSize(Path path);

  /**
   * Rewrites the provided path so that the filesystem would allow for write
   * operations.
   *
   * The same path can also be used to read, and should be considered the canonical
   * version of the original path.
   *
   * @param p the original path
   * @return a path allowing for write operations (possibly the same path)
   * @throws IOException
   */
  Path canonicalizePath(Path p) throws IOException;

  /**
   * Whether this FileSystem may support async reads.
   * @return true if async reads are supported for the given file.
   */
  boolean supportsAsync();

  /**
   * For a given file key, get an AsyncByteReader.
   * @param fileKey for which async reader is requested
   * @return async reader
   * @throws IOException if async reader cannot be instantiated
   */
  AsyncByteReader getAsyncByteReader(FileKey fileKey) throws IOException;
  /**
   * Calculation of host affinities for various filesystems are different. For example, HDFS read
   * can be on any of the replicas of the block. So, no ordering is required. In case of cached
   * file system, the read has to hit the node that cached the data. So, ordering of the locations is required.
   * @return
   */
  default boolean preserveBlockLocationsOrder() { return false; }

}
