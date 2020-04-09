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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

import com.dremio.io.CompressionCodec;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.FSInputStream;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

public class FileSystemUtils {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FileSystemUtils.class);
  private static final Queue<DeleteEntry> TO_DELETE_ON_EXIT = new ConcurrentLinkedQueue<>();

  private static class DeleteEntry {
    private final FileSystem fs;
    private final Path path;

    public DeleteEntry(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread("files-delete-on-exit") {
      @Override
      public void run() {
        for (DeleteEntry entry = TO_DELETE_ON_EXIT.poll(); entry != null; entry = TO_DELETE_ON_EXIT.poll()) {
          try {
            entry.fs.delete(entry.path, true);
          } catch (IOException e) {
            LOGGER.warn("Could not delete path {}", entry.path, e);
          }
        }
      }
    });
  }

  private FileSystemUtils() {
  }

  /**
   * Adds a hook to delete the file designated by {@code path} during JVM
   * shutdown.
   *
   * The operation is not guaranteed to complete successfully
   *
   * @param fs
   * @param path
   * @return a key to the entry for cancellation
   */
  public static Object deleteOnExit(FileSystem fs, Path path) {
    final DeleteEntry key = new DeleteEntry(fs, path);
    TO_DELETE_ON_EXIT.add(key);
    return key;
  }

  /**
   * Cancels the remove of a file on JVM shutdown
   * @param key the key returned by {@code deleteOnExit} to cancel
   */
  public static void cancelDeleteOnExit(Object key) {
    TO_DELETE_ON_EXIT.remove(key);
  }

  /** create a file with the provided permission
   * The permission of the file is set to be the provided permission as in
   * setPermission, not permission&~umask
   *
   * It is implemented using two RPCs. It is understood that it is inefficient,
   * but the implementation is thread-safe. The other option is to change the
   * value of umask in configuration to be 0, but it is not thread-safe.
   *
   * @param fs file system handle
   * @param file the name of the file to be created
   * @param permission the permission of the file
   * @return an output stream
   * @throws IOException
   */
  public static OutputStream create(FileSystem fs,
      Path file, Set<PosixFilePermission> permissions) throws IOException {
    // create the file with default permission
    OutputStream out = fs.create(file);
    try {
      // set its permission to the supplied one
      fs.setPermission(file, permissions);
    } catch (IOException e) {
      Closeables.close(out, true);
      throw e;
    }
    return out;
  }

  /**
   * Opens a compressed files and creates a input stream to read uncompressed data.
   *
   * If the compression codec factory does not find any suitable coded, the file is opened directly.
   *
   * @param factory the compression codec factory
   * @param fs the filesystem to use
   * @param path the file to open
   * @return a input stream. If file is compressed, returns an instance of {@code com.dremio.io.CompressedFSInputStream}
   * @throws IOException
   */
  public static FSInputStream openPossiblyCompressedStream(CompressionCodecFactory factory, FileSystem fs, Path path) throws IOException {
    final CompressionCodec codec = factory.getCodec(path);
    if (codec != null) {
      return codec.newInputStream(fs.open(path));
    } else {
      return fs.open(path);
    }
  }

  /**
   * Lists recursively all files present under the given path {@code path}
   *
   * @param fs the filesystem
   * @param path
   *          the path to use as the root of the search
   * @param pathFilter
   *          the filter to apply on entries. If a directory entry is filtered
   *          out, all the children of the directory will not be present in the
   *          returned stream.
   * @return a stream of file attributes
   * @throws IOException
   *           if an error occurs while listing directory content
   */
  public static DirectoryStream<FileAttributes> listRecursive(FileSystem fs, Path path,
      Predicate<Path> pathFilter) throws IOException {
    final DirectoryStream<FileAttributes> stream = fs.list(path, pathFilter);
    return new RecursiveDirectoryStream(fs, stream, pathFilter);
  }

  public static DirectoryStream<FileAttributes> globRecursive(FileSystem wrapper, Path pattern, Predicate<Path> filter) throws IOException {
    final DirectoryStream<FileAttributes> globStream = wrapper.glob(pattern, filter);
    return new RecursiveDirectoryStream(wrapper, globStream, filter);
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, true);
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             boolean overwrite) throws IOException {
    FileAttributes fileAttributes = srcFS.getFileAttributes(src);
    return copy(srcFS, fileAttributes, dstFS, dst, deleteSource, overwrite);
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, FileAttributes srcAttributes,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             boolean overwrite) throws IOException {
    Path src = srcAttributes.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcAttributes.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      try (DirectoryStream<FileAttributes> contents = srcFS.list(src)) {
        for (FileAttributes content : contents) {
          copy(srcFS, content, dstFS,
              dst.resolve(content.getPath().getName()),
              deleteSource, overwrite);
        }
      }
    } else {
      try (final InputStream in = srcFS.open(src);
          final OutputStream out = dstFS.create(dst, overwrite)) {
        ByteStreams.copy(in, out);
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }

  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
      boolean overwrite) throws IOException {
    if (dstFS.exists(dst)) {
      FileAttributes sdst = dstFS.getFileAttributes(dst);
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, dst.resolve(srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  //
  // If the destination is a subdirectory of the source, then
  // generate exception
  //
  private static void checkDependencies(FileSystem srcFS,
                                        Path src,
                                        FileSystem dstFS,
                                        Path dst)
                                        throws IOException {
    if (srcFS == dstFS) {
      String srcq = srcFS.makeQualified(src).toString() + Path.SEPARATOR;
      String dstq = dstFS.makeQualified(dst).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new IOException("Cannot copy " + src + " to its subdirectory " +
                                dst);
        }
      }
    }
  }

}
