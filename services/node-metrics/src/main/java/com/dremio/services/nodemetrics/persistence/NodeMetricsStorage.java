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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.dremio.plugins.nodeshistory.NodesHistoryTable;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

/** Used to write and retrieve node metrics files */
class NodeMetricsStorage {
  static final Set<PosixFilePermission> FILE_PERMISSIONS =
      Sets.immutableEnumSet(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);

  private final Provider<NodesHistoryStoreConfig> nodesHistoryStoreConfigProvider;

  NodeMetricsStorage(Provider<NodesHistoryStoreConfig> nodesHistoryStoreConfigProvider) {
    this.nodesHistoryStoreConfigProvider = nodesHistoryStoreConfigProvider;
  }

  void write(InputStream contents, NodeMetricsFile file) throws IOException {
    preWrite();
    Path filePath = getBaseDirectory().resolve(file.getName());
    try (OutputStream output = getFileSystem().create(filePath, false)) {
      IOUtils.copy(contents, output);
    }
  }

  void write(ByteArrayOutputStream contents, NodeMetricsFile file) throws IOException {
    preWrite();
    Path filePath = getBaseDirectory().resolve(file.getName());
    try (OutputStream output = getFileSystem().create(filePath, false)) {
      contents.writeTo(output);
    }
  }

  void delete(NodeMetricsFile file) throws IOException {
    Path path = getBaseDirectory().resolve(file.getName());
    getFileSystem().delete(path, false);
  }

  DirectoryStream<NodeMetricsFile> list() throws IOException {
    return new NodeMetricsDirectoryMultiStream(
        list(CompactionType.Uncompacted), list(CompactionType.Single), list(CompactionType.Double));
  }

  DirectoryStream<NodeMetricsFile> list(CompactionType compactionType) throws IOException {
    Path pattern;
    Predicate<String> nameFilter;
    Path baseDirectory = getBaseDirectory();
    switch (compactionType) {
      case Uncompacted:
        pattern = NodeMetricsPointFile.getGlob(baseDirectory);
        nameFilter = NodeMetricsPointFile::isValid;
        break;
      case Single:
        pattern = NodeMetricsCompactedFile.getGlob(baseDirectory, CompactionType.Single);
        nameFilter = NodeMetricsCompactedFile::isValid;
        break;
      case Double:
        pattern = NodeMetricsCompactedFile.getGlob(baseDirectory, CompactionType.Double);
        nameFilter = NodeMetricsCompactedFile::isValid;
        break;
      default:
        throw new IllegalStateException(
            String.format("Unexpected compaction type: %s", compactionType));
    }
    Predicate<Path> filter = (Path path) -> nameFilter.test(path.getName());
    return new NodeMetricsDirectoryStream(getFileSystem().glob(pattern, filter));
  }

  InputStream open(NodeMetricsFile file) throws IOException {
    Path path = getBaseDirectory().resolve(file.getName());
    return getFileSystem().open(path);
  }

  private void preWrite() throws IOException {
    // This is a no-op if the directory already exists with the desired permissions
    getFileSystem().mkdirs(getBaseDirectory(), FILE_PERMISSIONS);
  }

  private FileSystem getFileSystem() {
    return nodesHistoryStoreConfigProvider.get().getFileSystem();
  }

  private Path getBaseDirectory() {
    return nodesHistoryStoreConfigProvider
        .get()
        .getStoragePath()
        .resolve(NodesHistoryTable.METRICS.getName());
  }

  private static class NodeMetricsDirectoryStream implements DirectoryStream<NodeMetricsFile> {
    private final DirectoryStream<FileAttributes> stream;

    public NodeMetricsDirectoryStream(DirectoryStream<FileAttributes> stream) {
      this.stream = stream;
    }

    @Override
    public @NotNull Iterator<NodeMetricsFile> iterator() {
      Iterator<FileAttributes> it = stream.iterator();
      return new Iterator<>() {

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public NodeMetricsFile next() {
          FileAttributes next = it.next();
          String name = next.getPath().getName();
          if (NodeMetricsPointFile.isValid(name)) {
            return NodeMetricsPointFile.from(name);
          } else if (NodeMetricsCompactedFile.isValid(name)) {
            return NodeMetricsCompactedFile.from(name);
          } else {
            throw new IllegalStateException(
                String.format("File is not a node metrics file: %s", name));
          }
        }
      };
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  private static class NodeMetricsDirectoryMultiStream implements DirectoryStream<NodeMetricsFile> {
    private final DirectoryStream<NodeMetricsFile>[] streams;

    @SafeVarargs
    public NodeMetricsDirectoryMultiStream(DirectoryStream<NodeMetricsFile>... streams) {
      this.streams = streams;
    }

    @Override
    public @NotNull Iterator<NodeMetricsFile> iterator() {
      List<Iterator<NodeMetricsFile>> its =
          Arrays.stream(streams).map(DirectoryStream::iterator).collect(Collectors.toList());
      return new Iterator<>() {
        private int iteratorIdx = 0;

        @Override
        public boolean hasNext() {
          return (iteratorIdx < its.size() && its.get(iteratorIdx).hasNext())
              || (++iteratorIdx < its.size() && its.get(iteratorIdx).hasNext());
        }

        @Override
        public NodeMetricsFile next() {
          return its.get(iteratorIdx).next();
        }
      };
    }

    @Override
    public void close() throws IOException {
      for (DirectoryStream<NodeMetricsFile> stream : streams) {
        stream.close();
      }
    }
  }
}
