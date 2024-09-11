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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class TestNodeMetricsStorage {
  private static final Path testPath = Path.of("/a/b");
  private static final Path testPathMetrics = testPath.resolve("metrics");
  // For these, we need to manually create the filename to ensure they are unique across the
  // multiple node metrics files of that compaction type, since the name is based on
  // second-precision and would otherwise very likely be duplicated across the files
  private static final NodeMetricsFile uncompacted =
      NodeMetricsPointFile.from(TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(1)));
  private static final NodeMetricsFile uncompacted2 =
      NodeMetricsPointFile.from(TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(2)));
  private static final NodeMetricsFile compacted =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(1), CompactionType.Single));
  private static final NodeMetricsFile compacted2 =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(2), CompactionType.Single));
  private static final NodeMetricsFile recompacted =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(1), CompactionType.Double));
  private static final NodeMetricsFile recompacted2 =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(2), CompactionType.Double));

  @Test
  public void testWrite_InputStream() throws IOException {
    String input = "hello world";
    try (OutputStream outputStream = new OutputStream()) {
      FileSystem fs = mock(FileSystem.class);
      when(fs.create(testPathMetrics.resolve(uncompacted.getName()), false))
          .thenReturn(outputStream);
      NodesHistoryStoreConfig config = new NodesHistoryStoreConfig(testPath, fs);
      NodeMetricsStorage nodeMetricsStorage = new NodeMetricsStorage(() -> config);

      nodeMetricsStorage.write(IOUtils.toInputStream(input, StandardCharsets.UTF_8), uncompacted);

      verify(fs, times(1)).mkdirs(testPathMetrics, NodeMetricsStorage.FILE_PERMISSIONS);
      Assertions.assertArrayEquals(
          input.getBytes(StandardCharsets.UTF_8),
          outputStream.asByteArrayOutputStream().toByteArray());
    }
  }

  @Test
  public void testWrite_ByteArray() throws IOException {
    String input = "hello world";
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    IOUtils.copy(IOUtils.toInputStream(input, StandardCharsets.UTF_8), stream);

    try (OutputStream outputStream = new OutputStream()) {
      FileSystem fs = mock(FileSystem.class);
      when(fs.create(testPathMetrics.resolve(uncompacted.getName()), false))
          .thenReturn(outputStream);
      NodesHistoryStoreConfig config = new NodesHistoryStoreConfig(testPath, fs);
      NodeMetricsStorage nodeMetricsStorage = new NodeMetricsStorage(() -> config);

      nodeMetricsStorage.write(stream, uncompacted);

      verify(fs, times(1)).mkdirs(testPathMetrics, NodeMetricsStorage.FILE_PERMISSIONS);
      Assertions.assertArrayEquals(
          input.getBytes(StandardCharsets.UTF_8),
          outputStream.asByteArrayOutputStream().toByteArray());
    }
  }

  @Test
  public void testDelete() throws IOException {
    FileSystem fs = mock(FileSystem.class);
    NodesHistoryStoreConfig config = new NodesHistoryStoreConfig(testPath, fs);
    NodeMetricsStorage nodeMetricsStorage = new NodeMetricsStorage(() -> config);

    nodeMetricsStorage.delete(uncompacted);

    verify(fs, times(1)).delete(testPathMetrics.resolve(uncompacted.getName()), false);
  }

  private static Stream<Arguments> listArgsProvider() {
    return Stream.of(
        Arguments.of(
            Map.of(
                CompactionType.Uncompacted,
                List.of(uncompacted, uncompacted2),
                CompactionType.Single,
                List.of(compacted, compacted2),
                CompactionType.Double,
                List.of(recompacted, recompacted2))),
        Arguments.of(Map.of(CompactionType.Uncompacted, List.of(uncompacted, uncompacted2))),
        Arguments.of(Map.of(CompactionType.Single, List.of(compacted, compacted2))),
        Arguments.of(Map.of(CompactionType.Double, List.of(recompacted, recompacted2))));
  }

  @ParameterizedTest
  @MethodSource("listArgsProvider")
  void list(Map<CompactionType, List<NodeMetricsFile>> files) throws IOException {
    Preconditions.checkArgument(
        files.keySet().size() == 1 || files.keySet().containsAll(List.of(CompactionType.values())));

    FileSystem fs = mock(FileSystem.class);
    Set<CompactionType> compactionTypes = files.keySet();
    Map<CompactionType, ArgumentCaptor<Predicate<Path>>> predicateArgumentCaptors = new HashMap<>();
    for (CompactionType compactionType : compactionTypes) {
      String prefix = getFilePrefix(compactionType);
      Path expectedGlob =
          testPathMetrics.resolve(
              String.format(
                  "%s[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.csv",
                  prefix));
      ArgumentCaptor<Predicate<Path>> predicateArgumentCaptor =
          ArgumentCaptor.forClass(Predicate.class);
      predicateArgumentCaptors.put(compactionType, predicateArgumentCaptor);
      when(fs.glob(eq(expectedGlob), predicateArgumentCaptor.capture()))
          .thenReturn(
              new DirectoryStream<>() {
                @Override
                public @NotNull Iterator<FileAttributes> iterator() {
                  return files.get(compactionType).stream()
                      .map(TestNodeMetricsStorage.this::getTestFileAttributes)
                      .iterator();
                }

                @Override
                public void close() {}
              });
    }
    NodesHistoryStoreConfig config = new NodesHistoryStoreConfig(testPath, fs);
    NodeMetricsStorage nodeMetricsStorage = new NodeMetricsStorage(() -> config);

    DirectoryStream<NodeMetricsFile> dirStream;
    if (compactionTypes.size() > 1) {
      dirStream = nodeMetricsStorage.list();
    } else {
      dirStream = nodeMetricsStorage.list(compactionTypes.stream().findAny().get());
    }
    List<NodeMetricsFile> actual = IteratorUtils.toList(dirStream.iterator());
    Collections.sort(actual);

    List<NodeMetricsFile> expected =
        files.values().stream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
    Assertions.assertEquals(expected, actual);

    for (CompactionType compactionType : compactionTypes) {
      Predicate<Path> capturedPredicate = predicateArgumentCaptors.get(compactionType).getValue();
      Assertions.assertTrue(
          files.get(compactionType).stream()
              .allMatch(
                  file -> {
                    Path path = testPathMetrics.resolve(file.getName());
                    return capturedPredicate.test(path);
                  }));
    }
  }

  private String getFilePrefix(CompactionType compactionType) {
    switch (compactionType) {
      case Uncompacted:
        return "";
      case Single:
        return "s_";
      case Double:
        return "d_";
      default:
        throw new IllegalStateException();
    }
  }

  private FileAttributes getTestFileAttributes(NodeMetricsFile nodeMetricsFile) {
    return new FileAttributes() {
      @Override
      public Path getPath() {
        return testPathMetrics.resolve(nodeMetricsFile.getName());
      }

      @Override
      public Path getSymbolicLink() {
        return null;
      }

      @Override
      public UserPrincipal owner() {
        return null;
      }

      @Override
      public GroupPrincipal group() {
        return null;
      }

      @Override
      public Set<PosixFilePermission> permissions() {
        return null;
      }

      @Override
      public FileTime lastModifiedTime() {
        return null;
      }

      @Override
      public FileTime lastAccessTime() {
        return null;
      }

      @Override
      public FileTime creationTime() {
        return null;
      }

      @Override
      public boolean isRegularFile() {
        return false;
      }

      @Override
      public boolean isDirectory() {
        return false;
      }

      @Override
      public boolean isSymbolicLink() {
        return false;
      }

      @Override
      public boolean isOther() {
        return false;
      }

      @Override
      public long size() {
        return 0;
      }

      @Override
      public Object fileKey() {
        return null;
      }
    };
  }

  private static class OutputStream extends FSOutputStream {
    private final ByteArrayOutputStream wrappedStream;

    OutputStream() {
      this.wrappedStream = new ByteArrayOutputStream();
    }

    @Override
    public void write(int b) {
      wrappedStream.write(b);
    }

    @Override
    public long getPosition() {
      throw new NotSupportedException();
    }

    ByteArrayOutputStream asByteArrayOutputStream() {
      return wrappedStream;
    }
  }
}
