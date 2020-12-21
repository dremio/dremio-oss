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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotLinkException;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.io.AsyncByteReader;
import com.dremio.io.AsyncByteReader.FileKey;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.test.DremioTest;
import com.google.common.collect.Iterators;

/**
 * Tests for RecursiveDirectoryStream
 */
public class TestRecursiveDirectoryStream extends DremioTest {

  private static final class MockLocalFileSystem implements FileSystem {
    @Override
    public void close() throws IOException {
    }

    @Override
    public FSInputStream open(Path f) throws FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getScheme() {
      return "file";
    }

    @Override
    public FSOutputStream create(Path f) throws FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSOutputStream create(Path f, boolean overwrite) throws FileAlreadyExistsException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setPermission(Path p, Set<PosixFilePermission> permissions) throws FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
      return list(f, PathFilters.ALL_FILES);
    }

    @Override
    public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter)
        throws FileNotFoundException, IOException {
      final java.nio.file.Path p = Paths.get(f.toURI().getPath());
      final DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(p, path -> filter.test(Path.of(path.toUri())));
      return new DirectoryStream<FileAttributes>() {
        @Override
        public java.util.Iterator<FileAttributes> iterator() {
          return Iterators.transform(stream.iterator(), path -> new FileAttributes() {

            @Override
            public long size() {
              try {
                return Files.size(path);
              } catch (IOException e) {
                throw new IOError(e);
              }
            }

            @Override
            public FileTime lastModifiedTime() {
              try {
                return Files.getLastModifiedTime(path);
              } catch (IOException e) {
                throw new IOError(e);
              }
            }

            @Override
            public FileTime lastAccessTime() {
              throw new UnsupportedOperationException();
            }

            @Override
            public boolean isSymbolicLink() {
              return false;
            }

            @Override
            public boolean isRegularFile() {
              return Files.isRegularFile(path);
            }

            @Override
            public boolean isOther() {
              return false;
            }

            @Override
            public boolean isDirectory() {
              return Files.isDirectory(path);
            }

            @Override
            public Object fileKey() {
              return null;
            }

            @Override
            public FileTime creationTime() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Set<PosixFilePermission> permissions() {
              try {
                return Files.getPosixFilePermissions(path);
              } catch (IOException e) {
                throw new IOError(e);
              }
            }

            @Override
            public UserPrincipal owner() {
              try {
                return Files.getOwner(path);
              } catch (IOException e) {
                throw new IOError(e);
              }
            }

            @Override
            public GroupPrincipal group() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Path getSymbolicLink() throws NotLinkException, IOException {
              return null;
            }

            @Override
            public Path getPath() {
              return Path.of(path.toUri());
            }
          });
        }

        @Override
        public void close() throws IOException {
          stream.close();
        }
      };
    }

    @Override
    public DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter)
        throws FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirectory(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFile(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URI getUri() {
      return URI.create("file:///");
    }

    @Override
    public Path makeQualified(Path path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<FileBlockLocation> getFileBlockLocations(FileAttributes file, long start, long len)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void access(Path path, Set<AccessMode> mode)
        throws AccessControlException, FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPdfs() {
      return false;
    }

    @Override
    public boolean isMapRfs() {
      return false;
    }

    @Override
    public boolean supportsPath(Path path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getDefaultBlockSize(Path path) {
      return -1;
    }

    @Override
    public Path canonicalizePath(Path p) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsAsync() {
      return false;
    }

    @Override
    public AsyncByteReader getAsyncByteReader(FileKey fileKey) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Test
  public void testLazyness() throws Exception {
    for (String dir0: Arrays.asList("A", "B", "C")) {
      temporaryFolder.newFolder(dir0);
      for (String dir1: Arrays.asList("1", "2", "3", "4")) {
        temporaryFolder.newFolder(dir0, dir1);
        for (String file: Arrays.asList("file1")) {
          temporaryFolder.newFile(dir0 + "/" + dir1 + "/" + file);
        }
      }
    }

    final Path rootPath = Path.of(temporaryFolder.getRoot().toURI());

    final FileSystem fs = new MockLocalFileSystem();

    try (final RecursiveDirectoryStream directoryStream = new RecursiveDirectoryStream(fs, fs.list(rootPath), PathFilters.ALL_FILES)) {
      final Iterator<FileAttributes> iteratorWrapper = directoryStream.iterator();
      final Iterable<FileAttributes> iterable = () -> iteratorWrapper;

      {
        @SuppressWarnings("unused")
        List<FileAttributes> ignored = StreamSupport.stream(iterable.spliterator(), false)
            .limit(3)
            .collect(Collectors.toList());
      }

      // stack should be the root, root/A, root/A/1
      assertEquals(3, directoryStream.getStackSize());

      {
        @SuppressWarnings("unused")
        List<FileAttributes> ignored = StreamSupport.stream(iterable.spliterator(), false)
          .limit(24)
          .collect(Collectors.toList());
      }

      // stack should be the root, root/1996, root/1994/Q3
      assertEquals(3, directoryStream.getStackSize());

      // the files should now exhausted
      assertFalse(iteratorWrapper.hasNext());
      assertEquals(0, directoryStream.getStackSize());
    }
  }
}
