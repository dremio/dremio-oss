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

package com.dremio.exec.store.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.security.AccessControlException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ExecConstants;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.FilterFSInputStream;
import com.dremio.io.FilterFSOutputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FilterFileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionResolver;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;


/**
 * A {@link FileSystem} implementation which logs calls to a wrapped FileSystem.
 *
 * <p> Logging is done at three levels:
 * <ul>
 *   <li>WARN  - Calls that are >= the configured duration in the
 *   {@code filesystem.logger.warn.io_threshold_ms} support key.</li>
 *   <li>DEBUG - Calls that are >= the configured duration in the
 *   {@code filesystem.logger.debug.io_threshold_ms} support key.</li>
 *   <li>TRACE - All other calls are logged at TRACE level.  WARNING: This can be very verbose.</li>
 * </ul>
 *
 * <p>Logging can be disabled entirely by setting the log level for {@code com.dremio.exec.store.dfs.LoggedFileSystem}
 * to ERROR.
 */
public class LoggedFileSystem extends FilterFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(LoggedFileSystem.class);

  private final FileSystem fs;
  private final long warnThresholdMs;
  private final long debugThresholdMs;

  public LoggedFileSystem(FileSystem fs, OptionResolver options) {
    super(fs);
    this.fs = fs;
    this.warnThresholdMs = options.getOption(ExecConstants.FS_LOGGER_WARN_THRESHOLD_MS);
    this.debugThresholdMs = options.getOption(ExecConstants.FS_LOGGER_DEBUG_THRESHOLD_MS);
  }

  public static boolean isLoggingEnabled() {
    return LOG.isWarnEnabled();
  }

  @Override
  public FSInputStream open(Path f) throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("open", f)) {
      return new LoggedFSInputStream(super.open(f), f);
    }
  }

  @Override
  public FSOutputStream create(Path f) throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("create", f)) {
      return new LoggedFSOutputStream(super.create(f), f);
    }
  }

  @Override
  public FSOutputStream create(Path f, boolean overwrite) throws FileAlreadyExistsException, IOException {
    try (AutoLogger ignored = logDuration("create", f)) {
      return new LoggedFSOutputStream(super.create(f, overwrite), f);
    }
  }

  @Override
  public FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("getFileAttributes", f)) {
      return super.getFileAttributes(f);
    }
  }

  @Override
  public void setPermission(Path p, Set<PosixFilePermission> permissions) throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("setPermission", p)) {
      super.setPermission(p, permissions);
    }
  }

  @Override
  public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
    try (AutoLogger ignored = logDuration("mkdirs", f)) {
      return super.mkdirs(f, permissions);
    }
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    try (AutoLogger ignored = logDuration("mkdirs", f)) {
      return super.mkdirs(f);
    }
  }

  @Override
  public AsyncByteReader getAsyncByteReader(AsyncByteReader.FileKey fileKey, Map<String, String> options) throws IOException {
    return new LoggedAsyncByteReader(super.getAsyncByteReader(fileKey,options), fileKey.getPath());
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("list", f)) {
      return new LoggedDirectoryStream<>(super.list(f), "list", f);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("list", f)) {
      return new LoggedDirectoryStream<>(super.list(f, filter), "list", f);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("listFiles", f)) {
      return new LoggedDirectoryStream<>(super.listFiles(f, recursive), "listFiles", f);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("glob", pattern)) {
      return new LoggedDirectoryStream<>(super.glob(pattern, filter), "glob", pattern);
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try (AutoLogger ignored = logDuration("rename", src)) {
      return super.rename(src, dst);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try (AutoLogger ignored = logDuration("delete", f)) {
      return super.delete(f, recursive);
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try (AutoLogger ignored = logDuration("exists", f)) {
      return super.exists(f);
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    try (AutoLogger ignored = logDuration("isDirectory", f)) {
      return super.isDirectory(f);
    }
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    try (AutoLogger ignored = logDuration("isFile", f)) {
      return super.isFile(f);
    }
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(FileAttributes file, long start, long len)
      throws IOException {
    try (AutoLogger ignored = logDuration("getFileBlockLocations", file.getPath())) {
      return super.getFileBlockLocations(file, start, len);
    }
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len) throws IOException {
    try (AutoLogger ignored = logDuration("getFileBlockLocations", p)) {
      return super.getFileBlockLocations(p, start, len);
    }
  }

  @Override
  public void access(Path path, Set<AccessMode> mode)
      throws AccessControlException, FileNotFoundException, IOException {
    try (AutoLogger ignored = logDuration("access", path)) {
      super.access(path, mode);
    }
  }

  private AutoLogger logDuration(String op, Path path) {
    return new AutoLogger(op, path);
  }

  private class LoggedFSInputStream extends FilterFSInputStream {

    private final Path path;

    public LoggedFSInputStream(FSInputStream stream, Path path) {
      super(stream);
      this.path = path;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      int nbytes = -1;
      Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        nbytes = super.read(dst);
      } finally {
        logRead(stopwatch.elapsed(TimeUnit.MILLISECONDS), path, nbytes);
      }

      return nbytes;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int nbytes = -1;
      Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        nbytes = super.read(b, off, len);
      } finally {
        logRead(stopwatch.elapsed(TimeUnit.MILLISECONDS), path, nbytes);
      }

      return nbytes;
    }

    @Override
    public void close() throws IOException {
      try (AutoLogger ignored = logDuration("close", path)) {
        super.close();
      }
    }

    private void logRead(long elapsed, Path path, int nbytes) {
      if (elapsed >= warnThresholdMs) {
        LOG.warn("read elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      } else if (elapsed >= debugThresholdMs) {
        LOG.debug("read elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      } else if (nbytes > 0) {
        LOG.trace("read elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      }
    }
  }

  private class LoggedFSOutputStream extends FilterFSOutputStream {

    private final Path path;

    public LoggedFSOutputStream(FSOutputStream stream, Path path) {
      super(stream);
      this.path = path;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        super.write(b, off, len);
      } finally {
        logWrite(stopwatch.elapsed(TimeUnit.MILLISECONDS), path, len);
      }
    }

    @Override
    public void flush() throws IOException {
      try (AutoLogger ignored = logDuration("flush", path)) {
        super.flush();
      }
    }

    @Override
    public void close() throws IOException {
      try (AutoLogger ignored = logDuration("close", path)) {
        super.close();
      }
    }

    private void logWrite(long elapsed, Path path, int nbytes) {
      if (elapsed >= warnThresholdMs) {
        LOG.warn("write elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      } else if (elapsed >= debugThresholdMs) {
        LOG.debug("write elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      } else if (nbytes > 0) {
        LOG.trace("write elapsed={}ms scheme={} path={} nbytes={}", elapsed, fs.getScheme(), path, nbytes);
      }
    }
  }

  private class LoggedAsyncByteReader implements AsyncByteReader {

    private final AsyncByteReader reader;
    private final Path path;

    public LoggedAsyncByteReader(AsyncByteReader reader, Path path) {
      this.reader = reader;
      this.path = path;
    }

    @Override
    public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      LOG.trace("asyncRead.start scheme={} path={} offset={} nbytes={}", fs.getScheme(), path, offset, len);
      return reader.readFully(offset, dst, dstOffset, len)
          .whenComplete((result, throwable) -> logAsyncRead(throwable, stopwatch, offset, len));
    }

    @Override
    public CompletableFuture<Void> checkVersion(String version) {
      // for most implementations this is a no-op - skip logging for now
      return reader.checkVersion(version);
    }

    @Override
    public CompletableFuture<Void> versionedReadFully(String version, long offset, ByteBuf dst, int dstOffset,
        int len) {
      // no logging here as default implementation calls checkVersion and readFully
      return reader.versionedReadFully(version, offset, dst, dstOffset, len);
    }

    @Override
    public CompletableFuture<byte[]> readFully(long offset, int len) {
      // no logging here as default implementation calls ByteBuf-based readFully
      return reader.readFully(offset, len);
    }

    @Override
    public List<ReaderStat> getStats() {
      return reader.getStats();
    }

    @Override
    public void close() throws Exception {
      try (AutoLogger ignored = logDuration("close", path)) {
        reader.close();
      }
    }

    private void logAsyncRead(Throwable throwable, Stopwatch stopwatch, long offset, int nbytes) {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      String state = throwable == null ? "complete" : "failed";
      if (elapsed >= warnThresholdMs) {
        LOG.warn("asyncRead.{} elapsed={}ms scheme={} path={} offset={} nbytes={}", state, elapsed, fs.getScheme(),
            path, offset, nbytes);
      } else if (elapsed >= debugThresholdMs) {
        LOG.debug("asyncRead.{} elapsed={}ms scheme={} path={} offset={} nbytes={}", state, elapsed, fs.getScheme(),
            path, offset, nbytes);
      } else if (nbytes > 0) {
        LOG.trace("asyncRead.{} elapsed={}ms scheme={} path={} offset={} nbytes={}", state, elapsed, fs.getScheme(),
            path, offset, nbytes);
      }
    }
  }

  private class LoggedDirectoryStream<T> implements DirectoryStream<T> {

    private final DirectoryStream<T> stream;
    private final String parentOp;
    private final Path path;

    public LoggedDirectoryStream(DirectoryStream<T> stream, String parentOp, Path path) {
      this.stream = stream;
      this.parentOp = parentOp;
      this.path = path;
    }

    @Override
    public Iterator<T> iterator() {
      Iterator<T> iterator = stream.iterator();
      return new Iterator<T>() {

        @Override
        public boolean hasNext() {
          try (AutoLogger ignored = logDuration(".hasNext")) {
            return iterator.hasNext();
          }
        }

        @Override
        public T next() {
          try (AutoLogger ignored = logDuration(".next")) {
            return iterator.next();
          }
        }
      };
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }

    private AutoLogger logDuration(String op) {
      return new AutoLogger(parentOp + op, path);
    }
  }

  private class AutoLogger implements AutoCloseable {

    private final String op;
    private final Path path;
    private final Stopwatch stopwatch;


    public AutoLogger(String op, Path path) {
      this.op = op;
      this.path = path;
      this.stopwatch = Stopwatch.createStarted();
    }

    @Override
    public void close() {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      log(op, elapsed, path);
    }
  }

  private void log(String op, long elapsed, Path path) {
    if (elapsed >= warnThresholdMs) {
      LOG.warn("{} elapsed={}ms scheme={} path={}", op, elapsed, fs.getScheme(), path);
    } else if (elapsed >= debugThresholdMs) {
      LOG.debug("{} elapsed={}ms scheme={} path={}", op, elapsed, fs.getScheme(), path);
    } else {
      LOG.trace("{} elapsed={}ms scheme={} path={}", op, elapsed, fs.getScheme(), path);
    }
  }
}
