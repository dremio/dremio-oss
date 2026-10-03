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
package com.dremio.plugins.icebergcatalog.dfs;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.BoostedFileSystem;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorStats;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link com.dremio.io.file.FileSystem} instance which in turn wraps a {@link
 * org.apache.hadoop.fs.FileSystem} instance. The Hadoop FS is served from a cache which is the only
 * place where validity of such instances is ensured (e.g. FS not closed, valid credentials, etc...)
 *
 * <p>This class ensures that the underlying Hadoop FS will always be valid for any FS ops to be
 * carried out. It will sync with the cache to see if an updated instance is available (based on
 * {@link DatasetFileSystemCacheKey}) and will swap the enwrapped instance with a new one if so.
 *
 * <p>FS instances are prevented from being closed by a reference counter.
 */
public class SelfManagingCachedFileSystem implements FileSystem {

  private static final Logger logger = LoggerFactory.getLogger(SelfManagingCachedFileSystem.class);

  static final int MAX_LOCK_RETRIES = 10;

  private final DatasetFileSystemCacheKey key;
  private final LoadingCache<DatasetFileSystemCacheKey, LockableHadoopFileSystem> cache;
  private final boolean isAsyncEnabled;
  private final OperatorStats stats;
  private final boolean optimisticLocking;
  private FileSystem wrappedDremioFs;
  private LockableHadoopFileSystem lockableFs;

  SelfManagingCachedFileSystem(
      DatasetFileSystemCacheKey key,
      LoadingCache<DatasetFileSystemCacheKey, LockableHadoopFileSystem> cache,
      OperatorStats stats,
      boolean isAsyncEnabled,
      boolean optimisticLocking) {
    this.key = key;
    this.cache = cache;
    this.stats = stats;
    this.isAsyncEnabled = isAsyncEnabled;
    this.optimisticLocking = optimisticLocking;
  }

  private synchronized LockableHadoopFileSystem ensureLockedFs() {
    // lookup latest instance from cache
    LockableHadoopFileSystem freshLockableFs = cache.get(key);
    int retries = 0;

    while (true) {
      // lock it for use
      freshLockableFs.lockForUse();
      LockableHadoopFileSystem fs;

      if (optimisticLocking) {
        // assume the lock() invocation came fast enough after cache.get()
        // so that the cached entry was not invalidated meanwhile
        logger.debug("{}: Using optimistic locking, only recommended for troubleshooting.", this);
        break;
      }

      try {
        // lookup again
        fs = cache.get(key);
      } catch (Throwable t) {
        freshLockableFs.unlock();
        throw t;
      }

      if (fs == freshLockableFs) {
        // matching instances mean that we acquired lock on the latest FS instance
        logger.debug("{}: Acquired lock on the most recent FS instance {}.", this, fs);
        break;
      } else {
        // mismatching instances mean that we were unlucky and are likely
        // keeping a lock on the invalidated FS instance - unlock and try again
        logger.debug(
            "{}: Acquired lock on an outdated FS instance {}. Newer instance observed: {}. "
                + "Releasing and retrying.",
            this,
            freshLockableFs,
            fs);
        freshLockableFs.unlock();
        freshLockableFs = fs;
        if (++retries > MAX_LOCK_RETRIES) {
          throw UserException.ioExceptionError(
                  new IOException(
                      "Unable to acquire lock on freshly produced FS instance after "
                          + retries
                          + " attempts"))
              .buildSilently();
        }
      }
    }

    // by now we have ensured to have lock for use on recent, working FS instance
    // check and update the wrapped instance if needed
    if (lockableFs == null || freshLockableFs.getFs() != lockableFs.getFs()) {
      lockableFs = freshLockableFs;
      wrappedDremioFs = HadoopFileSystem.get(freshLockableFs.getFs(), stats, isAsyncEnabled);
      logger.debug("{}: Swapped to new FS.", this);
    } else {
      logger.debug("{}: Kept existing FS instance.", this);
    }
    return lockableFs;
  }

  @VisibleForTesting
  void eagerInitialize() {
    try {
      ensureLockedFs();
    } finally {
      if (lockableFs != null) {
        lockableFs.unlock();
      }
    }
  }

  @VisibleForTesting
  FileSystem getWrappedDremioFs() {
    return wrappedDremioFs;
  }

  @VisibleForTesting
  LockableHadoopFileSystem getWrappedLockableFs() {
    return lockableFs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Integer.toHexString(System.identityHashCode(this)));
    sb.append("/key=");
    sb.append(key);
    sb.append(",fs=");
    sb.append(lockableFs);
    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    // close is eventually called by cache upon object removal
    cache.invalidate(key);
  }

  @Override
  public FSInputStream open(Path f) throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.open(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public String getScheme() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getScheme();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public FSOutputStream create(Path f) throws FileNotFoundException, IOException {
    return new ManagedOutputStream(f);
  }

  @Override
  public FSOutputStream create(Path f, boolean overwrite)
      throws FileAlreadyExistsException, IOException {
    return new ManagedOutputStream(f, overwrite);
  }

  @Override
  public FileAttributes getFileAttributes(Path f) throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getFileAttributes(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public void setPermission(Path p, Set<PosixFilePermission> permissions)
      throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      wrappedDremioFs.setPermission(p, permissions);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.unwrap(clazz);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.mkdirs(f, permissions);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.mkdirs(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.list(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.list(f, filter);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public DirectoryStream<FileAttributes> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.listFiles(f, recursive);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.glob(pattern, filter);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.rename(src, dst);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.delete(f, recursive);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.exists(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.isDirectory(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.isFile(f);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public URI getUri() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getUri();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public Path makeQualified(Path path) {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.makeQualified(path);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(
      FileAttributes file, long start, long len) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getFileBlockLocations(file, start, len);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getFileBlockLocations(p, start, len);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public void access(Path path, Set<AccessMode> mode)
      throws AccessControlException, FileNotFoundException, IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      wrappedDremioFs.access(path, mode);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean isPdfs() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.isPdfs();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean isMapRfs() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.isMapRfs();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean supportsBlockAffinity() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.supportsBlockAffinity();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean supportsPath(Path path) {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.supportsPath(path);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public long getDefaultBlockSize(Path path) {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getDefaultBlockSize(path);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public Path canonicalizePath(Path p) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.canonicalizePath(p);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean supportsAsync() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.supportsAsync();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public AsyncByteReader getAsyncByteReader(
      AsyncByteReader.FileKey fileKey, Map<String, String> options) throws IOException {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getAsyncByteReader(fileKey, options);
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean preserveBlockLocationsOrder() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.preserveBlockLocationsOrder();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean supportsBoosting() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.supportsBoosting();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public BoostedFileSystem getBoostedFilesystem() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.getBoostedFilesystem();
    } finally {
      fs.unlock();
    }
  }

  @Override
  public boolean supportsPathsWithScheme() {
    LockableHadoopFileSystem fs = ensureLockedFs();
    try {
      return wrappedDremioFs.supportsPathsWithScheme();
    } finally {
      fs.unlock();
    }
  }

  /**
   * Wraps an output stream and ensures that the FS instance's reference counter is only decremented
   * when the stream has been closed. This prevents the FS from being closed by other threads.
   */
  class ManagedOutputStream extends FSOutputStream {

    private final FSOutputStream outputStream;
    private final LockableHadoopFileSystem fs;

    // Workaround PartitionStatsMetadataUtil#writeMetadata doubly closing its outputstream
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    ManagedOutputStream(Path f, boolean overwrite) throws IOException {
      this.fs = ensureLockedFs();
      this.outputStream = wrappedDremioFs.create(f, overwrite);
    }

    ManagedOutputStream(Path f) throws IOException {
      this(f, true);
    }

    @Override
    public long getPosition() throws IOException {
      return outputStream.getPosition();
    }

    @Override
    public void write(int b) throws IOException {
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      // Claim the close atomically: a waiter woken by unlock() may call close() concurrently, and
      // unlocking twice would drive the FS reference count negative.
      if (isClosed.compareAndSet(false, true)) {
        try {
          outputStream.close();
        } finally {
          fs.unlock();
        }
      }
    }
  }
}
