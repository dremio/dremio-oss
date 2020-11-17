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
package com.dremio.exec.store.hive.exec.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

import com.dremio.exec.hadoop.HadoopAsyncByteReader;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.OpenFileTracker;
import com.dremio.exec.store.dfs.SimpleFileBlockLocation;
import com.dremio.exec.store.hive.exec.DremioFileSystem;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.exec.util.AssertionUtil;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.FilterFSInputStream;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * FileSystemWrapper is the wrapper around the actual FileSystem implementation.
 *
 * If {@link OperatorStats} are provided it returns an instrumented FSDataInputStream to
 * measure IO wait time and tracking file open/close operations.
 */
public class DremioHadoopFileSystemWrapper
  implements com.dremio.io.file.FileSystem, OpenFileTracker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioHadoopFileSystemWrapper.class);
  private final static boolean TRACKING_ENABLED = AssertionUtil.isAssertionsEnabled();

  private final static DremioFileSystemCache DREMIO_FS_CACHE = new DremioFileSystemCache();

  private static final String FORCE_REFRESH_LEVELS = "dremio.fs.force_refresh_levels";
  private static int FORCE_REFRESH_LEVELS_VALUE = Integer.getInteger(FORCE_REFRESH_LEVELS, 2);

  public static final String HDFS_SCHEME = "hdfs";
  public static final String MAPRFS_SCHEME = "maprfs";
  public static final String NAS_SCHEME = "file";
  public static final String WEBHDFS_SCHEME = "webhdfs";

  private static final String NON_EXISTENT_FILE_SUFFIX = ".___NoFile___._";
  private static long NON_EXISTENT_FILE_COUNTER = 1;

  private final ConcurrentMap<FSInputStream, DebugStackTrace> openedFiles = Maps.newConcurrentMap();

  private final FileSystem underlyingFs;
  final OperatorStats operatorStats;
  private final boolean isMapRfs;
  private final boolean isNAS;
  private final boolean isHDFS;
  private final boolean enableAsync;


  public DremioHadoopFileSystemWrapper(org.apache.hadoop.fs.Path path, Configuration fsConf, OperatorStats operatorStats, boolean enableAsync) throws IOException {
    this(fsConf, path.getFileSystem(fsConf), operatorStats, enableAsync);
  }

  public DremioHadoopFileSystemWrapper(Configuration fsConf, FileSystem fs, OperatorStats operatorStats, boolean enableAsync) {
    this.underlyingFs = fs;
    this.operatorStats = operatorStats;
    this.isMapRfs = isMapRfs(underlyingFs);
    this.isNAS = isNAS(underlyingFs);
    this.isHDFS = isHDFS(underlyingFs);
    this.enableAsync = enableAsync;
    if(operatorStats != null) {
      operatorStats.createMetadataReadIOStats();
    }
  }

  private static boolean isMapRfs(FileSystem fs) {
    try {
      return MAPRFS_SCHEME.equals(fs.getScheme().toLowerCase());
    } catch (UnsupportedOperationException e) {
    }
    return false;
  }

  private static boolean isNAS(FileSystem fs) {
    try {
      return fs instanceof LocalSyncableFileSystem || NAS_SCHEME.equals(fs.getScheme().toLowerCase(Locale.ROOT));
    } catch (UnsupportedOperationException e) {
    }
    return false;
  }

  private static boolean isHDFS(org.apache.hadoop.fs.FileSystem fs) {
    final String scheme = fs.getScheme().toLowerCase(Locale.ROOT);
    try {
      return HDFS_SCHEME.equals(scheme) || WEBHDFS_SCHEME.equals(scheme);
    } catch (UnsupportedOperationException e) {
    }
    return false;
  }

  protected FileSystem getUnderlyingFs() {
    return underlyingFs;
  }

  // See DX-15492
  private void openNonExistentFileInPath(org.apache.hadoop.fs.Path f) throws IOException {
    org.apache.hadoop.fs.Path nonExistentFile = f.suffix(NON_EXISTENT_FILE_SUFFIX + NON_EXISTENT_FILE_COUNTER++);

    try (FSDataInputStream is = underlyingFs.open(nonExistentFile)) {
    } catch (FileNotFoundException fileNotFoundException) {
      return;
    } catch (AccessControlException accessControlException) {
      String errMsg = "Open failed for file: " + f.toString() + " error: Permission denied (13)";
      throw new AccessControlException(errMsg);
    } catch (Exception e) {
      // Re-throw exception
      throw e;
    }
  }

  private void checkAccessAllowedOnPathIfMaprfs(org.apache.hadoop.fs.Path f) throws IOException {
    if (!isMapRfs) {
      return;
    }

    openNonExistentFileInPath(f);
  }

  private FSDataInputStream openFile(Path f) throws IOException {
    org.apache.hadoop.fs.Path p = toHadoopPath(f);
    checkAccessAllowedOnPathIfMaprfs(p);
    return underlyingFs.open(p);
  }

  // See DX-15492
  private void checkAccessAllowed(org.apache.hadoop.fs.Path f, FsAction mode) throws IOException {
    if (!isMapRfs) {
      underlyingFs.access(f, mode);
      return;
    }

    openNonExistentFileInPath(f);
    underlyingFs.access(f, mode);
  }

  /**
   * If OperatorStats are provided return a instrumented {@link FSDataInputStream}.
   */
  @Override
  public FSInputStream open(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      return newFSDataInputStreamWrapper(f, openFile(f));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public String getScheme() {
    return underlyingFs.getScheme();
  }

  @Override
  public FSOutputStream create(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(toHadoopPath(f)), f.toString());
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSOutputStream create(Path f, boolean overwrite) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(toHadoopPath(f), overwrite), f.toString());
    } catch(FSError e) {
      throw propagateFSError(e);
    } catch(FileAlreadyExistsException e) {
      throw new java.nio.file.FileAlreadyExistsException(e.getMessage());
    }
  }

  @Override
  public FileAttributes getFileAttributes(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      final FileStatus result = underlyingFs.getFileStatus(toHadoopPath(f));
      // safe-guarding against misbehaving filesystems
      if (result == null) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      return new HadoopFileStatusWrapper(result);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setPermission(Path p, Set<PosixFilePermission> permissions) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, p)) {
      underlyingFs.setPermission(toHadoopPath(p), toFsPermission(permissions));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clazz) {
    if(clazz.isAssignableFrom(underlyingFs.getClass())) {
      return (T) underlyingFs;
    }

    return null;
  }

  @Override
  public boolean mkdirs(Path f, Set<PosixFilePermission> permissions) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      return underlyingFs.mkdirs(toHadoopPath(f), toFsPermission(permissions));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (TRACKING_ENABLED) {
      if (openedFiles.size() != 0) {
        final StringBuffer errMsgBuilder = new StringBuffer();

        errMsgBuilder.append(String.format("Not all files opened using this FileSystem are closed. " + "There are" +
            " still [%d] files open.\n", openedFiles.size()));

        for (DebugStackTrace stackTrace : openedFiles.values()) {
          stackTrace.addToStringBuilder(errMsgBuilder);
        }

        final String errMsg = errMsgBuilder.toString();
        logger.error(errMsg);
        throw new IllegalStateException(errMsg);
      }
    }
  }

  @Override
  public boolean mkdirs(Path folderPath) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, folderPath)) {
      org.apache.hadoop.fs.Path path = toHadoopPath(folderPath);
      if (!underlyingFs.exists(path)) {
        return underlyingFs.mkdirs(path);
      } else if (!underlyingFs.getFileStatus(path).isDirectory()) {
        throw new IOException("The specified folder path exists and is not a folder.");
      }
      return false;
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      return new ArrayDirectoryStream(underlyingFs.listStatus(toHadoopPath(f)));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> list(Path f, Predicate<Path> filter) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      return new ArrayDirectoryStream(underlyingFs.listStatus(toHadoopPath(f), toPathFilter(filter)));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public DirectoryStream<FileAttributes> glob(Path pattern, Predicate<Path> filter)
      throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, pattern)) {
      return new ArrayDirectoryStream(underlyingFs.globStatus(toHadoopPath(pattern), toPathFilter(filter)));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, dst)) {
      return underlyingFs.rename(toHadoopPath(src), toHadoopPath(dst));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.delete(toHadoopPath(f), recursive);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    final org.apache.hadoop.fs.Path p = toHadoopPath(f);
    boolean exists = false;
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      exists = underlyingFs.exists(p);
      if (!exists && isNAS) {
        forceRefresh(f);
        exists = underlyingFs.exists(p);
      }
    } catch(FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    final org.apache.hadoop.fs.Path p = toHadoopPath(f);
    boolean exists = false;
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      exists = underlyingFs.isDirectory(p);
      if (!exists && isNAS) {
        forceRefresh(f);
        exists = underlyingFs.isDirectory(p);
      }
    } catch(FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    final org.apache.hadoop.fs.Path p = toHadoopPath(f);
    boolean exists = false;
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, f)) {
      exists = underlyingFs.isFile(p);
      if (!exists && isNAS) {
        forceRefresh(f);
        exists = underlyingFs.isFile(p);
      }
    } catch(FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  public URI getUri() {
    return underlyingFs.getUri();
  }

  @Override
  public Path makeQualified(Path path) {
    return fromHadoopPath(underlyingFs.makeQualified(toHadoopPath(path)));
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(FileAttributes file, long start, long len) throws IOException {
    if (!(file instanceof HadoopFileStatusWrapper)) {
      throw new ProviderMismatchException();
    }
    final FileStatus status = ((HadoopFileStatusWrapper) file).getFileStatus();
    Path p = status == null ? null : file.getPath();
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, p)) {
      return toFileBlockLocations(() -> underlyingFs.getFileBlockLocations(status, start, len));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Iterable<FileBlockLocation> getFileBlockLocations(Path p, long start, long len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, p)) {
      return toFileBlockLocations(() -> underlyingFs.getFileBlockLocations(toHadoopPath(p), start, len));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void access(final Path path, final Set<AccessMode> mode) throws AccessControlException, FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getMetadataWaitRecorder(operatorStats, path)) {
      checkAccessAllowed(toHadoopPath(path), toFsAction(mode));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  private void forceRefresh(Path f) {
    /*
      In some cases, especially for NFS, the directory lookup is from the cache. So, a
      new file/directory created in another client may not be seen by this client.
      Now, NFS must adhere to close-to-open consistency.  Hence opening is a way to force
      a refresh of the attribute cache.

      This uses Java File APIs directly.
     */
    java.nio.file.Path p = Paths.get(f.toString());

    /*
      n level of directories may be created in the executor. To refresh, the base directory
      the coordinator is aware of needs to be refreshed. The default level is 2.
     */
    for (int i = 0; i < FORCE_REFRESH_LEVELS_VALUE; i++) {
      p = p.getParent();
      if (p == null) {
        return;
      }
      /*
        Need to use a call that would cause the trigger of a directory refresh.
        Checking isFile() or isDirectory() does not refresh the NFS directory cache.
        Opening the file/directory also does not also refresh the NFS diretory cache.
        Parent of a file or directory is always a directory. Attempting to open  a directory
        already known to the client refreshes the directory tree.
      */
      try (DirectoryStream<java.nio.file.Path> ignore = Files.newDirectoryStream(p)) {
        return; //return if there is no exception, i.e. it was found
      } catch (IOException e) {
        logger.trace("Refresh generated exception: {}", e);
      }
    }
  }

  @Override
  public boolean isPdfs() {
    return false;
  }

  @Override
  public boolean isMapRfs() {
    return isMapRfs;
  }

  @Override
  public boolean supportsPath(Path path) {
    try {
      underlyingFs.makeQualified(toHadoopPath(path));
      return true;
    } catch (IllegalArgumentException ie) {
      return false;
    }
  }

  @Override
  public long getDefaultBlockSize(Path path) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Path canonicalizePath(Path p) throws IOException {
    return p;
  }

  @Override
  public void fileOpened(Path path, FSInputStream fsDataInputStream) {
    openedFiles.put(fsDataInputStream, new DebugStackTrace(path, Thread.currentThread().getStackTrace()));
  }

  @Override
  public void fileClosed(FSInputStream fsDataInputStream) {
    openedFiles.remove(fsDataInputStream);
  }

  @Override
  public boolean supportsAsync() {
    if (!enableAsync) {
      return false;
    }

    if (underlyingFs instanceof MayProvideAsyncStream) {
      return ((MayProvideAsyncStream) underlyingFs).supportsAsync();
    } else if (isHDFS) {
      // will use wrapper to emulate async APIs.
      return true;
    } else if (underlyingFs instanceof DremioFileSystem) {
      return ((DremioFileSystem) underlyingFs).supportsAsync();
    } else {
      return false;
    }
  }

  @Override
  public AsyncByteReader getAsyncByteReader(AsyncByteReader.FileKey fileKey) throws IOException {

    final org.apache.hadoop.fs.Path path = toHadoopPath(fileKey.getPath());
    if (underlyingFs instanceof MayProvideAsyncStream) {
      return ((MayProvideAsyncStream) underlyingFs).getAsyncByteReader(path, fileKey.getVersion());
    } else if (underlyingFs instanceof DremioFileSystem) {
      return ((DremioFileSystem) underlyingFs).getAsyncByteReader(fileKey, operatorStats);
    } else {
      throw new UnsupportedOperationException("Unsupported path in Hive Parquet readers");
    }
  }

  private static final class ArrayDirectoryStream implements DirectoryStream<FileAttributes> {
    private final List<FileAttributes> delegate;

    private ArrayDirectoryStream(FileStatus[] statuses) {
      delegate = statuses != null
          ? ImmutableList.copyOf(Iterables.transform(Arrays.asList(statuses), HadoopFileStatusWrapper::new))
          : Collections.emptyList();
    }

    @Override
    public Iterator<FileAttributes> iterator() {
      return delegate.iterator();
    }

    @Override
    public void close() throws IOException {
    }
  }

  public static class DebugStackTrace {
    final private StackTraceElement[] elements;
    final private Path path;

    public DebugStackTrace(Path path, StackTraceElement[] elements) {
      this.path = path;
      this.elements = elements;
    }

    public void addToStringBuilder(StringBuffer sb) {
      sb.append("File '");
      sb.append(path.toString());
      sb.append("' opened at callstack:\n");

      // add all stack elements except the top three as they point to FileSystemWrapper.open() and inner stack elements.
      for (int i = 3; i < elements.length; i++) {
        sb.append("\t");
        sb.append(elements[i]);
        sb.append("\n");
      }
      sb.append("\n");
    }
  }

  FSInputStream newFSDataInputStreamWrapper(Path f, final FSDataInputStream is) throws IOException {
    try {
      return (operatorStats != null) ?
        com.dremio.exec.store.hive.exec.dfs.FSDataInputStreamWithStatsWrapper.of(is, operatorStats, true, f.toString()) :
        com.dremio.exec.store.hive.exec.dfs.FSDataInputStreamWrapper.of(is);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  FSOutputStream newFSDataOutputStreamWrapper(final FSDataOutputStream os, String filePath) throws IOException {
    try {
      return (operatorStats != null) ?
        new com.dremio.exec.store.hive.exec.dfs.FSDataOutputStreamWithStatsWrapper(os, operatorStats, filePath) :
        new com.dremio.exec.store.hive.exec.dfs.FSDataOutputStreamWrapper(os);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  FSInputStream newFSDataInputStreamWrapper(Path f, final FSDataInputStream is, boolean recordWaitTime) throws IOException {
    FSInputStream result;
    if (operatorStats != null) {
      result = com.dremio.exec.store.hive.exec.dfs.FSDataInputStreamWithStatsWrapper.of(is, operatorStats, recordWaitTime, f.toString());
    } else {
      result = com.dremio.exec.store.hive.exec.dfs.FSDataInputStreamWrapper.of(is);
    }
    if (TRACKING_ENABLED) {
      result = new FilterFSInputStream(result) {
        @Override
        public void close() throws IOException {
          fileClosed(this);
          super.close();
        }
      };
      fileOpened(f, result);
    }
    return result;
  }

  @VisibleForTesting
  static FsAction toFsAction(Set<AccessMode> mode) {
    final char[] perms = new char[] { '-', '-', '-'};
    for(AccessMode m: mode) {
      switch(m) {
      case READ:
        perms[0] = 'r';
        break;

      case WRITE:
        perms[1] = 'w';
        break;

      case EXECUTE:
        perms[2] = 'x';
        break;
      }
    }
    return FsAction.getFsAction(new String(perms));
  }

  private static Path fromHadoopPath(org.apache.hadoop.fs.Path path) {
    return Path.of(path.toUri());
  }

  private static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toURI());
  }

  private static PathFilter toPathFilter(Predicate<Path> predicate) {
    return p -> predicate.test(fromHadoopPath(p));
  }

  @VisibleForTesting
  static FsPermission toFsPermission(Set<PosixFilePermission> permissions) {
    return FsPermission.valueOf("-" + PosixFilePermissions.toString(permissions));
  }

  public static IOException propagateFSError(FSError e) throws IOException {
    Throwables.propagateIfPossible(e.getCause(), IOException.class);
    return new IOException("Unexpected FSError", e);
  }

  @FunctionalInterface
  private interface IOCallable<V> {
    V call() throws IOException;
  }

  private static Iterable<FileBlockLocation> toFileBlockLocations(IOCallable<BlockLocation[]> call) throws IOException {
    final BlockLocation[] blocks = call.call();
    final List<FileBlockLocation> results = new ArrayList<>(blocks.length);
    for(BlockLocation block: blocks) {
      results.add(new SimpleFileBlockLocation(block.getOffset(), block.getLength(), ImmutableList.copyOf(block.getHosts())));
    }
    return results;
  }
}
