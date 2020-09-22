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
package com.dremio.exec.store.hive.exec.apache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.context.OperatorStats.WaitRecorder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

/**
 * HadoopFileSystemWrapper is the wrapper around the actual FileSystem implementation.
 *
 * If {@link com.dremio.sabot.exec.context.OperatorStats} are provided it returns an instrumented FSDataInputStream to
 * measure IO wait time and tracking file open/close operations.
 */
public class HadoopFileSystemWrapper
  extends FileSystem {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HadoopFileSystemWrapper.class);

  public static final String MAPRFS_SCHEME = "maprfs";

  private final FileSystem underlyingFs;
  private final OperatorStats operatorStats;
  private final boolean isMapRfs;

//  public HadoopFileSystemWrapper(Configuration fsConf) throws IOException {
//    this(fsConf, null, null);
//  }

  public boolean isMapRfs() {
    return isMapRfs;
  }

  public HadoopFileSystemWrapper(Configuration fsConf, final FileSystem fs)  {
    this(fsConf, fs, null);
  }

  public HadoopFileSystemWrapper(Configuration fsConf, final FileSystem fs, OperatorStats operatorStats) {
    this.underlyingFs = fs;
    this.operatorStats = operatorStats;
    this.isMapRfs = isMapRfs(fs);
  }

  public HadoopFileSystemWrapper(Path path, Configuration fsConf, OperatorStats operatorStats) throws IOException {
    this(fsConf, path.getFileSystem(fsConf), operatorStats);
  }

  public HadoopFileSystemWrapper(Path path, Configuration fsConf) throws IOException {
    this(path, fsConf, null);
  }

  public HadoopFileSystemWrapper(Configuration fsConf, OperatorContext operatorContext) throws IOException {
    this(fsConf, FileSystem.get(FileSystem.getDefaultUri(fsConf), fsConf), (operatorContext == null) ? null : operatorContext.getStats());
  }

  private static boolean isMapRfs(FileSystem fs) {
    try {
      return MAPRFS_SCHEME.equals(fs.getScheme().toLowerCase());
    } catch (UnsupportedOperationException e) {
    }
    return false;
  }

  @Override
  public void setConf(Configuration conf) {
    // Guard against setConf(null) call that is called as part of superclass constructor (Configured) of the
    // FileSystemWrapper, at which point underlyingFs is null.
    if (conf != null && underlyingFs != null) {
      underlyingFs.setConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    return underlyingFs.getConf();
  }

  protected FileSystem getUnderlyingFs() {
    return underlyingFs;
  }

  /**
   * If OperatorStats are provided return a instrumented {@link org.apache.hadoop.fs.FSDataInputStream}.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataInputStreamWrapper(f, underlyingFs.open(f, bufferSize));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  /**
   * If OperatorStats are provided return a instrumented {@link org.apache.hadoop.fs.FSDataInputStream}.
   */
  @Override
  public FSDataInputStream open(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataInputStreamWrapper(f, underlyingFs.open(f));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.initialize(name, conf);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public String getScheme() {
    return underlyingFs.getScheme();
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, overwrite));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, replication));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, replication, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, overwrite, bufferSize));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, overwrite, bufferSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, overwrite, bufferSize, replication, blockSize));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, overwrite, bufferSize, replication, blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileStatus(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.createSymlink(target, link, createParent);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileLinkStatus(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean supportsSymlinks() {
    return underlyingFs.supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getLinkTarget(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileChecksum(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    underlyingFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    underlyingFs.setWriteChecksum(writeChecksum);
  }

  @Override
  public FsStatus getStatus() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getStatus();
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getStatus(p);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setPermission(p, permission);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setOwner(p, username, groupname);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setTimes(p, mtime, atime);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.createSnapshot(path, snapshotName);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.renameSnapshot(path, snapshotOldName, snapshotNewName);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.deleteSnapshot(path, snapshotName);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.modifyAclEntries(path, aclSpec);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.removeAclEntries(path, aclSpec);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.removeDefaultAcl(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.removeAcl(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setAcl(path, aclSpec);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getAclStatus(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getWorkingDirectory();
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.append(f, bufferSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.concat(trg, psrcs);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public short getReplication(Path src) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getReplication(src);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.setReplication(src, replication);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clazz) {
    if(clazz.isAssignableFrom(underlyingFs.getClass())) {
      return (T) underlyingFs;
    }

    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.mkdirs(f, permission);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyFromLocalFile(src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.moveFromLocalFile(srcs, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.moveFromLocalFile(src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyFromLocalFile(delSrc, src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyFromLocalFile(delSrc, overwrite, src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyToLocalFile(src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.moveToLocalFile(src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyToLocalFile(delSrc, src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.startLocalOutput(fsOutputFile, tmpLocalFile);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.completeLocalOutput(fsOutputFile, tmpLocalFile);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void close() throws IOException {
    underlyingFs.close();
  }

  @Override
  public long getUsed() throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getUsed();
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    try {
      return underlyingFs.getBlockSize(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public long getDefaultBlockSize() {
    return underlyingFs.getDefaultBlockSize();

  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return underlyingFs.getDefaultBlockSize(f);
  }

  @Override
  @Deprecated
  public short getDefaultReplication() {
    return underlyingFs.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return underlyingFs.getDefaultReplication(path);
  }

  @Override
  public boolean mkdirs(Path folderPath) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      if (!underlyingFs.exists(folderPath)) {
        return underlyingFs.mkdirs(folderPath);
      } else if (!underlyingFs.getFileStatus(folderPath).isDirectory()) {
        throw new IOException("The specified folder path exists and is not a folder.");
      }
      return false;
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, permission, flags, bufferSize, replication,
          blockSize, progress, checksumOpt));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.createNonRecursive(f, overwrite, bufferSize, replication,
          blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.createNonRecursive(f, permission, overwrite, bufferSize, replication,
          blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
   try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.createNewFile(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.append(f));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.append(f, bufferSize));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short
      replication, long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return newFSDataOutputStreamWrapper(underlyingFs.create(f, permission, flags, bufferSize, replication, blockSize, progress));
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listStatus(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listCorruptFileBlocks(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listStatus(f, filter);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listStatus(files);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listStatus(files, filter);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.globStatus(pathPattern);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.globStatus(pathPattern, filter);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listLocatedStatus(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listFiles(f, recursive);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path getHomeDirectory() {
    return underlyingFs.getHomeDirectory();
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    underlyingFs.setWorkingDirectory(new_dir);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.rename(src, dst);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public boolean delete(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.delete(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.delete(f, recursive);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.deleteOnExit(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.cancelDeleteOnExit(f);
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.exists(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    boolean exists = false;
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      exists = underlyingFs.isDirectory(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    boolean exists = false;
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      exists = underlyingFs.isFile(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  @Deprecated
  public long getLength(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getLength(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getContentSummary(f);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public URI getUri() {
    return underlyingFs.getUri();
  }

  @Override
  @LimitedPrivate({"HDFS", "MapReduce"})
  public String getCanonicalServiceName() {
    return underlyingFs.getCanonicalServiceName();
  }

  @Override
  @Deprecated
  public String getName() {
    return underlyingFs.getName();
  }

  @Override
  public Path makeQualified(Path path) {
    return underlyingFs.makeQualified(path);
  }

  @Override
  @Private
  public Token<?> getDelegationToken(String renewer) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getDelegationToken(renewer);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @LimitedPrivate({"HDFS", "MapReduce"})
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.addDelegationTokens(renewer, credentials);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @LimitedPrivate({"HDFS"})
  @VisibleForTesting
  public FileSystem[] getChildFileSystems() {
    return underlyingFs.getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileBlockLocations(file, start, len);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileBlockLocations(p, start, len);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    try {
      return underlyingFs.getServerDefaults();
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    try {
      return underlyingFs.getServerDefaults(p);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.resolvePath(p);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean truncate(final Path f, final long newLength) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.truncate(f, newLength);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p) throws FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listStatusIterator(p);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void access(final Path path, final FsAction mode) throws AccessControlException, FileNotFoundException, IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.access(path, mode);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileChecksum getFileChecksum(final Path f, final long length) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getFileChecksum(f, length);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setXAttr(final Path path, final String name, final byte[] value) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setXAttr(path, name, value);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setXAttr(final Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.setXAttr(path, name, value, flag);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public byte[] getXAttr(final Path path, final String name) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getXAttr(path, name);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getXAttrs(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path, final List<String> names) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.getXAttrs(path, names);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public List<String> listXAttrs(final Path path) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      return underlyingFs.listXAttrs(path);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeXAttr(final Path path, final String name) throws IOException {
    try (WaitRecorder recorder = OperatorStats.getWaitRecorder(operatorStats)) {
      underlyingFs.removeXAttr(path, name);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  FSDataInputStreamWrapper newFSDataInputStreamWrapper(Path f, final FSDataInputStream is) throws IOException {
    try {
      return (operatorStats != null) ? new FSDataInputStreamWithStatsWrapper(is, operatorStats) : new FSDataInputStreamWrapper(is);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  FSDataOutputStreamWrapper newFSDataOutputStreamWrapper(FSDataOutputStream os) throws IOException {
    try {
      return (operatorStats != null) ? new FSDataOutputStreamWithStatsWrapper(os, operatorStats) : new FSDataOutputStreamWrapper(os);
    } catch(FSError e) {
      throw propagateFSError(e);
    }
  }

  public static IOException propagateFSError(FSError e) throws IOException {
    Throwables.propagateIfPossible(e.getCause(), IOException.class);
    return new IOException("Unexpected FSError", e);
  }

  public static RuntimeException propagateFSRuntimeException(FSError e) {
    Throwables.throwIfUnchecked(e.getCause());
    return new RuntimeException("Unexpected FSError", e);
  }
}
