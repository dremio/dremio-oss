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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.dremio.exec.store.hive.HiveFsUtils;
import com.dremio.exec.store.hive.exec.HadoopFsCacheWrapperPluginClassLoader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * This class is wrapper for the File system which is created in Plugin class loader.
 * This class also maintain the cache required to provide the FileSystem related to specific conf.
 * For Cache Key, we unique identify the plugin conf using the property UNIQUE_CONF_IDENTIFIER_PROPERTY_NAME.
 * For the identified conf, we get the HadoopFsCacheWrapperPluginClassLoader, which internally manages the FileSystem for specific conf.
 * To clean cache, on plugin close cleanCache method will be called with uniqueConfIdentifier using will we will clean conf related objects
 */
public class HadoopFsWrapperWithCachePluginClassLoader extends FileSystem {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HadoopFsWrapperWithCachePluginClassLoader.class);
  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  static final LoadingCache<String, HadoopFsCacheWrapperPluginClassLoader> cache = CacheBuilder.newBuilder()
    .softValues()
    .removalListener(new RemovalListener<String, HadoopFsCacheWrapperPluginClassLoader>() {
      @Override
      public void onRemoval(RemovalNotification<String, HadoopFsCacheWrapperPluginClassLoader> notification) {
        try {
          notification.getValue().close();
        } catch (Exception e) {
          // Ignore
          logger.error("Unable to clean FS from HadoopFsCacheKeyPluginClassLoader", e);
        }
      }
    })
    .build(new CacheLoader<String, HadoopFsCacheWrapperPluginClassLoader>() {
      @Override
      public HadoopFsCacheWrapperPluginClassLoader load(String key/*confUniqueIdentifier*/) {
        return new HadoopFsCacheWrapperPluginClassLoader();
      }
    });


  protected FileSystem delegateFs;

  public HadoopFsWrapperWithCachePluginClassLoader() {
    delegateFs = null;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    String fsClassNameProperty = String.format("fs.%s.impl", name.getScheme());
    String orginalClassNameProperty = HiveFsUtils.getOldFSClassPropertyName(name.getScheme());
    Configuration newConf = new Configuration(conf);
    String originalFsClassName = conf.get(orginalClassNameProperty);
    if (originalFsClassName != null) {
      // We always expect to have the property here
      newConf.set(fsClassNameProperty, originalFsClassName);
    } else {
      newConf.unset(fsClassNameProperty);
    }
    try {
      delegateFs = getFs(name, newConf); //cache.get( conf.get(HiveConfFactory.UNIQUE_CONF_IDENTIFIER_PROPERTY)).getHadoopFsSupplierPluginClassLoader(name.toString(), newConf).get();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Unable to get FS for uri: %s ", name), e);
    }
  }

  public static void cleanCache(String uniqueIdentifier) {
    try {
      cache.invalidate(uniqueIdentifier);
    } catch (Exception e) {
      logger.error(String.format("Unable to clear cache for for UniqueIdentifier: %s ", uniqueIdentifier), e);
    }
  }

  private static FileSystem getFs(URI name, Configuration conf) throws Exception {
    try {
      String pluginConfIdentifier = conf.get(HiveFsUtils.UNIQUE_CONF_IDENTIFIER_PROPERTY_NAME);
      return cache.get(pluginConfIdentifier)
        .getHadoopFsSupplierPluginClassLoader(name.toString(), conf, UserGroupInformation.getCurrentUser().getUserName()).get();
    } catch (Exception e) {
      logger.error("FileSystem can not be created", e);
      throw new Exception("FileSystem can not be created");
    }
  }

  @Override
  public void setConf(Configuration conf) {
    // Guard against setConf(null) call that is called as part of superclass constructor (Configured) of the
    // FileSystemWrapper, at which point underlyingFs is null.
    if (conf != null && delegateFs != null) {
      delegateFs.setConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    return delegateFs.getConf();
  }

  /**
   * If OperatorStats are provided return a instrumented {@link org.apache.hadoop.fs.FSDataInputStream}.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try {
      return delegateFs.open(f, bufferSize);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  /**
   * If OperatorStats are provided return a instrumented {@link org.apache.hadoop.fs.FSDataInputStream}.
   */
  @Override
  public FSDataInputStream open(Path f) throws IOException {
    try {
      return delegateFs.open(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public String getScheme() {
    return delegateFs.getScheme();
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    try {
      return delegateFs.create(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    try {
      return delegateFs.create(f, overwrite);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    try {
      return delegateFs.create(f, replication);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, replication, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    try {
      return delegateFs.create(f, overwrite, bufferSize);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, overwrite, bufferSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
                                   long blockSize) throws IOException {
    try {
      return delegateFs.create(f, overwrite, bufferSize, replication, blockSize);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, overwrite, bufferSize, replication, blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try {
      return delegateFs.getFileStatus(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
    try {
      delegateFs.createSymlink(target, link, createParent);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws AccessControlException, FileNotFoundException,
    UnsupportedFileSystemException, IOException {
    try {
      return delegateFs.getFileLinkStatus(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean supportsSymlinks() {
    return delegateFs.supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    try {
      return delegateFs.getLinkTarget(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    try {
      return delegateFs.getFileChecksum(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    delegateFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    delegateFs.setWriteChecksum(writeChecksum);
  }

  @Override
  public FsStatus getStatus() throws IOException {
    try {
      return delegateFs.getStatus();
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    try {
      return delegateFs.getStatus(p);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    try {
      delegateFs.setPermission(p, permission);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    try {
      delegateFs.setOwner(p, username, groupname);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try {
      delegateFs.setTimes(p, mtime, atime);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    try {
      return delegateFs.createSnapshot(path, snapshotName);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    try {
      delegateFs.renameSnapshot(path, snapshotOldName, snapshotNewName);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    try {
      delegateFs.deleteSnapshot(path, snapshotName);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      delegateFs.modifyAclEntries(path, aclSpec);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      delegateFs.removeAclEntries(path, aclSpec);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    try {
      delegateFs.removeDefaultAcl(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    try {
      delegateFs.removeAcl(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      delegateFs.setAcl(path, aclSpec);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    try {
      return delegateFs.getAclStatus(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return delegateFs.getWorkingDirectory();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    try {
      return delegateFs.append(f, bufferSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    try {
      delegateFs.concat(trg, psrcs);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public short getReplication(Path src) throws IOException {
    try {
      return delegateFs.getReplication(src);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    try {
      return delegateFs.setReplication(src, replication);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isAssignableFrom(delegateFs.getClass())) {
      return (T) delegateFs;
    }

    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try {
      return delegateFs.mkdirs(f, permission);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    try {
      delegateFs.copyFromLocalFile(src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    try {
      delegateFs.moveFromLocalFile(srcs, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    try {
      delegateFs.moveFromLocalFile(src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try {
      delegateFs.copyFromLocalFile(delSrc, src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    try {
      delegateFs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    try {
      delegateFs.copyFromLocalFile(delSrc, overwrite, src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    try {
      delegateFs.copyToLocalFile(src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    try {
      delegateFs.moveToLocalFile(src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try {
      delegateFs.copyToLocalFile(delSrc, src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    try {
      delegateFs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try {
      return delegateFs.startLocalOutput(fsOutputFile, tmpLocalFile);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try {
      delegateFs.completeLocalOutput(fsOutputFile, tmpLocalFile);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void close() throws IOException {
    delegateFs.close();
  }

  @Override
  public long getUsed() throws IOException {
    try {
      return delegateFs.getUsed();
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    try {
      return delegateFs.getBlockSize(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public long getDefaultBlockSize() {
    return delegateFs.getDefaultBlockSize();

  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return delegateFs.getDefaultBlockSize(f);
  }

  @Override
  @Deprecated
  public short getDefaultReplication() {
    return delegateFs.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return delegateFs.getDefaultReplication(path);
  }

  @Override
  public boolean mkdirs(Path folderPath) throws IOException {
    try {
      if (!delegateFs.exists(folderPath)) {
        return delegateFs.mkdirs(folderPath);
      } else if (!delegateFs.getFileStatus(folderPath).isDirectory()) {
        throw new IOException("The specified folder path exists and is not a folder.");
      }
      return false;
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    try {
      return delegateFs.create(f, permission, flags, bufferSize, replication,
        blockSize, progress, checksumOpt);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
                                               long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.createNonRecursive(f, overwrite, bufferSize, replication,
        blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.createNonRecursive(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    try {
      return delegateFs.createNewFile(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    try {
      return delegateFs.append(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    try {
      return delegateFs.append(f, bufferSize);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short
    replication, long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    try {
      return delegateFs.create(f, permission, flags, bufferSize, replication, blockSize, progress);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listStatus(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    try {
      return delegateFs.listCorruptFileBlocks(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listStatus(f, filter);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listStatus(files);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listStatus(files, filter);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    try {
      return delegateFs.globStatus(pathPattern);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    try {
      return delegateFs.globStatus(pathPattern, filter);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listLocatedStatus(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listFiles(f, recursive);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path getHomeDirectory() {
    return delegateFs.getHomeDirectory();
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    delegateFs.setWorkingDirectory(new_dir);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      return delegateFs.rename(src, dst);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public boolean delete(Path f) throws IOException {
    try {
      return delegateFs.delete(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      return delegateFs.delete(f, recursive);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    try {
      return delegateFs.deleteOnExit(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    return delegateFs.cancelDeleteOnExit(f);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try {
      return delegateFs.exists(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    boolean exists = false;
    try {
      exists = delegateFs.isDirectory(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    boolean exists = false;
    try {
      exists = delegateFs.isFile(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
    return exists;
  }

  @Override
  @Deprecated
  public long getLength(Path f) throws IOException {
    try {
      return delegateFs.getLength(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    try {
      return delegateFs.getContentSummary(f);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public URI getUri() {
    return delegateFs.getUri();
  }

  @Override
  @LimitedPrivate({"HDFS", "MapReduce"})
  public String getCanonicalServiceName() {
    return delegateFs.getCanonicalServiceName();
  }

  @Override
  @Deprecated
  public String getName() {
    return delegateFs.getName();
  }

  @Override
  public Path makeQualified(Path path) {
    return delegateFs.makeQualified(path);
  }

  @Override
  @Private
  public Token<?> getDelegationToken(String renewer) throws IOException {
    try {
      return delegateFs.getDelegationToken(renewer);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @LimitedPrivate({"HDFS", "MapReduce"})
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    try {
      return delegateFs.addDelegationTokens(renewer, credentials);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @LimitedPrivate({"HDFS"})
  @VisibleForTesting
  public FileSystem[] getChildFileSystems() {
    return delegateFs.getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    try {
      return delegateFs.getFileBlockLocations(file, start, len);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    try {
      return delegateFs.getFileBlockLocations(p, start, len);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    try {
      return delegateFs.getServerDefaults();
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    try {
      return delegateFs.getServerDefaults(p);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    try {
      return delegateFs.resolvePath(p);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public boolean truncate(final Path f, final long newLength) throws IOException {
    try {
      return delegateFs.truncate(f, newLength);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p) throws FileNotFoundException, IOException {
    try {
      return delegateFs.listStatusIterator(p);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void access(final Path path, final FsAction mode) throws AccessControlException, FileNotFoundException, IOException {
    try {
      delegateFs.access(path, mode);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public FileChecksum getFileChecksum(final Path f, final long length) throws IOException {
    try {
      return delegateFs.getFileChecksum(f, length);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setXAttr(final Path path, final String name, final byte[] value) throws IOException {
    try {
      delegateFs.setXAttr(path, name, value);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void setXAttr(final Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag) throws IOException {
    try {
      delegateFs.setXAttr(path, name, value, flag);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public byte[] getXAttr(final Path path, final String name) throws IOException {
    try {
      return delegateFs.getXAttr(path, name);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path) throws IOException {
    try {
      return delegateFs.getXAttrs(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(final Path path, final List<String> names) throws IOException {
    try {
      return delegateFs.getXAttrs(path, names);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public List<String> listXAttrs(final Path path) throws IOException {
    try {
      return delegateFs.listXAttrs(path);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  @Override
  public void removeXAttr(final Path path, final String name) throws IOException {
    try {
      delegateFs.removeXAttr(path, name);
    } catch (FSError e) {
      throw propagateFSError(e);
    }
  }

  public static IOException propagateFSError(FSError e) throws IOException {
    Throwables.propagateIfPossible(e.getCause(), IOException.class);
    return new IOException("Unexpected FSError", e);
  }
}
