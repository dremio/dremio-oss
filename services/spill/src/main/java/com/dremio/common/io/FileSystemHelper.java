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
package com.dremio.common.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to walk directories in the file system looking for executor and job directories
 * that needs clean up.
 */
final class FileSystemHelper {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTemporaryFolderManager.class);
  private static final String NEW_INCARNATION_FORMAT = "%s" + File.separator + "%d";
  private static final FsPermission PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private final Configuration conf;

  FileSystemHelper(Configuration conf) {
    this.conf = conf;
  }

  void visitDirectory(Path rootPath, String prefix, Consumer<FileStatus> statusConsumer) {
    try (final LocalFileSystemWrapper wrapper = new LocalFileSystemWrapper(rootPath, conf)) {
      final FileSystem fileSystem = wrapper.getWrappedFs();
      final Path prefixPath = (prefix == null || prefix.isEmpty()) ? rootPath : new Path(rootPath, prefix);
      if (fileSystem.exists(prefixPath)) {
        final FileStatus[] statuses = fileSystem.listStatus(prefixPath);
        for (final FileStatus fStatus : statuses) {
          statusConsumer.accept(fStatus);
        }
      }
    } catch (IOException e) {
      logger.warn("IO exception occurred while looking for stale executor incarnations. {}", e.getMessage());
    }
  }

  void visitNonEmptyDirectory(Path rootPath, Consumer<FileStatus> statusConsumer) {
    try (final LocalFileSystemWrapper wrapper = new LocalFileSystemWrapper(rootPath, conf)) {
      final FileSystem fileSystem = wrapper.getWrappedFs();
      if (fileSystem.exists(rootPath)) {
        final FileStatus[] statuses = fileSystem.listStatus(rootPath);
        for (final FileStatus fStatus : statuses) {
          if (fStatus.isDirectory() && fileSystem.listStatus(fStatus.getPath()).length > 0) {
            statusConsumer.accept(fStatus);
          }
        }
      }
    } catch (IOException e) {
      logger.warn("IO exception occurred while looking for stale executor incarnations. {}", e.getMessage());
    }
  }

  void safeCleanDirectory(Path rootPath, String prefix, int stalenessLimitSeconds) {
    visitDirectory(rootPath, prefix, fileStatus -> {
      if (fileStatus.isDirectory() && isStaleModify(fileStatus, stalenessLimitSeconds)) {
        safeCleanOldIncarnation(fileStatus.getPath(), stalenessLimitSeconds);
      }
    });
  }

  Path createTmpDirectory(Path rootPath, String prefix) throws IOException {
    final long incarnation = Instant.now().toEpochMilli();
    final Path tmpDirPath = new Path(rootPath, String.format(NEW_INCARNATION_FORMAT, prefix, incarnation));
    try (final LocalFileSystemWrapper wrapper = new LocalFileSystemWrapper(rootPath, conf)) {
      final FileSystem fileSystem = wrapper.getWrappedFs();
      if (fileSystem.exists(tmpDirPath) || fileSystem.mkdirs(tmpDirPath, PERMISSIONS)) {
        return tmpDirPath;
      }
    }
    throw new IOException("Unable to create temporary directory");
  }

  void safeCleanOldIncarnation(Path incarnationDir, int stalenessLimitSeconds) {
    try (final LocalFileSystemWrapper wrapper = new LocalFileSystemWrapper(incarnationDir, conf)) {
      final FileSystem fileSystem = wrapper.getWrappedFs();
      final boolean[] canDelete = new boolean[1];
      canDelete[0] = true;
      walkDirectory(fileSystem, incarnationDir, fileStatus -> {
        if (!isStaleAccess(fileStatus, stalenessLimitSeconds)) {
          canDelete[0] = false;
        }
      });
      if (canDelete[0]) {
        logger.info("Cleaning up {}", incarnationDir.toString());
        fileSystem.delete(incarnationDir, true);
      }
    } catch (IOException e) {
      logger.warn("Unable to clean {}. Maybe retried in the next iteration", incarnationDir.toString());
    }
  }

  private void walkDirectory(FileSystem fs, Path dirPath, Consumer<FileStatus> statusConsumer) {
    try {
      if (fs.exists(dirPath)) {
        final FileStatus[] statuses = fs.listStatus(dirPath);
        for (final FileStatus fStatus : statuses) {
          if (fStatus.isDirectory()) {
            walkDirectory(fs, fStatus.getPath(), statusConsumer);
          } else {
            statusConsumer.accept(fStatus);
          }
        }
      }
    } catch (IOException e) {
      logger.warn("IO exception occurred while looking for stale executor incarnations. {}", e.getMessage());
    }
  }

  private static boolean isStaleModify(FileStatus fileStatus, int stalenessLimitSeconds) {
    final long currentTime = Instant.now().toEpochMilli();
    final long lastModified = TimeUnit.MILLISECONDS.toSeconds(currentTime - fileStatus.getModificationTime());
    return lastModified > stalenessLimitSeconds;
  }

  private static boolean isStaleAccess(FileStatus fileStatus, int stalenessLimitSeconds) {
    final long currentTime = Instant.now().toEpochMilli();
    final long lastAccessed = TimeUnit.MILLISECONDS.toSeconds(currentTime - fileStatus.getAccessTime());
    final long lastModified = TimeUnit.MILLISECONDS.toSeconds(currentTime - fileStatus.getModificationTime());
    return lastAccessed > stalenessLimitSeconds && lastModified > stalenessLimitSeconds;
  }

  private static final class LocalFileSystemWrapper implements Closeable {
    private final boolean closeWrapped;
    private final FileSystem wrappedFs;

    LocalFileSystemWrapper(Path path, Configuration conf) throws IOException {
      this.wrappedFs = path.getFileSystem(conf);
      String scheme;
      try {
        scheme = this.wrappedFs.getScheme();
      } catch (UnsupportedOperationException e) {
        // default to file
        scheme = "file";
      }
      final String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
      // if cache is disabled in the conf for the scheme, we should cleanup after ourselves
      closeWrapped = conf.getBoolean(disableCacheName, false);
    }

    private FileSystem getWrappedFs() {
      return this.wrappedFs;
    }

    @Override
    public void close() {
      if (closeWrapped) {
        try {
          wrappedFs.close();
        } catch (IOException ignored) {
        }
      }
    }
  }
}
