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

import java.io.IOException;
import java.nio.file.NotLinkException;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.Path;

public final class HadoopFileStatusWrapper implements FileAttributes {
  private final UserPrincipal owner = new UserPrincipal() {

    @Override
    public String getName() {
      return status.getOwner();
    }
  };

  private final GroupPrincipal group = new GroupPrincipal() {

    @Override
    public String getName() {
      return status.getGroup();
    }
  };

  private final FileStatus status;

  public HadoopFileStatusWrapper(FileStatus status) {
    this.status = status;
  }

  @Override
  public UserPrincipal owner() {
    return owner;
  }

  @Override
  public GroupPrincipal group() {
    return group;
  }

  @Override
  public Set<PosixFilePermission> permissions() {
    final FsPermission permission = status.getPermission();
    return PosixFilePermissions.fromString(
        permission.getUserAction().SYMBOL + permission.getGroupAction().SYMBOL + permission.getOtherAction().SYMBOL);
  }

  @Override
  public FileTime lastModifiedTime() {
    return FileTime.fromMillis(status.getModificationTime());
  }

  @Override
  public FileTime lastAccessTime() {
    return FileTime.fromMillis(status.getAccessTime());
  }

  @Override
  public FileTime creationTime() {
    return FileTime.fromMillis(status.getModificationTime());
  }

  @Override
  public boolean isRegularFile() {
    return status.isFile();
  }

  @Override
  public boolean isDirectory() {
    return status.isDirectory();
  }

  @Override
  public boolean isSymbolicLink() {
    return status.isSymlink();
  }

  @Override
  public boolean isOther() {
    return !status.isDirectory() && !status.isFile() && !status.isSymlink();
  }

  @Override
  public long size() {
    return status.getLen();
  }

  @Override
  public Object fileKey() {
    return null;
  }

  @Override
  public Path getPath() {
    return Path.of(status.getPath().toUri());
  }

  @Override
  public Path getSymbolicLink() throws IOException {
    if (!isSymbolicLink()) {
      throw new NotLinkException(status.getPath().toString());
    }

    return Path.of(status.getSymlink().toUri());
  }

  public FileStatus getFileStatus() {
    return status;
  }

  @Override
  public String toString() {
    return status.toString();
  }
}
