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

import java.io.IOException;
import java.util.List;

import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class DefaultFileSelectionProcessor implements FileSelectionProcessor {

  private FileSystem fs;
  private FileSelection fileSelection;
  private final Path datasetRoot;
  private FileProtobuf.FileUpdateKey updateKey;
  private final int maxFiles;

  public DefaultFileSelectionProcessor(FileSystem fs, FileSelection fileSelection, int maxFiles) {
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.datasetRoot = Path.of(fileSelection.getSelectionRoot());
    this.maxFiles = maxFiles;
  }

  @Override
  public FileProtobuf.FileUpdateKey generateUpdateKey() throws IOException {
    if(updateKey != null) {
      return this.updateKey;
    }

    // Get subdirectories under file selection before pruning directories
    final FileProtobuf.FileUpdateKey.Builder updateKeyBuilder = FileProtobuf.FileUpdateKey.newBuilder();
    final FileAttributes rootAttributes = fs.getFileAttributes(datasetRoot);

    if (rootAttributes.isDirectory()) {
      // first entity is always a root
      updateKeyBuilder.addCachedEntities(fromFileAttributes(rootAttributes));
    }

    if(fileSelection.isNoDirs()) {
      //If the selection has no dirs then create a new selection as dir attr are required for
      //update key generation
      throw new IllegalStateException("FileSelection object should be present and should contain subdirectory attributes to generate update key");
    }

    // fileSelection is expanded at this point of time - see normalizeForPlugin()
    final List<FileAttributes> fileAttributes = fileSelection.getFileAttributesList();
    for (FileAttributes attributes : fileAttributes) {
      if(attributes.isDirectory()) {
        updateKeyBuilder.addCachedEntities(fromFileAttributes(attributes));
      }
    }
    this.updateKey = updateKeyBuilder.build();
    return this.updateKey;
  }

  @Override
  public FileSelection normalizeForPlugin(FileSelection selection) throws IOException {
    selection.expand(datasetRoot.getName(), fs, maxFiles);
    generateUpdateKey();
    fileSelection = selection.minusDirectories();
    return fileSelection;
  }

  protected FileProtobuf.FileSystemCachedEntity fromFileAttributes(FileAttributes attributes) {
    return FileProtobuf.FileSystemCachedEntity.newBuilder()
      .setPath(attributes.getPath().toString())
      .setLastModificationTime(attributes.lastModifiedTime().toMillis())
      .build();
  }
}
