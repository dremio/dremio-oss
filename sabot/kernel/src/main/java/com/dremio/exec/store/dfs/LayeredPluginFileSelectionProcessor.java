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

import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class LayeredPluginFileSelectionProcessor implements FileSelectionProcessor {

  private FileSystem fs;
  private FileSelection fileSelection;

  public LayeredPluginFileSelectionProcessor(FileSystem fs, FileSelection fileSelection) {
    this.fs = fs;
    this.fileSelection = fileSelection;
  }

  @Override
  public FileProtobuf.FileUpdateKey generateUpdateKey() throws IOException {
    final Path datasetRoot = Path.of(fileSelection.getSelectionRoot());
    final FileProtobuf.FileUpdateKey.Builder updateKeyBuilder = FileProtobuf.FileUpdateKey.newBuilder();
    final FileAttributes rootAttributes = fs.getFileAttributes(datasetRoot);

    if (rootAttributes.isDirectory()) {
      updateKeyBuilder.addCachedEntities(FileProtobuf.FileSystemCachedEntity.newBuilder()
              .setPath(rootAttributes.getPath().toString())
              .setLastModificationTime(rootAttributes.lastModifiedTime().toMillis())
              .build());
    }
    return updateKeyBuilder.build();
  }

  @Override
  public FileSelection normalizeForPlugin(FileSelection selection) {
    return selection;
  }
}
