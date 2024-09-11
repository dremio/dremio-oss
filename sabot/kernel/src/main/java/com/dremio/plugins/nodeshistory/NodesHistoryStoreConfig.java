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
package com.dremio.plugins.nodeshistory;

import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Configuration of the nodes history system source. This source is not directly accessible by users
 * but is utilized internally by the "sys" source (sysflight storage plugin).
 */
public final class NodesHistoryStoreConfig {
  public static final String STORAGE_PLUGIN_NAME = "__nodeHistory";

  private final Path storagePath;
  private final FileSystem fileSystem;

  public NodesHistoryStoreConfig(Path storagePath, FileSystem fileSystem) {
    this.storagePath = storagePath;
    this.fileSystem = fileSystem;
  }

  public Path getStoragePath() {
    return storagePath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
