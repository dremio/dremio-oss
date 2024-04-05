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
package com.dremio.exec.store;

import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/** Configuration for job results store. */
public final class JobResultsStoreConfig {

  private final String storageName;
  private final Path storagePath;
  private final FileSystem fileSystem;

  public JobResultsStoreConfig(String storageName, Path storagePath, FileSystem fileSystem) {
    this.storageName = storageName;
    this.storagePath = storagePath;
    this.fileSystem = fileSystem;
  }

  public String getStorageName() {
    return storageName;
  }

  public Path getStoragePath() {
    return storagePath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
