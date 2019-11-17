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

import java.io.Closeable;
import java.io.IOException;

import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Wraps instance of fileysystem to extend behavior
 */
@FunctionalInterface
public interface FileSystemWrapper extends Closeable {
  String FILE_SYSTEM_WRAPPER_CLASS = "dremio.filesystemwrapper.class";

  FileSystem wrap(FileSystem fs, String storageId, FileSystemConf<?, ?> conf, OperatorContext context,
      boolean enableAsync, boolean isMetadataRefresh) throws IOException;

  @Override
  default void close() throws IOException {
  }
}
