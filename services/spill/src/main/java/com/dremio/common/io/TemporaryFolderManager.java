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

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Generic interface for temporary folder management.
 *
 * // TODO: This needs to move somewhere else for other services to reuse. Not sure where yet.
 */
public interface TemporaryFolderManager extends AutoCloseable {
  /**
   * Start folder monitoring.
   *
   * <p>
   * Folder monitoring is only started if available executor(s) can be queried.
   * </p>
   */
  void startMonitoring();

  /**
   * Creates a new incarnation of the temporary directory. If an old incarnation is found
   * for the same executor under {@code rootPath}, a task will be internally scheduled to
   * cleanup that directory.
   *
   * @param rootPath Root path under which the temporary directory has to be created.
   *
   * @return a path to the temporary directory of the new incarnation of the executor.
   */
  Path createTmpDirectory(Path rootPath) throws IOException;
}
