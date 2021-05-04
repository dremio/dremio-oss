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

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.file.proto.FileProtobuf;

/**
 * Performs FormatPlugin specific duties on the FileSelection objects
 */
public interface FileSelectionProcessor {

  /**
   * Generates a UpdateKey based on the input files.
   *
   * @return
   * @throws IOException
   */
  FileProtobuf.FileUpdateKey generateUpdateKey() throws IOException;

  /**
   * To ensure the number of files in the directory are within compatible range. This is tracked via FileDatasetHandle.DFS_MAX_FILES
   *
   * @param config
   * @param isInternal
   * @throws IOException
   */
  void assertCompatibleFileCount(SabotContext config, boolean isInternal) throws IOException;

  /**
   * Return a new normalised version of FileSelection, apt for plugin's usage. Eg. regular format plugins require file
   * entries after removing the directories.
   * @param fileSelection
   * @return
   * @throws IOException
   */
  FileSelection normalizeForPlugin(FileSelection fileSelection) throws IOException;

  /**
   * Default FileSelection is in non-expanded mode. This function is to provide desirability handle to the accessor.
   * This way, the FileSelection will be lazily expanded.
   *
   * @throws IOException
   */
  void expandIfNecessary() throws IOException;
}
