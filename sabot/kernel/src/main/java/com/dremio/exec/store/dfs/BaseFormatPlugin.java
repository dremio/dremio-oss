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

import static com.dremio.io.file.PathFilters.NO_HIDDEN_FILES;

import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import java.io.IOException;
import java.nio.file.DirectoryStream;

public abstract class BaseFormatPlugin implements FormatPlugin {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseFormatPlugin.class);

  private final SabotContext context;
  private final FileSystemPlugin<?> fsPlugin;

  protected BaseFormatPlugin(SabotContext context, FileSystemPlugin<?> fsPlugin) {
    this.context = context;
    this.fsPlugin = fsPlugin;
  }

  public FileSystemPlugin<?> getFsPlugin() {
    return fsPlugin;
  }

  @Override
  public DirectoryStream<FileAttributes> getFilesForSamples(
      FileSystem fs, FileSystemPlugin<?> fsPlugin, Path path)
      throws IOException, FileCountTooLargeException {
    int maxFilesLimit = getMaxFilesLimit();
    return FileSystemUtils.listFilterDirectoryRecursive(fs, path, maxFilesLimit, NO_HIDDEN_FILES);
  }
}
