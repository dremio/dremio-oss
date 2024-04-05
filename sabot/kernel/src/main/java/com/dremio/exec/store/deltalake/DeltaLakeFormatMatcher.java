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
package com.dremio.exec.store.deltalake;

import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import java.io.IOException;
import org.apache.hadoop.security.AccessControlException;

public class DeltaLakeFormatMatcher extends FormatMatcher {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DeltaLakeFormatMatcher.class);
  private static final String METADATA_DIR_NAME = "_delta_log";
  private final FormatPlugin plugin;

  public DeltaLakeFormatMatcher(FormatPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public boolean matches(
      FileSystem fs, FileSelection fileSelection, CompressionCodecFactory codecFactory)
      throws IOException {
    return isDeltaLakeTable(fs, fileSelection.getSelectionRoot());
  }

  public boolean isDeltaLakeTable(FileSystem fs, String tableRootPath) throws IOException {
    try {
      Path metaDir = Path.of(tableRootPath).resolve(METADATA_DIR_NAME);
      return fs.isDirectory(metaDir);
    } catch (AccessControlException ex) {
      // HadoopFileSystem::isDirectory throws AccessControlException if the root itself is not a
      // directory.
      return false;
    }
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
