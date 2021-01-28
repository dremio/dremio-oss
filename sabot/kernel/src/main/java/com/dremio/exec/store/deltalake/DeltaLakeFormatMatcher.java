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

import java.io.IOException;
import java.nio.file.AccessMode;
import java.util.Collections;

import org.apache.hadoop.security.AccessControlException;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.iceberg.IcebergFormatMatcher;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class DeltaLakeFormatMatcher extends FormatMatcher {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergFormatMatcher.class);
  private static final String METADATA_DIR_NAME = "_delta_log";
  private FormatPlugin plugin;

  public DeltaLakeFormatMatcher(FormatPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public boolean matches(FileSystem fs, FileSelection fileSelection, CompressionCodecFactory codecFactory) throws IOException{

    if(!plugin.getContext().getOptionManager().getOption(ExecConstants.ENABLE_DELTALAKE)) {
      return false;
    }

    Path rootDir = Path.of(fileSelection.getSelectionRoot());
    Path metaDir = rootDir.resolve(METADATA_DIR_NAME);

    if(!fs.exists(metaDir) && !fs.isDirectory(metaDir)) {
      return false;
    }

    try {
      fs.access(metaDir, Collections.singleton(AccessMode.READ));
    }
    catch (AccessControlException e) {
      //fail silently as the file can still be read as parquet dataset.
      logger.error("{} not matched as a deltalake dataset as _delta_log is not readable. Exception {}", rootDir, e.getMessage());
      return false;
    }

    return true;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
