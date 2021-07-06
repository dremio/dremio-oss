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
package com.dremio.exec.store.iceberg;

import java.io.IOException;

import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Matcher for iceberg format. We expect :
 *
 * a. directory with name "metadata",
 *  (and)
 * b. file with pattern v\d*.metadata.json in (a)
 *  (and)
 * c. file with name "version-hint.text" in (a)
 *
 */
public class IcebergFormatMatcher extends FormatMatcher {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergFormatMatcher.class);
  public static final String METADATA_DIR_NAME = "metadata";
  private final FormatPlugin plugin;

  public IcebergFormatMatcher(FormatPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public FormatPlugin getFormatPlugin()  {
    return this.plugin;
  }

  @Override
  public boolean matches(FileSystem fs, FileSelection fileSelection, CompressionCodecFactory codecFactory) throws IOException {
    Path rootDir = Path.of(fileSelection.getSelectionRoot());
    Path metaDir = rootDir.resolve(METADATA_DIR_NAME);
    return fs.isDirectory(rootDir) && fs.exists(metaDir) && fs.isDirectory(metaDir);
  }
}
