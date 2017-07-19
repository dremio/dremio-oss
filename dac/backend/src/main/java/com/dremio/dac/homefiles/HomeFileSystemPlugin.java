/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.homefiles;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.schema.Function;
import org.apache.hadoop.fs.FileSystem;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.google.common.collect.ImmutableList;

/**
 * Plugin for user uploaded files.
 *
 */
public class HomeFileSystemPlugin extends FileSystemPlugin {

  private final FileSystem fs;

  public HomeFileSystemPlugin(HomeFileSystemPluginConfig config, SabotContext dContext, String storageName) throws ExecutionSetupException, IOException {
    super(config, dContext, storageName);
    this.fs = config.getHomeFileConfig().createFileSystem();
  }

  public FileSystem getProccessFs(){
    return fs;
  }


  public HomeFileSystemStoragePlugin2 getStoragePlugin2() {
    return new HomeFileSystemStoragePlugin2(getStorageName(), getConfig(), this);
  }

  @Override
  public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return getOptionExtractor().getFunctions(tableSchemaPath, this, schemaConfig);
  }

  @Override
  public boolean folderExists(String userName, List<String> folderPath) throws IOException {
    return getFS(userName).exists(PathUtils.toFSPath(
      ImmutableList.<String>builder()
        .addAll(folderPath.subList(1, folderPath.size()))
        .build()));
  }
}
